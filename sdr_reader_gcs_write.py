import threading
import queue
import time
import json
import itertools
from collections import deque, Counter
from dataclasses import dataclass
import os
import sys
import numpy as np

# ── Timestamped stdout ────────────────────────────────────────────────────────
# Wraps every print() call with [HH:MM:SS.mmm PID] so all log lines are
# identifiable across threads and process restarts without changing call sites.
class _TimestampedStream:
    def __init__(self, stream):
        self._stream = stream
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._pending = ''

    def _safe_write(self, text):
        # The underlying stream's encoding (e.g. cp1252 under some Windows
        # Python environments) may not support every character a print()
        # call happens to use (e.g. arrows, checkmarks) - fall back to a
        # lossy-but-non-crashing encode rather than letting one debug
        # print() abort the whole run.
        try:
            self._stream.write(text)
        except UnicodeEncodeError:
            encoding = getattr(self._stream, 'encoding', None) or 'ascii'
            self._stream.write(text.encode(encoding, errors='replace').decode(encoding))

    def write(self, text):
        if not text:
            return
        with self._lock:
            self._pending += text
            while '\n' in self._pending:
                line, self._pending = self._pending.split('\n', 1)
                ts = time.strftime('%H:%M:%S') + f'.{int(time.time() * 1000) % 1000:03d}'
                self._safe_write(f'[{ts} {self._pid}] {line}\n')
            self._stream.flush()

    def flush(self):
        with self._lock:
            if self._pending:
                ts = time.strftime('%H:%M:%S') + f'.{int(time.time() * 1000) % 1000:03d}'
                self._safe_write(f'[{ts} {self._pid}] {self._pending}')
                self._pending = ''
            self._stream.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)

sys.stdout = _TimestampedStream(sys.stdout)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import bladerf                  # This is the high-level, user-friendly wrapper
    from bladerf import _bladerf    # This is the low-level module with all the constants
except Exception:
    _bladerf = None
from scipy import signal
from scipy.io import loadmat
import matplotlib.pyplot as plt

try:
    import somata as _somata
except Exception:
    _somata = None


@dataclass
class DecodedPacket:
    packet_num: int
    is_valid: bool
    bits: np.ndarray
    error_flag: bool = False  # changeofstrength_flag OR packetnum_anomaly_flag -
                               # kept merged for the v1/v2 arbitration fallback
                               # logic, which depends on this exact OR. Use the
                               # two fields below instead when you need to know
                               # which one actually fired (characterization only -
                               # the persisted quality_packed/GCS output still
                               # only carries this merged bit, by design - see
                               # sdr_reader_gcs_write.py plan from this session).
    changeofstrength_flag: bool = False  # bitThreshold_hdl.m's Changeofstrengthflag,
                                          # OR'd across the packet's words - isolated
                                          # from packetnum_anomaly_flag below
    packetnum_anomaly_flag: bool = False  # this packet's packet_num field disagreed
                                           # with what inter-packet timing/distance
                                           # predicted - isolated from
                                           # changeofstrength_flag above. packet_num
                                           # is not Viterbi-protected and sits at the
                                           # least-timing-settled point in the packet,
                                           # so this fires far more than genuine link
                                           # degradation does - see this session's
                                           # header-drop investigation.
    low_conf_count: int = 0  # softCombine_hdl.m's per-packet low-confidence-bit
                              # count, saturating at 63 (6-bit field, same USB
                              # word as packet_num/error_flag/valid_flag)
    superseded_flag: bool = False  # softCombine_hdl.m's superseded_flag - this
                                    # packet was abandoned mid-payload in favor
                                    # of a stronger competing peak (see
                                    # searchPeak2_hdl.m's accept_cand/
                                    # superseded_pulse doc, new_blocks/) - its
                                    # content should be treated as untrustworthy,
                                    # same as a high low_conf_count. Same latch-
                                    # on-packet_flag / held-until-next-packet_flag
                                    # convention as low_conf_count above, read the
                                    # same one-frame-forward way - see
                                    # superseded_flag_arr's own comment below.
    reason: str = ''  # why this packet is invalid: cross_gap, intra_gap, out_of_order, ch_bounds, group_builder


# Severe-packet low_conf_count cutoff, for characterization reporting
# (get_decoded_flags()) only - not used by the v1/v2 arbitration logic, which
# compares raw low_conf_count values directly rather than thresholding them.
# Empirically validated earlier this session against real duplicate-pair
# capture data: at this cutoff, the flagged side matched the actual weak side
# (per per-bit margin analysis) in 8/8 pairs with enough disputed bits to be
# a meaningful comparison. Not re-derived per-deployment - recalibrate if the
# link/margin_cutoff characteristics change substantially.
LOW_CONF_COUNT_THRESHOLD = 10

# Cutoff for excluding a packet from group_builder's pending queue entirely
# (see _decode_packet_groups) - deliberately a SEPARATE, much stricter
# constant from LOW_CONF_COUNT_THRESHOLD above, not a reuse of it. The two
# jobs have very different costs for a wrong call: v1/v2 arbitration just
# picks the less-bad of two copies you already have (a soft choice), while
# excluding a packet here throws away real decodable data outright (a much
# higher-cost mistake). Calibrated 2026-07-26 via a full threshold sweep
# against PRBS ground truth on the agcslow_slowwalk survey (19 snapshots):
# errors dropped ~24x (1914->79 on v1) from disabled down to 5, while
# missing count barely moved (+4.5%) - see wireless_link/README or session
# notes for the full sweep table. Only 2 hard-error events existed in the
# smaller 10/15/20m PRBS comparison set, not enough on their own to
# calibrate this - the slow-walk survey's larger sample is what grounds it.
GROUP_BUILDER_LOW_CONF_CUTOFF = 5

# EXPERIMENTAL (2026-08-03, not yet validated/defaulted on): a
# packetnum_anomaly_flag packet's packet_num has usually ALREADY been
# corrected to the distance-forced expected_at_current value by the time it
# reaches here (see the packet_num_distance_corrections logic above this
# packet's construction) - or, in the confirmed-real-skip case, the OBSERVED
# value was independently confirmed correct by _lookahead_confirms_real_skip.
# Either way, the packet_num on a flagged packet is often trustworthy; the
# flag really only means "packet_num disagreed with the naive per-frame
# distance guess at some point," not "this packet's data is bad." The
# current unconditional exclusion (packetnum_anomaly_filtered_before_builder)
# throws all of these away regardless. This cutoff, when not None, rescues a
# packetnum_anomaly-flagged packet if its OWN low_conf_count is still below
# the given value (i.e. the payload itself looks trustworthy even though the
# packet_num field triggered the anomaly check) - same idea as
# GROUP_BUILDER_LOW_CONF_CUTOFF, just applied to a different exclusion path.
# None = fully disabled, current (unconditional exclusion) behavior.
PACKETNUM_ANOMALY_LOW_CONF_RESCUE_CUTOFF = None

# Structured dtype for get_decoded_flags()/decoded_flags_by_channel - defined
# once and reused so the empty-result case can't drift out of sync with the
# real per-sample construction in _decode_packet_groups().
DECODED_FLAGS_DTYPE = np.dtype([
    ('changeofstrength_v1', bool), ('changeofstrength_v2', bool),
    ('packetnum_anomaly_v1', bool), ('packetnum_anomaly_v2', bool),
    ('low_conf_v1', bool), ('low_conf_v2', bool),
    ('superseded_v1', bool), ('superseded_v2', bool),
])

# Structured dtype for get_decoded_bits()/decoded_bits_by_channel - raw
# pre-decode bits for both duplicate copies, for ground-truth (e.g. PRBS)
# ber comparison rather than duplicate-copy-agreement proxy comparison.
# v1_bits/v2_bits are zero-filled (not meaningful) wherever the
# corresponding *_missing flag is True - check missing before using bits.
DECODED_BITS_WIDTH = 20  # payload bits per channel per packet (see
                          # _decode_value_from_packet_bits's bits[:20] use
                          # elsewhere in this file)
DECODED_BITS_DTYPE = np.dtype([
    ('v1_bits', np.uint8, (DECODED_BITS_WIDTH,)),
    ('v2_bits', np.uint8, (DECODED_BITS_WIDTH,)),
    ('v1_missing', bool),
    ('v2_missing', bool),
    # Exact raw word position (same abs_word/bit_clock_hz domain as
    # _raw_frame_log and the raw logged Outports - push1/packetnum1/
    # accept_offset/use_last_good_dbg/low_conf_count) this sample's frame
    # started at. -1 wherever *_missing is True (no real frame to point
    # to). Use THIS for cross-referencing a decoded sample against raw
    # signals/scope time - idx/output_rate_hz is only a nominal-cadence
    # approximation and drifts from true time (confirmed - see PRBS
    # debugging session), it is NOT a substitute for this field.
    ('v1_word_pos', np.int64),
    ('v2_word_pos', np.int64),
    # Raw (unthresholded) low_conf_count per copy - packet-level, same
    # value replicated across all 4 channels for a given packet (see
    # pkt_low_conf_count in _extract_channel_packets), so any channel's
    # copy is representative. Meaningless (0) wherever *_missing is True.
    # Unlike get_decoded_flags()'s low_conf_v1/v2 (thresholded boolean at
    # LOW_CONF_COUNT_THRESHOLD), this is the raw count, for quality-aware
    # v1-vs-v2 arbitration rather than a fixed cutoff.
    ('v1_low_conf', np.uint8),
    ('v2_low_conf', np.uint8),
    # Raw, self-reported packet_num (0-7) each copy's own DecodedPacket
    # actually carried when it was placed into this slot (2026-08-02).
    # NOT derived from the slot index (s / s+4) - that would be circular,
    # since a packet can only reach slot s in the first place because
    # _build_group_with_placeholders already required
    # pending[0].packet_num == s before accepting it (see that function's
    # own header). Exposed here specifically so a caller who does NOT want
    # to trust the group-builder's own slot-assignment invariant can
    # independently verify it against the actual decoded field, rather
    # than assuming packet_num_v1==s/packet_num_v2==s+4 by construction.
    # Meaningless (0, same as a valid packet_num=0 would read) wherever
    # *_missing is True - check missing before trusting this like every
    # other field here.
    ('v1_packet_num', np.uint8),
    ('v2_packet_num', np.uint8),
])

# GCS credentials file path - resolved lazily in _init_gcs_clients(), not at
# import time. This used to run unconditionally on import (printing a
# warning on every offline .mat-decode script that imports this module,
# e.g. compare_prbs_ber.py, regardless of enable_gcs) - moved to only run
# when a reader with enable_gcs=True actually initializes GCS clients.
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'ueegproject-aea2731f9c3a.json')

class TimeStampBasedReader:
    # Consecutive frames where detected pkt# agrees with the word-distance
    # expectation before we trust distance to override the noisy pkt# field.
    # Two full 8-packet groups of clean agreement = confident phase lock.
    PHASE_LOCK_THRESHOLD = 16
    # Leaky mismatch run (mismatch +1, agreement -1) that, once reached while
    # locked, drops the lock to re-acquire phase. One group of net mismatch is a
    # real phase shift, not isolated bit-error noise.
    PHASE_UNLOCK_THRESHOLD = 8

    def __init__(
        self,
        sample_rate=8e6,
        frequency=914.5e6,
        gain=25,
        gain_mode='manual',
        counter=False,
        raw=False,
        device=1,
        bandwidth=5e6,
        gcs_bucket=None,
        gcs_blob_name=None,
        gcs_buffer_size=400,
        gcs_channels=(1, 2, 3, 4),
        gcs_format='binary',
        enable_gcs_trigger=False,
        gcs_trigger_topic_id='sdr-commands',
        gcs_trigger_subscription_id='sdr-commands-pi-sub',
        gcs_trigger_pull_timeout=.5,
        enable_plotting=True,
        enable_gcs=False,
        enable_bandpass_filter=True,
        enable_window_stats=False,
        buffer_size=65536,
        frame_length=250,
        accepted_frame_lengths=None,
        bit_clock_hz=100_000,
        frame_length_counts=None,
        bits_per_channel=40,
        channel_to_decode=3,
        plot_channels=None,
        save_plot_csv=True,
        mismatch_threshold=0.001,
        decode_scale=1/512*.3,#0.03 / 256 * 2,
        decoded_group_maxlen=5000,
        group_builder_low_conf_cutoff=None,
        quiet=False,
        reader_label='ant1',
        rx_channel=0,
        bladerf_identifier=None,
        block_resume_after_unclean_exit=False,
        disable_header_drops=True,
        raw_packet_dump_path=None,
    ):
        self.quiet = bool(quiet)  # suppress per-buffer decode warnings (e.g. for secondary reader)
        self.disable_header_drops = bool(disable_header_drops)
        self.block_resume_after_unclean_exit = bool(block_resume_after_unclean_exit)
        # Opt-in, off by default: dump every RAW extracted packet (both
        # "copies", before group_builder assigns them to v1/v2 slots) to a
        # local CSV as they're extracted from the live BladeRF stream - a
        # post-processing safety net for the group_builder packet_num
        # swap/duplication issue (2026-07-27), so pairing can be
        # reconstructed offline from ground-truth per-packet data instead of
        # trusting the live grouping. Local file, NOT routed through the GCS
        # write path - that pipeline is untouched and keeps writing the
        # normal processed output to the cloud as before.
        #
        # raw_packet_dump_path is a BASE path, not the literal filename - a
        # session timestamp is inserted before the extension so an
        # autorestart (systemd, watchdog, crash-recovery, etc.) always gets
        # its own fresh file instead of appending into the previous
        # session's, which would otherwise silently collide: word_pos/t_s
        # both reset to 0 on every fresh reader instance (relative to this
        # instance's own _words_processed_total), so two sessions appended
        # into one file would have overlapping, ambiguous position/time
        # values with no marker showing where one session ends and the next
        # begins.
        self.raw_packet_dump_path = raw_packet_dump_path
        self._raw_packet_dump_file = None
        if self.raw_packet_dump_path:
            _base, _ext = os.path.splitext(self.raw_packet_dump_path)
            _session_id = time.strftime('%Y%m%d_%H%M%S') + f'_{int((time.time() % 1) * 1000):03d}'
            self.raw_packet_dump_path = f'{_base}_{_session_id}{_ext or ".csv"}'
            self._raw_packet_dump_file = open(self.raw_packet_dump_path, 'a', newline='')
            if self._raw_packet_dump_file.tell() == 0:
                self._raw_packet_dump_file.write(
                    'word_pos,t_s,channel,packet_num,is_valid,low_conf_count,reason,bits\n'
                )
                self._raw_packet_dump_file.flush()
            if not self.quiet:
                print(f'Raw packet dump: {self.raw_packet_dump_path}')
        self.reader_label = str(reader_label)
        self.sample_rate = int(sample_rate)
        self.frequency = int(frequency)
        self.gain = gain
        self.gain_mode = gain_mode
        self.is_counter = bool(counter)
        self.is_raw = bool(raw)
        self.device_num = int(device)
        self.rx_channel = int(rx_channel)
        self.bladerf_identifier = bladerf_identifier  # e.g. "*:serial=abc123" or "*:instance=1"
        self.bandwidth = int(bandwidth)
        self.buffer_size = int(buffer_size)

        self.enable_plotting = bool(enable_plotting)
        self.enable_gcs = bool(enable_gcs)
        self.enable_bandpass_filter = bool(enable_bandpass_filter)
        self.enable_window_stats = bool(enable_window_stats)

        self.gcs_bucket = gcs_bucket
        self.gcs_blob_name = gcs_blob_name
        self.gcs_buffer_size = gcs_buffer_size
        self.gcs_channels = tuple(sorted(set(int(ch) for ch in gcs_channels if 1 <= int(ch) <= 4)))
        if len(self.gcs_channels) == 0:
            self.gcs_channels = (1, 2, 3, 4)
        self.gcs_format = 'binary'
        self.enable_gcs_trigger = bool(enable_gcs_trigger)
        self.gcs_trigger_topic_id = gcs_trigger_topic_id
        self.gcs_trigger_subscription_id = gcs_trigger_subscription_id
        self.gcs_trigger_pull_timeout = gcs_trigger_pull_timeout

        self.gcs_client = None
        self.gcs_bucket_obj = None
        self.gcs_subscriber = None
        self.gcs_recording_active = False
        self.gcs_write_buffer = []  # list of (values_row_f32, quality_packed_u16)
        self.gcs_chunk_counter = 0
        self._last_gcs_flush_time = 0.0  # wall-clock time of last successful flush
        self.gcs_session_id = None
        self.gcs_trigger_thread = None
        self._gcs_trigger_duration = None
        self._gcs_recording_start_time = None
        self._gcs_buffer_lock = threading.RLock()
        self.gcs_temp_name = f"{self.gcs_blob_name}.temp" if self.gcs_blob_name else None
        self.gcs_samples_written = 0
        # Per-channel last-good value for NaN carry-forward (shape (4,) per channel)
        self._gcs_last_good_values = {ch: np.int32(0) for ch in range(1, 5)}
        self._carry_forward_log_count = 0
        self._carry_forward_log_max = 50

        self.frame_length = int(frame_length)
        if accepted_frame_lengths is None:
            accepted_frame_lengths = (self.frame_length,)
        self.accepted_frame_lengths = tuple(sorted(set(int(v) for v in accepted_frame_lengths)))
        if len(self.accepted_frame_lengths) == 0:
            raise ValueError('accepted_frame_lengths must contain at least one value')
        self.bit_clock_hz = int(bit_clock_hz)
        # frame_length_counts: dict of {frame_length: count_in_repeating_pattern}
        # e.g. {250: 18, 248: 1} for 18 gaps of 250 words and 1 gap of 248 per cycle.
        # If None, uses frame_length as the average (nominal rate).
        self.frame_length_counts = {int(k): int(v) for k, v in frame_length_counts.items()} \
            if frame_length_counts else None
        self.bits_per_channel = int(bits_per_channel)
        self.channel_to_decode = int(channel_to_decode)
        self.mismatch_threshold = float(mismatch_threshold)
        self.decode_scale = float(decode_scale)

        if self.channel_to_decode < 1 or self.channel_to_decode > 4:
            raise ValueError('channel_to_decode must be 1..4')

        # Channels stacked as subplots by the post-capture plot. Defaults to the
        # decode channel; set to a list (e.g. [1, 2, 3, 4]) to plot several at once.
        if plot_channels is None:
            self.plot_channels = [self.channel_to_decode]
        elif isinstance(plot_channels, (list, tuple, set)):
            self.plot_channels = [self._normalize_channel_index(c, self.channel_to_decode)
                                  for c in plot_channels]
        else:
            self.plot_channels = [self._normalize_channel_index(plot_channels, self.channel_to_decode)]

        # When False, the post-capture plot is shown but no per-channel CSV is written.
        self.save_plot_csv = bool(save_plot_csv)

        self.running = False
        self._rx_running = False
        self._sdr_restart_log = []  # list of {sample_idx, timestamp_utc, drop_rate, reason}
        self._rx_thread_ref = None
        self._proc_thread_ref = None
        self.sdr_watchdog_window_seconds = 20
        self.sdr_restart_drop_threshold = 0.50
        self.sdr_restart_rate_upper_factor = 1.10  # restart if rate > expected * this
        self.sdr_restart_rate_lower_factor = 0.50  # restart if rate < expected * this
        # Rate is measured per-window from a bursty producer, so a single stall in
        # one window is followed by a catch-up burst in the next (e.g. a GCS write
        # 504 holds back counting, then the backlog flushes). Either window alone
        # can fall out of band while the two-window average is fine. Require this
        # many *consecutive* out-of-band windows before restarting on a rate
        # excursion, so a lone stall/catch-up pair averages out.
        self.sdr_restart_rate_consecutive_windows = 2

        self._last_decoded_sample_time = None      # wall-clock time of most recent quality append
        self._last_chunk_end_word_timestamp = None  # wall-clock end of last consumed chunk
        self._current_chunk_first_word_timestamp = None  # wall-clock start of chunk being extracted

        # Blob chain tracking — populated on resume from a state file
        self._restart_count = 0
        self._original_blob_name = None   # the very first blob name in this session chain
        self._previous_blob_name = None   # the blob that preceded this one

        self.device = None
        self.channel = None
        self.channel2 = None

        self.data_queue = queue.Queue(maxsize=64)
        # None (default) keeps the module-level GROUP_BUILDER_LOW_CONF_CUTOFF
        # constant (=5, calibrated 2026-07-26 - see that constant's own
        # docstring) - pass an explicit value per-instance to override
        # without touching the shared default other callers rely on.
        self.group_builder_low_conf_cutoff = (
            GROUP_BUILDER_LOW_CONF_CUTOFF if group_builder_low_conf_cutoff is None
            else group_builder_low_conf_cutoff)
        self.decoded_group_maxlen = int(decoded_group_maxlen)
        self.decoded_groups_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        self.decoded_quality_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        # Per-sample characterization diagnostics (changeofstrength/packetnum
        # anomaly/low_conf, both copies) - parallel to decoded_quality_by_channel,
        # same group-append cadence, not part of the persisted quality_packed/GCS
        # path. See get_decoded_flags().
        self.decoded_flags_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        # Raw pre-decode bits for both copies, for ground-truth (PRBS) BER
        # comparison - same parallel structure as decoded_flags_by_channel.
        # See get_decoded_bits().
        self.decoded_bits_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        self.decoded_sample_count_by_channel = {ch: 0 for ch in range(1, 5)}
        self.mismatch_events_by_channel = {
            ch: deque(maxlen=2000) for ch in range(1, 5)
        }
        self.bit_mismatch_events_by_channel = {
            ch: deque(maxlen=2000) for ch in range(1, 5)
        }
        self.payload_short_log_by_channel = {
            ch: deque(maxlen=500) for ch in range(1, 5)
        }
        # Histogram of how many valid_flag==1 bits were extracted per packet per channel.
        # Key = bit count, value = number of packets with that count.
        # Expected peak at bits_per_channel/2 (=20 for the default 40-bit, 50% duty-cycle config).
        self.valid_flag_bitcount_hist_by_channel = {
            ch: Counter() for ch in range(1, 5)
        }
        self.only_side_cause_counts_by_channel = {
            ch: Counter() for ch in range(1, 5)
        }
        self.only_side_missing_packetnum_by_channel = {
            ch: {
                'for_only_v1': np.zeros(8, dtype=np.int64),
                'for_only_v2': np.zeros(8, dtype=np.int64),
            }
            for ch in range(1, 5)
        }
        self.resync_drops_by_channel = {ch: 0 for ch in range(1, 5)}
        self.packet_sequence_events = deque(maxlen=2000)
        self.packet_sequence_anomaly_count = 0
        self.packet_sequence_header_drops = 0
        self.prefix_overlap_frames_skipped = 0
        self.placeholder_inserts_cross_chunk = 0
        self.placeholder_inserts_intra_chunk = 0
        self.placeholder_inserts_group_builder = 0
        self.low_conf_filtered_before_builder = 0  # packets excluded from the group-builder
            # queue entirely because low_conf_count>=LOW_CONF_COUNT_THRESHOLD (2026-07-26) -
            # counted separately from placeholder_inserts_group_builder so the two causes
            # (low-confidence exclusion vs a genuine distance-justified gap found inside
            # group_builder itself) aren't conflated when reporting why a slot is missing.
        self.packetnum_anomaly_filtered_before_builder = 0  # packets excluded from the
            # group-builder queue because packetnum_anomaly_flag was True (2026-08-02) -
            # same rationale/precedent as low_conf_filtered_before_builder above, but for
            # packets whose packet_num field was independently flagged unreliable even
            # when low_conf_count itself was low (confirmed via a real mispairing: v1/v2
            # sample paired 2.5ms apart instead of the normal 10ms, with
            # packetnum_anomaly_flag=True on both copies and low_conf_count=0 on both -
            # the existing low_conf-only filter didn't catch this case).
        self.packetnum_anomaly_rescued_by_low_conf = 0  # packets that WOULD have been
            # excluded by packetnum_anomaly_filtered_before_builder above, but were kept
            # instead because PACKETNUM_ANOMALY_LOW_CONF_RESCUE_CUTOFF is set and this
            # packet's own low_conf_count was below it (2026-08-03, experimental - see
            # that constant's own comment). Always 0 when the rescue cutoff is None.
        self.superseded_filtered_before_builder = 0  # packets excluded from the
            # group-builder queue because superseded_flag was True (2026-08-02) - same
            # rationale as the two filters above: this packet's own accept was abandoned
            # mid-payload in favor of a stronger competing peak (searchPeak2_hdl.m), so
            # its packet_num/content shouldn't be trusted as a sequence anchor either.
        self.packet_num_distance_corrections = 0  # frames relabeled from noisy pkt# to distance value
        self._phase_locked = False           # when locked, distance overrides noisy pkt# field
        self._phase_agreement_run = 0        # consecutive detect-vs-distance agreements toward lock
        self._phase_mismatch_run = 0         # leaky mismatch run toward re-acquire unlock
        self.phase_relocks = 0               # times the phase lock was dropped to re-acquire
        self._group_builder_input_log = []   # (packet_num, is_valid) for ch1, capped at 100k
        self._group_builder_decision_log = []  # per-slot decision trace for ch1, capped at 100k -
            # each entry: expected/pending_front_pkt/action(match|distance_accept_mismatch|
            # placeholder)/accepted_pkt_num/distance_words/expected_frames - see
            # _build_group_with_placeholders. Added 2026-07-26 to trace exactly how a
            # confirmed real anomaly (a live recording producing garbled packet_num
            # sequences like {4,5,6,7,1,2,3,4} instead of the expected {0,1,...,7} cycle)
            # actually happens, one slot-fill decision at a time.
        self.gap_estimate_agree_count = 0
        self.gap_estimate_disagree_count = 0
        self.decoded_groups = self.decoded_groups_by_channel[self.channel_to_decode]
        self.decoded_quality = self.decoded_quality_by_channel[self.channel_to_decode]

        self._decode_buffer = np.array([], dtype=np.uint16)
        # deque, not list (2026-08-03): _build_group_with_placeholders drains
        # these from the FRONT every group (pending.pop(0)/positions.pop(0)) -
        # on a plain list that's O(n) per pop, shifting every remaining
        # element down. Profiled as 72% of total decode() runtime (57.6s of
        # 79.3s on a 229s recording) via cProfile - deque.popleft() is O(1).
        self._pending_packets_by_channel = {ch: deque() for ch in range(1, 5)}
        self._synced_to_packet0_by_channel = {ch: False for ch in range(1, 5)}
        self._last_extracted_packet_num = None  # persists across chunk calls for cross-chunk gap detection
        self._last_extracted_frame_abs_word_start = None  # absolute word start of last extracted valid frame
        self._extract_lookback_words = np.array([], dtype=np.uint16)  # preserves 2-word context for -2 bit alignment
        self._words_processed_total = 0       # absolute word offset for timestamps
        self._raw_frame_log = []              # list of (abs_word_idx, packet_num, frame_length, passed_valid)
        self._raw_packet_bits_log = {ch: [] for ch in range(1, 5)}  # list of (abs_word_idx, 20-bit array) per
            # channel, real (is_valid=True) packets ONLY, logged before group_builder/any pre-filter - see the
            # single append site's own comment (2026-08-03, for "raw BER" computed independent of packet-level
            # accept/reject decisions)
        self._word_timestamps = []  # Stores timestamp for each word in decode buffer
        # Set to (start_s, end_s) to print raw packet bits and decoded ints for every
        # sample whose time (sample_idx / output_rate_hz) falls in the window.
        self.debug_packet_window = None
        self.gcs_timestamp_log = []  # List of {gcs_sample_idx, timestamp_utc, system_time_s}
        self.gcs_timestamp_log_interval = 12000  # Log timestamp every 12000 samples (60 seconds at 200 Hz)
        self._pending_packet_word_positions = {ch: deque() for ch in range(1, 5)}  # Track word positions for packets - deque, same reason as _pending_packets_by_channel above
        self._first_group_skipped = False  # Discard first decoded group (startup artifact)
        self._force_timestamp_after_restart = False  # Set True after SDR restart to force a checkpoint
        self.capture_start_time = None
        self.samples_captured = 0

        # Dual-antenna mode: attach a second TimeStampBasedReader to decode the other
        # antenna stream (opposite I/Q of RX0) from the same BladeRF capture.
        # Set this before calling start_capture(). The secondary reader must NOT have
        # setup_device() or start_capture() called on it — the primary reader drives
        # hardware and feeds data into secondary_reader.data_queue automatically.
        self.secondary_reader = None
        self._secondary_proc_thread = None

        if self.frame_length_counts:
            total_words = sum(fl * cnt for fl, cnt in self.frame_length_counts.items())
            total_gaps = sum(self.frame_length_counts.values())
            avg_frame_length = total_words / total_gaps
        else:
            avg_frame_length = float(self.frame_length)
        # 4 EEG samples per group of 8 packets; each inter-packet gap = avg_frame_length words
        self.output_rate_hz = self.bit_clock_hz * 4.0 / (8.0 * avg_frame_length)
        self._init_filter()

        if self.enable_gcs and str(gcs_format).lower() != 'binary':
            print(f"⚠️  gcs_format='{gcs_format}' is not supported in this reader. Forcing 'binary'.")
        if self.enable_gcs and not self.enable_gcs_trigger:
            self.enable_gcs_trigger = True
            print('⚠️  Trigger-only mode enabled for GCS. Recording will start only after a trigger message.')
        if self.is_raw:
            print('⚠️  raw=True is not supported in this timestamp-based decoder; treating stream as packet mode.')

    def _init_gcs_clients(self):
        if not self.enable_gcs:
            return
        if not self.gcs_bucket or not self.gcs_blob_name:
            raise ValueError('enable_gcs=True requires gcs_bucket and gcs_blob_name.')

        if os.path.exists(CREDENTIALS_FILE):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_FILE
            print(f"✓ GCS credentials loaded from: {CREDENTIALS_FILE}")
        else:
            print(f"⚠️  GCS credentials file not found: {CREDENTIALS_FILE}")
            print("   GCS functionality will be disabled unless credentials are set via gcloud auth")

        try:
            from google.cloud import storage, pubsub_v1
        except Exception as exc:
            raise ImportError(f'Google Cloud packages not available: {exc}')

        if self.gcs_client is None:
            self.gcs_client = storage.Client()
            self.gcs_bucket_obj = self.gcs_client.bucket(self.gcs_bucket)

        if self.enable_gcs_trigger:
            if not self.gcs_trigger_subscription_id:
                raise ValueError('enable_gcs_trigger=True requires gcs_trigger_subscription_id.')
            if self.gcs_subscriber is None:
                self.gcs_subscriber = pubsub_v1.SubscriberClient()

    @property
    def _RECORDING_STATE_FILE(self):
        # Keyed on reader_label (fixed at construction) so the path is stable even if
        # gcs_blob_name is updated at runtime by a Pub/Sub trigger message.
        safe = self.reader_label.replace('/', '_').replace('.', '_')
        return f'/tmp/sdr_recording_state_{safe}.json'

    def _write_recording_state_file(self):
        """Persist current recording parameters to disk so they survive a process restart."""
        projected_end = None
        if self._gcs_trigger_duration is not None and self._gcs_recording_start_time is not None:
            projected_end = self._gcs_recording_start_time + self._gcs_trigger_duration
        state = {
            'session_id': self.gcs_session_id,
            'blob_name': self.gcs_blob_name,
            'start_time_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self._gcs_recording_start_time))
                if self._gcs_recording_start_time else None,
            'start_time_unix': self._gcs_recording_start_time,
            'duration_seconds': self._gcs_trigger_duration,
            'projected_end_time_unix': projected_end,
            'restart_count': self._restart_count,
            'original_blob_name': self._original_blob_name or self.gcs_blob_name,
        }
        try:
            with open(self._RECORDING_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as exc:
            print(f'Warning: could not write recording state file: {exc}')

    def _delete_recording_state_file(self):
        """Remove the on-disk recording state (called on normal stop or duration expiry)."""
        try:
            import os
            os.remove(self._RECORDING_STATE_FILE)
        except FileNotFoundError:
            pass
        except Exception as exc:
            print(f'Warning: could not delete recording state file: {exc}')

    def _check_resume_recording(self):
        """On startup, resume an in-progress recording if a valid state file exists."""
        try:
            with open(self._RECORDING_STATE_FILE) as f:
                state = json.load(f)
        except FileNotFoundError:
            return
        except Exception as exc:
            print(f'Warning: could not read recording state file: {exc}')
            return

        if self.block_resume_after_unclean_exit:
            print(
                f'WARNING: Unclean exit detected (state file: {self._RECORDING_STATE_FILE}). '
                f'block_resume_after_unclean_exit is set — recording will NOT auto-resume. '
                f'To clear this block and allow recording, delete the state file:\n'
                f'  rm {self._RECORDING_STATE_FILE}'
            )
            return

        projected_end = state.get('projected_end_time_unix')
        now = time.time()

        if projected_end is not None and now > projected_end:
            print('Recording state file found but window already elapsed — discarding.')
            self._delete_recording_state_file()
            return

        blob_name = state.get('blob_name')
        session_id = state.get('session_id')
        duration = state.get('duration_seconds')
        start_unix = state.get('start_time_unix')

        # Derive a new blob name — state file presence means the previous run was
        # unclean (signal loss, power failure, OOM kill). Never append to the old blob;
        # start a fresh one so the gap is explicit rather than silent.
        restart_count = int(state.get('restart_count', 0)) + 1
        original = state.get('original_blob_name') or blob_name or self.gcs_blob_name
        if original and '.' in original.split('/')[-1]:
            base, ext = original.rsplit('.', 1)
            new_blob = f'{base}_r{restart_count}.{ext}'
        elif original:
            new_blob = f'{original}_r{restart_count}'
        else:
            new_blob = self.gcs_blob_name  # fallback: no blob name known

        self._restart_count = restart_count
        self._original_blob_name = original
        self._previous_blob_name = blob_name
        self.gcs_blob_name = new_blob
        self.gcs_temp_name = f'{new_blob}.temp'

        print(
            f'Resuming after unclean exit: previous blob={blob_name}, '
            f'new blob={new_blob} (restart #{restart_count}).'
        )
        self._start_gcs_recording()
        # Restore duration/start fields if a timed session was in progress.
        with self._gcs_buffer_lock:
            if duration is not None and start_unix is not None:
                self._gcs_trigger_duration = duration
                self._gcs_recording_start_time = start_unix
        self._sdr_restart_log.append({
            'timestamp_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'sample_idx_at_restart': 0,
            'drop_rate_pct': None,
            'reason': 'process_restart_resume',
            'previous_blob': blob_name,
            'new_blob': new_blob,
        })

    def _start_gcs_recording(self):
        if self.enable_gcs and self.gcs_bucket_obj is None:
            print('WARNING: GCS start trigger received but GCS client is not initialised — recording NOT started.')
            return
        with self._gcs_buffer_lock:
            if self.gcs_recording_active:
                return  # already recording; ignore duplicate trigger
            self.gcs_recording_active = True
            self.gcs_write_buffer = []
            self.gcs_chunk_counter = 0
            self.gcs_session_id = time.strftime('%Y%m%d_%H%M%S')
            self.gcs_samples_written = 0
            self.gcs_timestamp_log = []
            self._gcs_recording_start_time = time.time()
            self._gcs_last_good_values = {ch: np.int32(0) for ch in range(1, 5)}
            self._carry_forward_log_count = 0
        # Update blob/temp names from trigger message if blob was updated
        self.gcs_temp_name = f"{self.gcs_blob_name}.temp" if self.gcs_blob_name else None
        print(f'GCS recording started (session={self.gcs_session_id}, blob={self.gcs_blob_name}).')
        self._write_recording_state_file()

    # Blobs with fewer than this many samples on unplanned exit are treated as
    # near-empty and deleted rather than kept as orphaned stubs.
    _NEAR_EMPTY_SAMPLE_THRESHOLD = 400  # one GCS chunk

    def _stop_gcs_recording(self, intentional: bool = False):
        with self._gcs_buffer_lock:
            was_active = bool(self.gcs_recording_active)
            self.gcs_recording_active = False
            self._gcs_recording_start_time = None
        if not was_active:
            return
        try:
            self._flush_gcs_buffer(force=True)
        except Exception as exc:
            print(f'GCS flush error during stop: {exc}')

        if intentional:
            self._delete_recording_state_file()
            print('GCS recording stopped.')
            return

        # Unplanned exit path — keep state file so monitor can trigger a new-blob resume.
        # But if this blob is near-empty, delete it and roll back the restart counter so
        # the next resume retries the same blob name rather than creating another stub.
        if self.gcs_samples_written < self._NEAR_EMPTY_SAMPLE_THRESHOLD:
            blob_name = self.gcs_blob_name
            if blob_name and self.gcs_bucket_obj is not None:
                try:
                    b = self.gcs_bucket_obj.blob(blob_name)
                    if b.exists():
                        b.delete()
                        print(f'Deleted near-empty blob ({self.gcs_samples_written} samples): gs://{self.gcs_bucket}/{blob_name}')
                except Exception as exc:
                    print(f'Warning: could not delete near-empty blob: {exc}')

            if self._restart_count == 0:
                # This was the very first blob — no prior data to resume from.
                self._delete_recording_state_file()
                print('GCS recording stopped (near-empty original blob removed).')
                return

            # Roll back so the next resume retries this blob name instead of advancing.
            self._restart_count = max(0, self._restart_count - 1)
            orig = self._original_blob_name or blob_name
            if self._restart_count == 0:
                predecessor = orig
            else:
                if '.' in orig.split('/')[-1]:
                    base, ext = orig.rsplit('.', 1)
                    predecessor = f'{base}_r{self._restart_count}.{ext}'
                else:
                    predecessor = f'{orig}_r{self._restart_count}'
            self.gcs_blob_name = predecessor
            self.gcs_temp_name = f'{predecessor}.temp'
            print(f'Near-empty blob removed; rolled back to predecessor ({predecessor}).')

        self._write_recording_state_file()
        print('GCS recording stopped.')

    def _handle_gcs_trigger_message(self, payload: str):
        text = (payload or '').strip()
        # Default: treat plain text as a command word
        command = text.lower()
        msg = {}
        try:
            msg = json.loads(text)
            # Support both "command" and "action" keys
            command = str(msg.get('command', msg.get('action', command))).lower()
        except Exception:
            pass

        # Optionally update blob name and session duration from message
        if 'blob' in msg and msg['blob']:
            self.gcs_blob_name = str(msg['blob'])
            print(f'GCS blob name updated to: {self.gcs_blob_name}')
            if self.secondary_reader is not None:
                base = str(msg['blob'])
                dot = base.rfind('.')
                sec_blob = (base[:dot] + '_ant2' + base[dot:]) if dot != -1 else (base + '_ant2')
                self.secondary_reader.gcs_blob_name = sec_blob
                self.secondary_reader.gcs_temp_name = f'{sec_blob}.temp'
                print(f'GCS blob name for antenna 2 updated to: {sec_blob}')
        _dur_raw = msg.get('duration_seconds', msg.get('duration'))
        if _dur_raw is not None:
            try:
                self._gcs_trigger_duration = float(_dur_raw)
                print(f'GCS trigger duration set to: {self._gcs_trigger_duration}s')
                if self.secondary_reader is not None:
                    self.secondary_reader._gcs_trigger_duration = self._gcs_trigger_duration
            except Exception:
                pass

        if command in ('start', 'record', 'resume'):
            self._start_gcs_recording()
            if self.secondary_reader is not None:
                self.secondary_reader._start_gcs_recording()
        elif command in ('stop', 'pause', 'end'):
            self._stop_gcs_recording(intentional=True)
            if self.secondary_reader is not None:
                self.secondary_reader._stop_gcs_recording(intentional=True)

    def _poll_gcs_triggers(self):
        if not self.enable_gcs or not self.enable_gcs_trigger or self.gcs_subscriber is None:
            return
        subscription_path = self.gcs_subscriber.subscription_path(
            self.gcs_client.project,
            self.gcs_trigger_subscription_id,
        )
        print(f'GCS trigger poller started (subscription={subscription_path})')
        while self.running:
            try:
                response = self.gcs_subscriber.pull(
                    request={
                        'subscription': subscription_path,
                        'max_messages': 10,
                    },
                    timeout=float(self.gcs_trigger_pull_timeout),
                )
                ack_ids = []
                for received in response.received_messages:
                    ack_ids.append(received.ack_id)
                    data = received.message.data.decode('utf-8', errors='ignore')
                    print(f'GCS trigger message received: {data!r}')
                    self._handle_gcs_trigger_message(data)
                if ack_ids:
                    self.gcs_subscriber.acknowledge(request={'subscription': subscription_path, 'ack_ids': ack_ids})
                self._trigger_poll_err_count = 0
            except Exception as exc:
                _trigger_err_count = getattr(self, '_trigger_poll_err_count', 0) + 1
                self._trigger_poll_err_count = _trigger_err_count
                if _trigger_err_count == 1 or _trigger_err_count % 60 == 0:
                    print(f'GCS trigger poll error (#{_trigger_err_count}): {exc}')
                time.sleep(0.5)

            # Duration check: always runs regardless of whether the Pub/Sub pull succeeded.
            if (
                self.gcs_recording_active
                and self._gcs_trigger_duration is not None
                and self._gcs_recording_start_time is not None
                and (time.time() - self._gcs_recording_start_time) >= self._gcs_trigger_duration
            ):
                elapsed = time.time() - self._gcs_recording_start_time
                print(
                    f'GCS recording duration ({self._gcs_trigger_duration}s) elapsed '
                    f'(actual={elapsed:.1f}s) — stopping recording.'
                )
                self._stop_gcs_recording()
                if self.secondary_reader is not None:
                    self.secondary_reader._stop_gcs_recording()
                self._delete_recording_state_file()

            # When recording is active we only need to catch stop commands — a few
            # seconds of lag is fine and avoids hammering Pub/Sub every 0.5s.
            if self.gcs_recording_active:
                time.sleep(5.0)

    def _append_gcs_group(self, group_values: dict, group_raw_values: dict, group_quality: dict, group_sample_timestamps=None):
        """Append one decoded group (4 time slots) as 4 rows to the GCS write buffer.
        Each row stores selected channel raw int32 values plus one packed quality field.
        Quality codes use 4 bits per channel packed into a uint16 in gcs_channels order.
        Carry-forward fills missing slots (quality=0) from the last known good raw integer.
        Leading missing values are written as 0.
        """
        if not self.enable_gcs:
            return
        rows_to_add = []
        for s in range(4):
            vals_row = []
            packed_quality = np.uint16(0)
            for ch_idx, ch in enumerate(self.gcs_channels):
                q = int(group_quality[ch][s]) if ch in group_quality else 0
                if ch in group_raw_values and q != 0:
                    r = int(group_raw_values[ch][s])
                    self._gcs_last_good_values[ch] = np.int32(r)
                else:
                    r = int(self._gcs_last_good_values[ch])  # carry-forward
                    if self._carry_forward_log_count < self._carry_forward_log_max:
                        self._carry_forward_log_count += 1
                        _cf_float = r / (1 << 12) * self.decode_scale
                        print(f'[carry-fwd #{self._carry_forward_log_count}] GCS write ch{ch} slot{s} q={q:#04x}: repeating raw={r} ({_cf_float:.6f})')
                        if self._carry_forward_log_count == self._carry_forward_log_max:
                            print(f'[carry-fwd] log limit ({self._carry_forward_log_max}) reached, suppressing further carry-forward logs')
                vals_row.append(np.int32(r))
                packed_quality = np.uint16(packed_quality | ((q & 0xF) << (4 * ch_idx)))
            rows_to_add.append((np.asarray(vals_row, dtype=np.int32), packed_quality))

        should_flush = False
        with self._gcs_buffer_lock:
            if not self.gcs_recording_active:
                return

            # Log timestamps periodically
            if group_sample_timestamps is not None:
                current_total = self.gcs_samples_written + len(self.gcs_write_buffer)
                new_total = current_total + 4
                interval = int(self.gcs_timestamp_log_interval)
                # Always log sample 0
                if current_total == 0:
                    ts_val = group_sample_timestamps[0]
                    if not np.isnan(ts_val):
                        self.gcs_timestamp_log.append({
                            'gcs_sample_idx': 0,
                            'sample_timestamp_s': float(ts_val),
                            'system_time_s': time.time(),
                        })
                elif self._force_timestamp_after_restart:
                    ts_val = group_sample_timestamps[0]
                    if not np.isnan(ts_val):
                        self.gcs_timestamp_log.append({
                            'gcs_sample_idx': int(current_total),
                            'sample_timestamp_s': float(ts_val),
                            'system_time_s': time.time(),
                            'reason': 'sdr_restart',
                        })
                    self._force_timestamp_after_restart = False
                elif interval > 0 and (new_total // interval) > (current_total // interval):
                    milestone = (new_total // interval) * interval
                    s_idx = max(0, min(3, milestone - current_total - 1))
                    ts_val = group_sample_timestamps[s_idx]
                    if not np.isnan(ts_val):
                        self.gcs_timestamp_log.append({
                            'gcs_sample_idx': int(milestone),
                            'sample_timestamp_s': float(ts_val),
                            'system_time_s': time.time(),
                        })

            self.gcs_write_buffer.extend(rows_to_add)
            if len(self.gcs_write_buffer) >= int(self.gcs_buffer_size):
                should_flush = True
        if should_flush:
            self._flush_gcs_buffer(force=False)

    def _flush_gcs_buffer(self, force=False):
        if not self.enable_gcs or self.gcs_bucket_obj is None:
            return
        if not self.gcs_blob_name or not self.gcs_temp_name:
            return

        with self._gcs_buffer_lock:
            if len(self.gcs_write_buffer) == 0:
                return
            if not force and len(self.gcs_write_buffer) < int(self.gcs_buffer_size):
                return
            # GCS limits mutation operations to ~1/second per object. If a previous
            # flush failed and requeued data, or a large gap fill caused rapid
            # re-triggering, enforce a minimum interval so the backlog accumulates
            # into one large write instead of many small ones.
            if not force:
                since_last = time.time() - self._last_gcs_flush_time
                if since_last < 1.5:
                    return
            # Take everything — if there's a backlog we flush it all in one Compose.
            buffer_snapshot = list(self.gcs_write_buffer)
            self.gcs_write_buffer = []
            # Stamp here (commit point) so upload latency doesn't inflate the interval.
            self._last_gcs_flush_time = time.time()

        # --- START OF NEW, EFFICIENT LOGIC ---
        
        # 1. Create the new data chunk as a NumPy array
        n_channels = len(self.gcs_channels)
        row_dtype = np.dtype([
            ('values', np.int32, (n_channels,)),
            ('quality_packed', np.uint16),
        ])
        new_data_chunk = np.empty(len(buffer_snapshot), dtype=row_dtype)
        for i, (vals_row, packed_quality) in enumerate(buffer_snapshot):
            new_data_chunk['values'][i] = vals_row
            new_data_chunk['quality_packed'][i] = packed_quality

        # 2. Convert the NumPy chunk to raw bytes
        new_bytes = new_data_chunk.tobytes()

        # 3. Upload the new raw byte chunk to a temporary, unique blob
        # Using a unique name for the temp blob prevents race conditions
        temp_blob_name = f"{self.gcs_blob_name}.temp.{self.gcs_session_id}.{self.gcs_chunk_counter}"
        temp_blob = self.gcs_bucket_obj.blob(temp_blob_name)
        try:
            temp_blob.upload_from_string(new_bytes, content_type='application/octet-stream')
        except Exception as exc:
            print(f'GCS compose/append error: {exc}')
            with self._gcs_buffer_lock:
                self.gcs_write_buffer = buffer_snapshot + self.gcs_write_buffer
            return

        # 4. Use GCS Compose to append the new chunk to the main blob
        main_blob = self.gcs_bucket_obj.blob(self.gcs_blob_name)

        try:
            # Check if the main blob exists to decide whether to compose or rename
            # This is a lightweight metadata call
            if main_blob.exists():
                # Append temp_blob to the end of main_blob
                main_blob.compose([main_blob, temp_blob])
            else:
                # If it's the first chunk, just rename the temp blob to become the main blob.
                # rename_blob is a server-side copy+delete, so the temp blob will no longer
                # exist afterwards — do NOT call temp_blob.delete() after this path.
                self.gcs_bucket_obj.rename_blob(temp_blob, new_name=self.gcs_blob_name)
                main_blob = self.gcs_bucket_obj.blob(self.gcs_blob_name)
                temp_blob = None  # already gone; skip delete below
        except Exception as exc:
            print(f'GCS compose/append error: {exc}')
            # Try to clean up the orphaned temp blob, then requeue
            try:
                temp_blob.delete()
            except Exception:
                pass
            with self._gcs_buffer_lock:
                self.gcs_write_buffer = buffer_snapshot + self.gcs_write_buffer
            return

        # 5. Clean up the temporary chunk blob (only needed after compose, not after rename)
        if temp_blob is not None:
            try:
                temp_blob.delete()
            except Exception:
                pass  # 404 here just means it was already cleaned up; not an error

        # --- END OF NEW, EFFICIENT LOGIC ---

        n_samples = len(new_data_chunk)
        self.gcs_samples_written += n_samples
        self.gcs_chunk_counter += 1
        self._write_gcs_metadata() # This MUST be called after updating samples_written
        
        print(
            f"GCS append: gs://{self.gcs_bucket}/{self.gcs_blob_name} "
            f"(+{n_samples} samples, total={self.gcs_samples_written} at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})"
        )

    def _write_gcs_metadata(self):
        """Write/update metadata describing the current GCS binary layout."""
        if not self.enable_gcs or self.gcs_bucket_obj is None or not self.gcs_blob_name:
            return

        try:
            meta_blob_name = f"{self.gcs_blob_name}.meta"
            meta_blob = self.gcs_bucket_obj.blob(meta_blob_name)

            channel_names = [f"ch{ch}" for ch in self.gcs_channels]
            quality_nibble_map = {
                f"bits_{4 * idx}_{4 * idx + 3}": f"quality code for ch{ch}"
                for idx, ch in enumerate(self.gcs_channels)
            }

            metadata = {
                'format': 'numpy_binary_structured',
                'sample_rate_hz': float(self.output_rate_hz),
                'sample_rate_hz_nominal': 200.0,
                'bit_clock_hz': self.bit_clock_hz,
                'frame_length_counts': self.frame_length_counts,
                'gcs_channels': list(self.gcs_channels),
                'channel_names': channel_names,
                'row_description': 'Each row is one decoded sample time-step across selected channels.',
                'dtype': {
                    'values': f"int32[{len(self.gcs_channels)}]",
                    'quality_packed': 'uint16',
                },
                'fields': ['values', 'quality_packed'],
                'values_field_order': channel_names,
                'quality_packed_format': {
                    'bits_per_channel': 4,
                    'packing_order': channel_names,
                    'quality_code_map': {
                        '0': 'no_packet',
                        '1': 'only_v1',
                        '2': 'only_v2',
                        '3': 'both_match',
                        '5': 'mismatch_picked_v1',
                        '6': 'mismatch_picked_v2',
                    },
                    'error_flag_bit': {
                        'bit': 3,
                        'mask': '0x08',
                        'meaning': 'interference or loss-of-signal detected mid-packet; errored copy discarded, non-errored copy used as sole source (quality bits 2:0 reflect the result as if only that copy was received)',
                    },
                    'bit_layout': quality_nibble_map,
                },
                'nan_fill_policy': 'carry_forward_per_channel_per_slot; leading missing values written as 0 (int32 zero)',
                'decode_scale': self.decode_scale,  # kept for backwards compatibility
                'sample_encoding': {
                    'storage_dtype': 'int32',
                    'fixed_point_divisor': 1 << 12,
                    'scale_factor': self.decode_scale,
                    'volts_per_lsb': self.decode_scale / (1 << 12),
                    'adc_bits': 20,
                    'to_volts': 'raw_int / fixed_point_divisor * scale_factor',
                },
                'center_frequency_hz': self.frequency,
                'gcs_samples_written': int(self.gcs_samples_written),
                'gcs_chunk_counter': int(self.gcs_chunk_counter),
                'session_id': self.gcs_session_id,
                'blob_name': self.gcs_blob_name,
                'timestamp_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                'sdr_restart_log': list(self._sdr_restart_log),
                'timestamp_log': list(self.gcs_timestamp_log),
                'timestamp_log_interval_samples': int(self.gcs_timestamp_log_interval),
                'notes': 'Load with numpy.load(). Structured array field `values` contains channel amplitudes; `quality_packed` stores one 4-bit quality code per selected channel. timestamp_log provides periodic sample-accurate UTC synchronization checkpoints.',
            }
            if self._previous_blob_name:
                metadata['previous_blob'] = self._previous_blob_name
            if self._original_blob_name:
                metadata['original_blob'] = self._original_blob_name
                metadata['restart_count'] = self._restart_count

            meta_blob.upload_from_string(json.dumps(metadata, indent=2), content_type='application/json')
        except Exception as exc:
            print(f'Error writing GCS metadata: {exc}')

    def _setup_gcs(self):
        if not self.enable_gcs:
            return False
        if self.gcs_client is not None and self.gcs_bucket_obj is not None:
            return True
        if not self.gcs_bucket:
            print('⚠️  enable_gcs=True but gcs_bucket is not set.')
            return False
        try:
            from google.cloud import storage
            self.gcs_client = storage.Client()
            self.gcs_bucket_obj = self.gcs_client.bucket(self.gcs_bucket)
        except Exception as exc:
            print(f'⚠️  Could not initialize GCS storage client: {exc}')
            return False

        if self.enable_gcs_trigger and self.gcs_subscriber is None:
            try:
                from google.cloud import pubsub_v1
                self.gcs_subscriber = pubsub_v1.SubscriberClient()
            except Exception as exc:
                print(f'⚠️  Could not initialize Pub/Sub subscriber client: {exc}')
                self.gcs_subscriber = None
        return True

    def _poll_gcs_trigger(self):
        """Single-shot poll used outside the trigger thread (e.g. before an upload)."""
        if not self.enable_gcs_trigger or self.gcs_subscriber is None or not self.gcs_trigger_subscription_id:
            return
        subscription_path = self.gcs_subscriber.subscription_path(
            self.gcs_client.project,
            self.gcs_trigger_subscription_id,
        )
        try:
            response = self.gcs_subscriber.pull(
                request={
                    'subscription': subscription_path,
                    'max_messages': 10,
                },
                timeout=float(self.gcs_trigger_pull_timeout),
            )
        except Exception as exc:
            print(f'GCS trigger single-poll error: {exc}')
            return

        ack_ids = []
        for msg in response.received_messages:
            ack_ids.append(msg.ack_id)
            try:
                payload = msg.message.data.decode('utf-8') if msg.message.data else '{}'
                print(f'GCS trigger message received: {payload!r}')
                self._handle_gcs_trigger_message(payload)
            except Exception as exc:
                print(f'GCS trigger message parse error: {exc}')
        if ack_ids:
            try:
                self.gcs_subscriber.acknowledge(
                    request={
                        'subscription': subscription_path,
                        'ack_ids': ack_ids,
                    }
                )
            except Exception as exc:
                print(f'GCS trigger ack error: {exc}')

    def _upload_series_to_gcs_binary(self, series: np.ndarray, quality_series: np.ndarray = None):
        if not self.enable_gcs:
            return
        if not self._setup_gcs():
            return
        if self.enable_gcs_trigger:
            self._poll_gcs_trigger()
        if not self.gcs_recording_active:
            print('GCS trigger is enabled and recording is not active; skipping upload.')
            return
        if series is None or len(series) == 0:
            print('No decoded samples available for GCS upload.')
            return

        base_name = self.gcs_blob_name or f'decoded_ch{self.channel_to_decode}_{int(time.time())}'
        series_blob_name = base_name if base_name.endswith('.bin') else f'{base_name}.bin'

        try:
            series_blob = self.gcs_bucket_obj.blob(series_blob_name)
            series_blob.upload_from_string(np.asarray(series, dtype=np.float32).tobytes(), content_type='application/octet-stream')
            print(f'Uploaded decoded binary to gs://{self.gcs_bucket}/{series_blob_name}')

            if quality_series is not None and len(quality_series) == len(series):
                q_blob_name = series_blob_name + '.quality.i8.bin'
                q_blob = self.gcs_bucket_obj.blob(q_blob_name)
                q_blob.upload_from_string(np.asarray(quality_series, dtype=np.int8).tobytes(), content_type='application/octet-stream')
                print(f'Uploaded quality flags to gs://{self.gcs_bucket}/{q_blob_name}')
        except Exception as exc:
            print(f'⚠️  GCS upload failed: {exc}')

    def _init_filter(self):
        fs = self.output_rate_hz
        nyquist = fs / 2.0
        low = 1.0 / nyquist
        high = 40.0 / nyquist
        self.b_bandpass, self.a_bandpass = signal.butter(4, [low, high], btype='band')
        zi = signal.lfilter_zi(self.b_bandpass, self.a_bandpass) * 0
        self.filter_zi = [zi.copy(), zi.copy(), zi.copy(), zi.copy()]

    def _recent_drop_rate(self):
        """Fraction of samples in the last watchdog window that have quality 0 (no packet).

        Returns 1.0 if no new samples have arrived within the watchdog window,
        which catches total signal loss that would otherwise leave the deque stale.
        """
        window = int(self.sdr_watchdog_window_seconds * self.output_rate_hz)
        quality_deque = self.decoded_quality_by_channel[self.channel_to_decode]

        # Total silence: deque never had data or no new samples within the window.
        if self._last_decoded_sample_time is None:
            return 0.0  # haven't started yet, don't trigger early
        silence_s = time.time() - self._last_decoded_sample_time
        if silence_s >= self.sdr_watchdog_window_seconds:
            return 1.0

        if len(quality_deque) == 0:
            return 0.0
        recent = list(quality_deque)[-window:]
        q = np.concatenate([np.asarray(g, dtype=np.int8) for g in recent]).reshape(-1)
        if q.size == 0:
            return 0.0
        return float(np.sum(q == 0)) / float(q.size)

    def _watchdog_thread_func(self):
        """Checks drop rate every watchdog_window_seconds; exits the process if too high."""
        cycle = 0
        count_before = self.decoded_sample_count_by_channel[self.channel_to_decode]
        t_before = time.time()
        rate_high_strikes = 0  # consecutive windows above the upper rate limit
        rate_low_strikes = 0   # consecutive windows below the lower rate limit

        while self.running:
            for _ in range(int(self.sdr_watchdog_window_seconds * 4)):
                if not self.running:
                    return
                time.sleep(0.25)
            if not self.running:
                return

            count_after = self.decoded_sample_count_by_channel[self.channel_to_decode]
            t_after = time.time()
            elapsed = t_after - t_before
            samples_this_window = count_after - count_before
            measured_rate_hz = samples_this_window / elapsed if elapsed > 0 else 0.0
            count_before = count_after
            t_before = t_after

            cycle += 1
            ts = time.strftime('%H:%M:%S', time.gmtime())

            drop_rate = self._recent_drop_rate()
            window = int(self.sdr_watchdog_window_seconds * self.output_rate_hz)
            quality_deque = self.decoded_quality_by_channel[self.channel_to_decode]
            actual_samples = min(len(list(quality_deque)), window)

            silence_s = (time.time() - self._last_decoded_sample_time) if self._last_decoded_sample_time else None
            silence_info = f'  silence={silence_s:.1f}s' if silence_s is not None and silence_s >= 1.0 else ''

            rate_pct = 100.0 * measured_rate_hz / self.output_rate_hz if self.output_rate_hz > 0 else 0.0

            sec_info = ''
            if self.secondary_reader is not None:
                sec_has_data = any(len(q) > 0 for q in self.secondary_reader.decoded_quality_by_channel.values())
                if sec_has_data:
                    sec_drop_rate = self.secondary_reader._recent_drop_rate()
                    sec_info = f'  secondary_drop={sec_drop_rate*100:.1f}%'

            print(
                f'[{ts}] Watchdog cycle {cycle}: drop={drop_rate*100:.1f}%'
                f'  threshold={self.sdr_restart_drop_threshold*100:.0f}%'
                f'  rate={measured_rate_hz:.1f}Hz ({rate_pct:.0f}% of {self.output_rate_hz:.1f}Hz)'
                f'  window={actual_samples}/{window} samples{silence_info}{sec_info}'
            )

            if drop_rate > self.sdr_restart_drop_threshold:
                if self.secondary_reader is not None:
                    sec_has_data = any(len(q) > 0 for q in self.secondary_reader.decoded_quality_by_channel.values())
                    if sec_has_data:
                        sec_drop_rate = self.secondary_reader._recent_drop_rate()
                        if sec_drop_rate <= self.sdr_restart_drop_threshold:
                            print(
                                f'⚠️  Watchdog: primary drop rate {drop_rate*100:.1f}% is high but '
                                f'secondary is healthy ({sec_drop_rate*100:.1f}%) — not exiting.'
                            )
                            continue
                print(
                    f'⚠️  Watchdog: drop rate {drop_rate*100:.1f}% > '
                    f'{self.sdr_restart_drop_threshold*100:.0f}% threshold — exiting for monitor-driven restart.'
                )
                self.running = False
                continue

            upper_rate_limit = self.output_rate_hz * self.sdr_restart_rate_upper_factor
            lower_rate_limit = self.output_rate_hz * self.sdr_restart_rate_lower_factor
            need_strikes = max(1, int(self.sdr_restart_rate_consecutive_windows))

            if measured_rate_hz > upper_rate_limit and self._last_decoded_sample_time is not None:
                rate_high_strikes += 1
                rate_low_strikes = 0
                if rate_high_strikes >= need_strikes:
                    print(
                        f'⚠️  Watchdog: measured rate {measured_rate_hz:.1f}Hz exceeds '
                        f'{upper_rate_limit:.1f}Hz ({self.sdr_restart_rate_upper_factor:.0%} of expected) '
                        f'for {rate_high_strikes} consecutive windows — exiting for monitor-driven restart.'
                    )
                    self.running = False
                    continue
                print(
                    f'⚠️  Watchdog: measured rate {measured_rate_hz:.1f}Hz exceeds '
                    f'{upper_rate_limit:.1f}Hz — transient (strike {rate_high_strikes}/{need_strikes}), '
                    f'likely stall/catch-up; confirming next window before restart.'
                )
            elif measured_rate_hz < lower_rate_limit and self._last_decoded_sample_time is not None:
                rate_low_strikes += 1
                rate_high_strikes = 0
                if rate_low_strikes >= need_strikes:
                    print(
                        f'⚠️  Watchdog: measured rate {measured_rate_hz:.1f}Hz below '
                        f'{lower_rate_limit:.1f}Hz ({self.sdr_restart_rate_lower_factor:.0%} of expected) '
                        f'for {rate_low_strikes} consecutive windows — exiting for monitor-driven restart.'
                    )
                    self.running = False
                    continue
                print(
                    f'⚠️  Watchdog: measured rate {measured_rate_hz:.1f}Hz below '
                    f'{lower_rate_limit:.1f}Hz — transient (strike {rate_low_strikes}/{need_strikes}), '
                    f'likely stall/catch-up; confirming next window before restart.'
                )
            else:
                # In band: a lone excursion was just a stall/catch-up pair averaging out.
                rate_high_strikes = 0
                rate_low_strikes = 0

    def setup_device(self):
        if _bladerf is None:
            raise ImportError('bladerf Python module is not available.')
        self.device = bladerf.BladeRF(self.bladerf_identifier) if self.bladerf_identifier else bladerf.BladeRF()

        self.channel = self.device.Channel(_bladerf.CHANNEL_RX(0))
        self.channel2 = self.device.Channel(_bladerf.CHANNEL_RX(1))

        for ch in (self.channel, self.channel2):
            ch.frequency = self.frequency
            ch.sample_rate = self.sample_rate
            ch.bandwidth = self.bandwidth
            mode = self.gain_mode.lower().replace('_', '')
            if mode == 'manual':
                ch.gain_mode = _bladerf.GainMode.Manual
                ch.gain = self.gain
            elif mode == 'fastattack':
                ch.gain_mode = _bladerf.GainMode.FastAttack_AGC
            elif mode == 'slowattack':
                ch.gain_mode = _bladerf.GainMode.SlowAttack_AGC
            elif mode == 'hybrid':
                ch.gain_mode = _bladerf.GainMode.Hybrid_AGC
            else:
                print(f'⚠️  Unknown gain_mode {self.gain_mode!r} — defaulting to Manual with gain={self.gain}')
                ch.gain_mode = _bladerf.GainMode.Manual
                ch.gain = self.gain

        self.device.sync_config(
            layout=_bladerf.ChannelLayout.RX_X2,
            fmt=_bladerf.Format.SC16_Q11,
            num_buffers=16,
            buffer_size=8192,
            num_transfers=8,
            stream_timeout=3500,
        )

        self.device.rx_mux = _bladerf.RXMux.Counter_32bit if self.is_counter else _bladerf.RXMux.Baseband

        print('\n=== BladeRF Configuration ===')
        print(f'  RX0: {self.channel.sample_rate/1e6:.2f} MSPS @ {self.channel.frequency/1e6:.2f} MHz')
        print(f'  RX1: {self.channel2.sample_rate/1e6:.2f} MSPS @ {self.channel2.frequency/1e6:.2f} MHz')
        print('  Layout: RX_X2, Format: SC16_Q11')
        print('=============================\n')
    def _extract_output_stream(self, rx_samples_u16: np.ndarray) -> np.ndarray:
        """Extract one antenna's stream from the interleaved RX0/RX1 I/Q buffer.
        Stride-4 layout: [RX0_I, RX0_Q, RX1_I, RX1_Q, ...].
        rx_channel selects RX0 (0) or RX1 (1); device_num selects I (1) or Q (2) within that channel.
        """
        offset = self.rx_channel * 2 + (self.device_num - 1)
        return rx_samples_u16[offset::4].copy()

    def rx_thread(self):
        self.channel.enable = True
        self.channel2.enable = True
        rx_buffer = bytearray(self.buffer_size * 4 * 2)
        meta = _bladerf.ffi.new("struct bladerf_metadata *")

        try:
            print("RX thread started. Waiting for samples...")

            # # ── Ring-buffer drain ──────────────────────────────────────────────
            # # Discard stale pre-buffered data so that the first real sync_rx call
            # # returns freshly-captured samples.  Measure sync_rx call duration
            # # before and after to confirm whether the ring was pre-filled.
            # drain_calls = (16 * 8192 + self.buffer_size - 1) // self.buffer_size  # ceil(ring / batch)
            # print(f"Draining BladeRF ring buffer ({drain_calls} calls)...")
            # for drain_i in range(drain_calls):
            #     t0 = time.time()
            #     self.device.sync_rx(rx_buffer, self.buffer_size, timeout_ms=3500, meta=meta)
            #     dt = time.time() - t0
            #     print(f"  drain call {drain_i}: sync_rx took {dt*1000:.1f} ms")
            # # First real call after drain — should block until fresh data arrives
            # t0 = time.time()
            # self.device.sync_rx(rx_buffer, self.buffer_size, timeout_ms=3500, meta=meta)
            # dt = time.time() - t0
            # print(f"  first post-drain sync_rx took {dt*1000:.1f} ms  ← should be ~{self.buffer_size/100:.0f} ms if ring was pre-filled")
            # # Re-enter the main loop; this buffer is fresh so process it normally
            # buffer_received_time = time.time()
            # actual_count = meta.actual_count
            # if actual_count > 0:
            #     rx_samples = np.frombuffer(rx_buffer, dtype=np.uint16, count=actual_count * 2)
            #     output_data = self._extract_output_stream(rx_samples)
            #     buffer_duration_s = len(output_data) / 100e3
            #     first_word_timestamp = buffer_received_time - buffer_duration_s
            #     self.samples_captured += len(output_data)
            #     try:
            #         self.data_queue.put((output_data, first_word_timestamp), timeout=0.2)
            #     except queue.Full:
            #         pass
            # # ── End drain ──────────────────────────────────────────────────────

            while self.running and self._rx_running:
                
                try:
                    self.device.sync_rx(rx_buffer, self.buffer_size, timeout_ms=3500, meta=meta)
                    # Capture system time immediately after sync_rx returns
                    buffer_received_time = time.time()
                except _bladerf.TimeoutError:
                    print("⚠️  RX timeout (no signal). Retrying...")
                    time.sleep(0.1)
                    continue
                except Exception as e:
                    print(f"⚠️  RX error: {e}. Retrying...")
                    time.sleep(0.1)
                    continue

                actual_count = meta.actual_count
                if actual_count <= 0:
                    continue
                
                # Use actual_count * 2 (I and Q per sample)
                valid_length = actual_count * 2
                rx_samples = np.frombuffer(rx_buffer, dtype=np.uint16, count=valid_length)
                
                output_data = self._extract_output_stream(rx_samples)

                # Calculate timestamp for FIRST word in this buffer
                # The buffer duration represents how long it took to fill
                # Last word arrived at buffer_received_time, first word arrived buffer_duration earlier
                buffer_duration_s = len(output_data) / 100e3
                first_word_timestamp = buffer_received_time - buffer_duration_s

                self.samples_captured += len(output_data)

                try:
                    # Pass data with timestamp of first word
                    self.data_queue.put((output_data, first_word_timestamp), timeout=0.2)
                except queue.Full:
                    pass

                # Dual-antenna: extract the other RX0 stream (opposite I/Q) and feed
                # the secondary reader's queue so it decodes in parallel.
                # Use put_nowait so a slow secondary processor never stalls rx_thread
                # (which would delay primary data and cause the BladeRF ring to fill up).
                if self.secondary_reader is not None:
                    sec_idx = 1 if self.device_num == 1 else 0
                    sec_data = rx_samples[sec_idx::4].copy()
                    sec_first_ts = buffer_received_time - len(sec_data) / 100e3
                    try:
                        # print("appending to second queue",sec_data.shape,sec_data[:10])
                        self.secondary_reader.data_queue.put_nowait((sec_data, sec_first_ts))
                    except queue.Full:
                        pass

        except Exception as exc:
            print(f'RX thread error: {exc}')
        finally:
            self.channel.enable = False
            self.channel2.enable = False
            try:
                self.data_queue.put(None, timeout=0.2)
            except Exception:
                pass
            if self.secondary_reader is not None:
                try:
                    self.secondary_reader.data_queue.put(None, timeout=0.2)
                except Exception:
                    pass

    @staticmethod
    def _bin2num_20_12(bits: np.ndarray) -> float:
        bit_string = ''.join(str(int(x)) for x in bits)
        value = int(bit_string, 2)
        if value >= (1 << 19):
            value -= (1 << 20)
        return value / (1 << 12)

    def _decode_value_from_packet_bits(self, bits: np.ndarray):
        if bits is None or len(bits) < 20:
            return None
        payload = bits[:20]
        payload_reversed = payload[::-1]
        return self._bin2num_20_12(payload_reversed) * self.decode_scale

    @staticmethod
    def _decode_raw_int_from_packet_bits(bits: np.ndarray):
        """Return the raw 20-bit signed integer from packet bits without any scaling."""
        if bits is None or len(bits) < 20:
            return None
        payload_reversed = bits[:20][::-1]
        bit_string = ''.join(str(int(x)) for x in payload_reversed)
        value = int(bit_string, 2)
        if value >= (1 << 19):
            value -= (1 << 20)
        return value

    def _pick_mismatch_value(self, v1: float, v2: float, left_neighbor, right_neighbor):
        neighbors = []
        if left_neighbor is not None and np.isfinite(left_neighbor):
            neighbors.append(float(left_neighbor))
        if right_neighbor is not None and np.isfinite(right_neighbor):
            neighbors.append(float(right_neighbor))

        if len(neighbors) > 0:
            score_v1 = sum(abs(float(v1) - n) for n in neighbors)
            score_v2 = sum(abs(float(v2) - n) for n in neighbors)
            if score_v1 < score_v2:
                return float(v1), 5, 'v1', 'neighbors'
            if score_v2 < score_v1:
                return float(v2), 6, 'v2', 'neighbors'

        if abs(v1) <= abs(v2):
            return float(v1), 5, 'v1', 'magnitude'
        return float(v2), 6, 'v2', 'magnitude'

    def _is_exact_multi_frame_length(self, length: int) -> bool:
        """True if length is an exact sum of N >= 1 accepted frame lengths.

        General coin-change reachability over self.accepted_frame_lengths - works
        for any set of accepted lengths, not just a pair spaced by one consistent
        step (a closed-form step formula lived here previously; it silently gave
        wrong answers as soon as a third, non-arithmetic length was added).
        """
        length = int(length)
        if length <= 0:
            return False
        lengths = [int(v) for v in self.accepted_frame_lengths if v > 0]
        if not lengths:
            return False
        reachable = np.zeros(length + 1, dtype=bool)
        reachable[0] = True
        for total in range(1, length + 1):
            for L in lengths:
                if L <= total and reachable[total - L]:
                    reachable[total] = True
                    break
        return bool(reachable[length])

    def _find_boundary_duplicate_offset(self, valid_flag: np.ndarray, start_idx: int) -> int:
        """Locate the single duplicate sample a run_sim_stream.m chunk
        restart inserts into an otherwise-251-word frame (confirmed
        empirically: valid_flag/push alternates 1,0,1,0,... starting at
        start_idx for the entire channel-extraction span of a real frame;
        the chunk-restart artifact repeats exactly one sample somewhere in
        that span, breaking the alternation at exactly one point - not
        always in the idle tail past the last real channel's window, as
        first assumed, sometimes within it, which silently corrupts
        channel bit-extraction if left uncorrected).

        Search is bounded to the actual channel-extraction span (up to
        channel 4's own ch_end, +margin) rather than the full 251-word
        frame, because the idle tail beyond that span is constant 0 by
        design - comparing it against a continued 1,0,1,0 alternation
        would flag every single idle word as a false "duplicate".

        Returns the offset (0-based, relative to start_idx) of the first
        break from pure alternation, or None if the span is clean (the
        real duplicate must then be further out in the harmless idle
        tail - no correction needed for bit extraction in that case).
        """
        search_len = min(4 * self.bits_per_channel + 4, len(valid_flag) - start_idx)
        if search_len <= 0:
            return None
        window = valid_flag[start_idx:start_idx + search_len]
        expected = np.resize([1, 0], search_len)
        mismatches = np.flatnonzero(window != expected)
        return int(mismatches[0]) if len(mismatches) else None

    def _estimate_frames_in_gap_linear(self, distance_words: int) -> int:
        """Estimate frame count using linear combinations of accepted frame lengths.

        Finds n such that some sum of n accepted lengths is closest to distance_words.
        For (248, 250), this corresponds to finding integers a,b >= 0 with
        a+b=n and a*248 + b*250 close to distance_words.
        """
        distance_words = int(distance_words)
        if distance_words <= 0:
            return 1

        lengths = tuple(sorted(self.accepted_frame_lengths))
        if len(lengths) == 1:
            return max(1, int(round(distance_words / lengths[0])))

        min_len = lengths[0]
        max_len = lengths[-1]
        max_frames = max(1, int(np.ceil((distance_words + 2 * max_len) / min_len)))

        reachable = {0}
        best_n = 1
        best_err = float('inf')

        for frame_count in range(1, max_frames + 1):
            next_reachable = set()
            for base_sum in reachable:
                for length in lengths:
                    next_reachable.add(base_sum + length)

            local_best_sum = min(next_reachable, key=lambda s: abs(s - distance_words))
            local_err = abs(local_best_sum - distance_words)
            if local_err < best_err:
                best_err = local_err
                best_n = frame_count
                if local_err == 0:
                    break

            reachable = next_reachable

        return max(1, int(best_n))

    def _lookahead_confirms_real_skip(self, valid_frame_starts, valid_frame_lengths,
                                       packet_nums_for_edges, k, observed_packet_num,
                                       distance_forced_packet_num):
        """Disambiguates a single-frame packet_num/distance disagreement (distance
        says 'just the next frame', packet_num disagrees) by checking whether the
        FOLLOWING frames continue counting from the OBSERVED packet_num (a real
        skip - e.g. GNU Radio dropped a USB packet, which removes that packet's
        samples from the recorded file entirely, so the word-distance in the file
        underestimates how many packets were actually skipped in the true
        transmitted sequence, even though packet_num itself - not bit-error-
        corrupted - is correct) or from the distance-forced value instead (isolated
        packet_num bit-error noise on this one frame - the assumption the caller's
        existing header-drop logic defaults to).

        Checks up to 2 following valid frames, already available in this buffer's
        precomputed frame arrays (the whole chunk is parsed into arrays before the
        per-frame loop starts) - no lookahead buffering/restructuring needed.
        Requires an UNAMBIGUOUS match: a following frame must agree with exactly one
        hypothesis, not both (a distance-implied gap that's a multiple of 8 packets
        aliases to the same packet_num under either hypothesis and can't be
        disambiguated this way) and not neither (inconclusive - e.g. that frame has
        its own independent anomaly). Stops at the first frame that cleanly resolves
        it rather than requiring every checked frame to agree, since one clean
        disambiguating match is already stronger evidence than the single-frame
        distance estimate this is replacing.

        Returns True if the observed packet_num is confirmed (trust it, treat as a
        real skip), False if the distance-forced value is confirmed (current default
        behavior was right), or None if inconclusive (caller should fall back to
        that existing default, unchanged).
        """
        n = len(valid_frame_starts)
        prev_start_for_lookahead = valid_frame_starts[k]
        prev_observed = observed_packet_num
        prev_distance_forced = distance_forced_packet_num

        for j in range(k + 1, min(k + 3, n)):
            next_start = valid_frame_starts[j]
            next_distance = int(next_start) - int(prev_start_for_lookahead)
            next_frames_in_gap = self._estimate_frames_in_gap_linear(next_distance)
            next_actual = int(packet_nums_for_edges[next_start])

            expected_from_observed = (prev_observed + next_frames_in_gap) % 8
            expected_from_distance_forced = (prev_distance_forced + next_frames_in_gap) % 8

            matches_observed = (next_actual == expected_from_observed)
            matches_distance = (next_actual == expected_from_distance_forced)

            if matches_observed and not matches_distance:
                return True
            if matches_distance and not matches_observed:
                return False
            # else: matches both (aliased at this gap size) or neither (this frame
            # has its own anomaly too) - uninformative, keep looking

            prev_start_for_lookahead = next_start
            prev_observed = expected_from_observed
            prev_distance_forced = expected_from_distance_forced

        return None

    def _dump_raw_packets(self, packets_by_channel, packet_word_positions):
        # Writes every raw extracted packet (both copies, whatever channel
        # they belong to) to a local CSV, BEFORE group_builder does any
        # slot assignment - immune to the packet_num swap/duplication issue
        # since nothing here depends on group_builder's output. Called
        # right after _extract_channel_packets, ahead of
        # _decode_packet_groups, at both processing_thread call sites.
        #
        # Gated on gcs_recording_active (the runtime flag toggled by
        # _start_gcs_recording/_stop_gcs_recording via the pub/sub trigger,
        # NOT the static enable_gcs constructor flag) - this is meant as a
        # companion to an actual recording session, not something that
        # writes during arbitrary test/idle runs where GCS recording isn't
        # even on. The file itself stays open for the reader's whole
        # lifetime (one per-session timestamped file, see __init__) even
        # across multiple start/stop toggles - only the writes are gated,
        # so word_pos/t_s stay globally consistent within one file rather
        # than needing a new file per recording start/stop.
        if self._raw_packet_dump_file is None or not self.gcs_recording_active:
            return
        positions = packet_word_positions or {}
        for ch, packets in packets_by_channel.items():
            pos_list = positions.get(ch, [])
            for i, p in enumerate(packets):
                wp = pos_list[i] if i < len(pos_list) else None
                t_s = (wp / self.bit_clock_hz) if wp is not None else ''
                bits_str = ''.join(str(int(b)) for b in p.bits) if p.bits is not None and len(p.bits) else ''
                self._raw_packet_dump_file.write(
                    f'{wp if wp is not None else ""},{t_s},{ch},{int(p.packet_num)},'
                    f'{int(p.is_valid)},{int(p.low_conf_count)},{p.reason},{bits_str}\n'
                )
        self._raw_packet_dump_file.flush()

    def _extract_channel_packets(self, data: np.ndarray):
        data = np.asarray(data, dtype=np.uint16).reshape(-1)
        prefix = self._extract_lookback_words
        prefix_len = int(len(prefix))
        if prefix_len > 0:
            working_data = np.concatenate([prefix, data])
        else:
            working_data = data

        data_bit = working_data & 1
        packet_nums_raw = (working_data & ((1 << 4) | (1 << 5) | (1 << 6))) >> 4
        valid_flag = (working_data & (1 << 8)) >> 8
        error_flag_arr = (working_data & (1 << 7)) >> 7
        # bits [14:9]: softCombine_hdl.m's low_conf_count, saturating 6-bit
        # per-packet count (held constant across a packet's words, like
        # packet_num - not an OR-per-word flag like error_flag_arr).
        low_conf_count_arr = (working_data & (0x3F << 9)) >> 9
        # bit [15]: softCombine_hdl.m's superseded_flag - last of the 4 spare
        # bits (1, 2, 3, 15) in this 16-bit USB word. Same held-constant-
        # across-a-packet's-words / one-frame-forward-latency semantics as
        # low_conf_count_arr above (both are latched on softCombine_hdl.m's
        # packet_flag edge and held until the next one) - deliberately NOT
        # truncated to process_until below, for the same reason
        # low_conf_count_arr isn't (see pkt_low_conf_count's own comment).
        superseded_flag_arr = (working_data & (1 << 15)) >> 15
        # print("error count:", sum(error_flag_arr),np.where(error_flag_arr==1))
        packet_nums_for_edges = packet_nums_raw.copy()
        valid_words = valid_flag.astype(bool)
        if np.any(valid_words):
            first_valid_idx = int(np.flatnonzero(valid_words)[0])
            last_pkt = int(packet_nums_for_edges[first_valid_idx])
            if first_valid_idx > 0:
                packet_nums_for_edges[:first_valid_idx] = last_pkt
            for idx in range(first_valid_idx + 1, len(packet_nums_for_edges)):
                if valid_words[idx]:
                    last_pkt = int(packet_nums_for_edges[idx])
                else:
                    packet_nums_for_edges[idx] = last_pkt
        else:
            return {1: [], 2: [], 3: [], 4: []}, 0

        if len(packet_nums_raw) < 2:
            return {1: [], 2: [], 3: [], 4: []}, 0

        transitions = np.where(np.diff(packet_nums_for_edges) != 0)[0]
        frame_starts = np.concatenate(([0], transitions + 1))
        frame_ends = np.concatenate((transitions, [len(packet_nums_for_edges) - 1]))

        if len(frame_starts) < 2:
            return {1: [], 2: [], 3: [], 4: []}, 0

        process_until = int(frame_starts[-1])
        if process_until <= 0:
            return {1: [], 2: [], 3: [], 4: []}, 0

        process_until_input = process_until - prefix_len
        if process_until_input <= 0:
            return {1: [], 2: [], 3: [], 4: []}, 0

        packet_nums_raw = packet_nums_raw[:process_until]
        packet_nums_for_edges = packet_nums_for_edges[:process_until]
        data_bit = data_bit[:process_until]
        valid_flag = valid_flag[:process_until]
        error_flag_arr = error_flag_arr[:process_until]
        # low_conf_count_arr is deliberately NOT truncated to process_until,
        # unlike the parallel arrays above: a packet's own low_conf_count only
        # lands one frame period after its start (see pkt_low_conf_count
        # below), which for the last frames in this buffer falls beyond
        # process_until. Those words are physically present in working_data -
        # process_until bounds frame DETECTION, not data availability - so
        # keeping the full array is what makes the one-frame-forward read
        # possible without deferring packets across buffers. Nothing else
        # indexes this array, so the length mismatch is contained here.
        transitions = np.where(np.diff(packet_nums_for_edges) != 0)[0]
        frame_starts = np.concatenate(([prefix_len], transitions+1))
        frame_ends = np.concatenate((transitions, [len(packet_nums_for_edges) - 1]))
        frame_lengths = frame_ends - frame_starts + 1

        starts_in_fresh_data = frame_starts >= prefix_len 
        self.prefix_overlap_frames_skipped += int(np.sum(~starts_in_fresh_data))
        valid_mask = (np.array([self._is_exact_multi_frame_length(int(fl)) for fl in frame_lengths])
                      & starts_in_fresh_data)
        valid_frame_starts = frame_starts[valid_mask]
        valid_frame_lengths = frame_lengths[valid_mask]

        # Log every detected frame before the valid-length gate
        for _fs, _fl, _fv, _fresh in zip(frame_starts, frame_lengths, valid_mask, starts_in_fresh_data):
            if not bool(_fresh):
                continue
            abs_word = self._words_processed_total + int(_fs) - prefix_len
            pkt_n = int(packet_nums_for_edges[_fs])
            self._raw_frame_log.append((int(abs_word), pkt_n, int(_fl), bool(_fv)))
            if not bool(_fv) and not self.quiet:
                print(
                    f'⚠️  Frame rejected (len={_fl}, not a valid N×frame-length): '
                    f'pkt#{pkt_n} @ word={abs_word} t={abs_word/self.bit_clock_hz:.3f}s'
                )

        if len(valid_frame_starts) == 0:
            consumed = int(process_until_input)
            if consumed > 0:
                self._extract_lookback_words = data[max(0, consumed - 2):consumed].copy()
            return ({1: [], 2: [], 3: [], 4: []}, {1: [], 2: [], 3: [], 4: []}), consumed

        packets_by_channel = {1: [], 2: [], 3: [], 4: []}
        packet_word_positions = {1: [], 2: [], 3: [], 4: []}  # NEW: track word positions
        prev_start = None
        prev_packet_num = self._last_extracted_packet_num
        prev_abs_start = self._last_extracted_frame_abs_word_start

        for k in range(len(valid_frame_starts)):
            start_idx = valid_frame_starts[k]
            frame_len = valid_frame_lengths[k]
            packet_num = int(packet_nums_for_edges[start_idx])
            cur_abs_start = int(self._words_processed_total + int(start_idx) - prefix_len)

            # Cross-chunk gap: first frame of this chunk vs last frame of previous chunk.
            # Neither packet numbers (unreliable due to bit errors) nor word-distance
            # (inflated by buffer-boundary artifacts) can be trusted here.
            # Use wall-clock time between chunk boundaries as the reliable estimator.
            if prev_start is None and prev_packet_num is not None:
                missing = 0
                t0 = self._last_chunk_end_word_timestamp
                t1 = self._current_chunk_first_word_timestamp
                if t0 is not None and t1 is not None:
                    gap_s = max(0.0, t1 - t0)
                    fps = self.bit_clock_hz / float(np.mean(list(self.accepted_frame_lengths)))
                    raw_missing = max(0, int(round(gap_s * fps)) - 1)
                    max_fill = int(self.sdr_watchdog_window_seconds * fps)
                    missing = min(raw_missing, max_fill)
                    _ts = self._words_processed_total / self.bit_clock_hz
                    if raw_missing > max_fill and not self.quiet:
                        print(
                            f"⚠️  Cross-chunk gap {gap_s:.2f}s @ t={_ts:.3f}s exceeds watchdog window "
                            f"— capping fill at {missing} frames (raw={raw_missing})"
                        )
                    elif missing > 0 and not self.quiet:
                        print(
                            f"⚠️  Cross-chunk gap {gap_s:.3f}s @ t={_ts:.3f}s: inserting={missing} frames"
                        )
                if missing > 0:
                    last_num = prev_packet_num
                    for _ in range(missing):
                        last_num = (last_num + 1) % 8
                        for ch in range(1, 5):
                            packets_by_channel[ch].append(
                                DecodedPacket(packet_num=last_num, is_valid=False, bits=np.array([], dtype=np.uint8), reason='cross_gap')
                            )
                            packet_word_positions[ch].append(None)
                    self.packet_sequence_anomaly_count += missing
                    self.placeholder_inserts_cross_chunk += int(missing)

            seq_anomaly = False
            if prev_start is not None:
                distance = start_idx - prev_start
                expected_frames_in_gap = self._estimate_frames_in_gap_linear(distance)
                expected_next = (prev_packet_num + 1) % 8 if prev_packet_num is not None else None
                expected_at_current = None
                missing_from_distance = 0
                if prev_packet_num is not None and expected_frames_in_gap >= 1:
                    expected_at_current = (prev_packet_num + expected_frames_in_gap) % 8
                    missing_from_distance = max(0, expected_frames_in_gap - 1)
                seq_anomaly = False
                if expected_at_current is not None and packet_num != expected_at_current:
                    seq_anomaly = True
                    self.packet_sequence_anomaly_count += 1
                    self.packet_sequence_events.append(
                        {
                            'prev_packet': int(prev_packet_num),
                            'observed_packet': int(packet_num),
                            'expected_next': int(expected_next),
                            'expected_at_current': int(expected_at_current),
                            'distance_words': int(distance),
                            'expected_frames_in_gap': int(expected_frames_in_gap),
                            'start_idx': int(start_idx),
                            'timestamp_s': (self._words_processed_total + int(start_idx) - prefix_len) / self.bit_clock_hz,
                        }
                    )
                    # print(
                    #     f"⚠️  Packet sequence anomaly: prev={prev_packet_num}, "
                    #     f"expected_next={expected_next}, observed={packet_num}, "
                    #     f"expected_at_current={expected_at_current}, "
                    #     f"distance={distance} words (~{expected_frames_in_gap} frame(s))"
                    # )

                    # Sustained mismatches mean a real phase shift — e.g. the packet
                    # counter advanced across a genuine signal dropout — not isolated
                    # bit-error noise.  Track a leaky run; once it crosses the unlock
                    # threshold, drop the lock and re-acquire phase from the real
                    # packet numbers (exactly like startup).  Without this the lock
                    # latches forever and every healthy post-gap frame is mis-scored,
                    # producing a fake reception "cliff" while the signal is fine.
                    if self._phase_locked:
                        self._phase_mismatch_run += 1
                        if self._phase_mismatch_run >= self.PHASE_UNLOCK_THRESHOLD:
                            self._phase_locked = False
                            self._phase_mismatch_run = 0
                            self._phase_agreement_run = 0
                            self.phase_relocks += 1
                            if not self.quiet:
                                _ts_ul = (self._words_processed_total + int(start_idx) - prefix_len) / self.bit_clock_hz
                                print(f"↻ Phase unlock @ t={_ts_ul:.3f}s: sustained mismatch, re-acquiring phase")
                        else:
                            # Still locked: default assumption is isolated packet_num
                            # bit-error noise on this one frame. But a dropped USB
                            # packet on the CAPTURE side (GNU Radio) removes that
                            # packet's samples from the recorded file entirely - the
                            # word-distance in the file then underestimates how many
                            # packets were really skipped in the true transmitted
                            # sequence, even though packet_num itself is correct.
                            # Both look identical from this one frame alone
                            # (expected_frames_in_gap==1, packet_num disagrees) - check
                            # whether the FOLLOWING frames confirm which is actually
                            # true before assuming noise and discarding.
                            # Scoped to exactly the header-drop's own precondition
                            # (expected_frames_in_gap==1) - that's the only case where
                            # expected_next IS the competing hypothesis being tested
                            # against. When expected_frames_in_gap>1 the competing
                            # hypothesis is expected_at_current instead (handled by
                            # the existing distance-trusted correction below,
                            # unchanged - not something this lookahead addresses).
                            lookahead_confirms_real_skip = None
                            if (expected_frames_in_gap == 1 and expected_next is not None
                                    and packet_num != expected_next):
                                lookahead_confirms_real_skip = self._lookahead_confirms_real_skip(
                                    valid_frame_starts, valid_frame_lengths, packet_nums_for_edges,
                                    k, packet_num, expected_next)

                            if lookahead_confirms_real_skip:
                                # Confirmed real skip, not noise - the mismatch-run
                                # increment above was premature, undo it (mirrors the
                                # decrement the agreement branch below already does on
                                # confirmed-fine frames - this is a confirmed-fine
                                # frame too, just not a distance-agreeing one).
                                if self._phase_mismatch_run > 0:
                                    self._phase_mismatch_run -= 1
                                packet_num_jump = (packet_num - prev_packet_num) % 8
                                missing_from_distance = max(0, packet_num_jump - 1)
                                if not self.quiet:
                                    ts_confirm = (self._words_processed_total + int(start_idx) - prefix_len) / self.bit_clock_hz
                                    print(
                                        f"✓ Lookahead confirms real skip (not header noise) @ t={ts_confirm:.3f}s: "
                                        f"prev_pkt#{prev_packet_num}, observed pkt#{packet_num} "
                                        f"(distance implied pkt#{expected_next}) - backfilling {missing_from_distance}"
                                    )
                            # If this frame should be the immediate next packet (1 frame
                            # apart) but packet_num disagrees, treat it as a bad header
                            # and discard it.  Disabled when disable_header_drops=True,
                            # or when the lookahead above confirmed this isn't noise.
                            elif (not self.disable_header_drops
                                    and expected_frames_in_gap == 1
                                    and expected_next is not None
                                    and packet_num != expected_next):
                                self.packet_sequence_header_drops += 1
                                if not self.quiet:
                                    ts_drop = (self._words_processed_total + int(start_idx) - prefix_len) / self.bit_clock_hz
                                    print(
                                        f"⚠️  Dropping out-of-order observed packet @ t={ts_drop:.3f}s: "
                                        f"expected {expected_next}, observed {packet_num}"
                                    )
                                # Insert a quality=0 placeholder so the watchdog counts this loss.
                                for ch in range(1, 5):
                                    packets_by_channel[ch].append(
                                        DecodedPacket(packet_num=expected_next, is_valid=False, bits=np.array([], dtype=np.uint8), reason='out_of_order')
                                    )
                                    packet_word_positions[ch].append(None)
                                self.packet_sequence_anomaly_count += 1
                                # Advance prev so the NEXT frame's distance is measured
                                # from this dropped position, not from before it.
                                prev_start = start_idx
                                prev_packet_num = expected_next
                                prev_abs_start = cur_abs_start
                                continue

                            elif not lookahead_confirms_real_skip:
                                # Multi-frame gap (isolated): distance is trusted over
                                # the noisy number field — relabel so one misread can't
                                # seed a phantom group in the builder downstream. Not
                                # reached when lookahead confirmed a real skip above -
                                # packet_num stays as observed in that case, and
                                # missing_from_distance was already recomputed from the
                                # packet_num jump instead of this distance estimate.
                                self.packet_num_distance_corrections += 1
                                packet_num = expected_at_current

                    # Not locked (startup or just-unlocked): the real packet number
                    # flows untouched so the group builder can (re)find true phase.
                    # This frame disagreed, so the run toward (re)lock resets.
                    if not self._phase_locked:
                        self._phase_agreement_run = 0
                elif expected_at_current is not None:
                    # Detection agreed with distance.
                    if self._phase_locked:
                        # Decay the mismatch run; isolated noise recovers, only a
                        # sustained run survives to trigger unlock.
                        if self._phase_mismatch_run > 0:
                            self._phase_mismatch_run -= 1
                    else:
                        # Build confidence toward (re)lock.
                        self._phase_agreement_run += 1
                        if self._phase_agreement_run >= self.PHASE_LOCK_THRESHOLD:
                            self._phase_locked = True
                            self._phase_mismatch_run = 0

                # Use distance-based inference only — packet numbers are unreliable
                # (bit errors corrupt the number field, causing false large gaps).
                # Within a single buffer the word distance is accurate.
                missing = missing_from_distance
                if missing > 0 and prev_packet_num is not None:
                    if not self.quiet:
                        _ts_fill = (self._words_processed_total + int(start_idx) - prefix_len) / self.bit_clock_hz
                        print(
                            f'⚠️  Intra-chunk fill: {missing} placeholder(s) before pkt#{packet_num} '
                            f'@ t={_ts_fill:.3f}s (dist={distance} words, expected {expected_frames_in_gap} frames, prev_pkt#{prev_packet_num})'
                        )
                    last_num = prev_packet_num
                    for _ in range(missing):
                        last_num = (last_num + 1) % 8
                        for ch in range(1, 5):
                            packets_by_channel[ch].append(
                                DecodedPacket(packet_num=last_num, is_valid=False, bits=np.array([], dtype=np.uint8), reason='intra_gap')
                            )
                            packet_word_positions[ch].append(None)
                    self.placeholder_inserts_intra_chunk += int(missing)

            # Packet-level error flag: hardware interference flag OR packet sequence anomaly.
            # Kept merged (pkt_error) for the v1/v2 arbitration fallback, which depends
            # on this exact OR - pkt_changeofstrength/seq_anomaly below are the same two
            # terms kept separate, for characterization reporting only.
            _pkt_err_end = start_idx + 4 * self.bits_per_channel
            if start_idx >= 0 and _pkt_err_end <= len(error_flag_arr):
                _all_valid = valid_flag[start_idx:_pkt_err_end]
                _all_error = error_flag_arr[start_idx:_pkt_err_end]
                pkt_changeofstrength = bool(np.any(_all_error[_all_valid == 1]))
            else:
                pkt_changeofstrength = False
            pkt_error = pkt_changeofstrength
            if seq_anomaly:
                pkt_error = True

            # low_conf_count is held constant across a packet's words (like
            # packet_num), not per-word like error_flag - read one
            # representative sample rather than OR/max-aggregating.
            #
            # ONE-FRAME LATENCY: the value held across THIS packet's words is
            # the PREVIOUS packet's - softCombine_hdl streams the whole payload
            # before the count settles, so a packet's own value only appears
            # one frame period later (right where the next packet is read).
            # Read at start_idx + frame_len instead; both are already in the
            # 100 kHz word domain so no rescaling is needed. Validated against
            # PRBS ground truth: raises low_conf-vs-actual-error correlation
            # from r=0.36 to 0.48, cuts paired-arbitration misfires from 9.7%
            # to 3.9%, and flips low_conf arbitration from net-harmful to
            # net-helpful across all four characterization recordings.
            _lc_idx = int(start_idx) + int(frame_len)
            if 0 <= _lc_idx < len(low_conf_count_arr):
                pkt_low_conf_count = int(low_conf_count_arr[_lc_idx])
            else:
                # Next frame genuinely hasn't arrived yet (packet at the very
                # end of a buffer with no trailing words) - a real streaming
                # limit, not an error. Rare, since the untruncated array
                # normally extends past process_until by the partial trailing
                # frame. 0 = "fully confident", matching the pre-existing
                # out-of-range fallback.
                pkt_low_conf_count = 0

            # superseded_flag: identical one-frame-forward read as
            # pkt_low_conf_count immediately above, same _lc_idx (both
            # signals are latched on the same packet_flag edge and held
            # until the next one - see DecodedPacket.superseded_flag's own
            # comment).
            if 0 <= _lc_idx < len(superseded_flag_arr):
                pkt_superseded_flag = bool(superseded_flag_arr[_lc_idx])
            else:
                pkt_superseded_flag = False

            # 251-word frames are run_sim_stream.m's chunk-restart artifact -
            # one duplicate raw sample inserted somewhere in the frame
            # (confirmed empirically, offset varies per-frame depending on
            # where the chunk boundary lands). Tolerating the extra length
            # (accepted_frame_lengths including 251) fixed packet-drop
            # stats, but left the duplicate in place, shifting every
            # channel's extraction window that starts after it by one word -
            # splice it back out here so bit extraction sees a true 250-word
            # frame again, instead of just accepting the wrong-length one.
            dup_offset = None
            corrected_data_bit = None
            corrected_valid_flag = None
            if int(frame_len) == 251:
                dup_offset = self._find_boundary_duplicate_offset(valid_flag, start_idx)
                if dup_offset is not None:
                    span_end = min(start_idx + int(frame_len), len(data_bit))
                    corrected_data_bit = np.delete(data_bit[start_idx:span_end], dup_offset)
                    corrected_valid_flag = np.delete(valid_flag[start_idx:span_end], dup_offset)

            for ch in range(1, 5):
                channel_offset = (ch - 1) * self.bits_per_channel
                if corrected_data_bit is not None:
                    # local corrected frame (duplicate already spliced out),
                    # 0-indexed at start_idx instead of absolute
                    ch_start = channel_offset - 2
                    ch_end = ch_start + self.bits_per_channel
                    bounds_ok = ch_start >= 0 and ch_end + 5 <= len(corrected_data_bit)
                    src_data_bit, src_valid_flag = corrected_data_bit, corrected_valid_flag
                else:
                    ch_start = start_idx + channel_offset - 2
                    ch_end = ch_start + self.bits_per_channel
                    bounds_ok = ch_start >= 0 and ch_end + 5 <= len(packet_nums_raw)
                    src_data_bit, src_valid_flag = data_bit, valid_flag

                if not bounds_ok:
                    packets_by_channel[ch].append(
                        DecodedPacket(packet_num=packet_num, is_valid=False, bits=np.array([], dtype=np.uint8), reason='ch_bounds')
                    )
                    # absolute position, same convention as _raw_frame_log's
                    # abs_word - NOT chunk-relative, so it stays comparable
                    # to raw signal indices even across multiple
                    # decode_from_word_stream calls / internal sub-chunks
                    packet_word_positions[ch].append(self._words_processed_total + start_idx - prefix_len)
                    continue

                bits_block = src_data_bit[ch_start:ch_end]
                valid_block = src_valid_flag[ch_start+2:ch_end+2]
                selected_bits = bits_block[valid_block == 1]
                self.valid_flag_bitcount_hist_by_channel[ch][len(selected_bits)] += 1
                packets_by_channel[ch].append(
                    DecodedPacket(packet_num=packet_num, is_valid=True, bits=selected_bits.astype(np.uint8),
                                  error_flag=pkt_error, changeofstrength_flag=pkt_changeofstrength,
                                  packetnum_anomaly_flag=seq_anomaly, low_conf_count=pkt_low_conf_count,
                                  superseded_flag=pkt_superseded_flag)
                )
                # absolute position, same convention as _raw_frame_log's
                # abs_word - see comment on the other append site above
                _abs_word_pos = self._words_processed_total + start_idx - prefix_len
                packet_word_positions[ch].append(_abs_word_pos)
                # RAW packet bits log (2026-08-03) - logged here, at the ONLY
                # site that constructs a real (is_valid=True) packet, deliberately
                # BEFORE group_builder/pending and all three of its pre-filters
                # (low_conf_count/packetnum_anomaly_flag/superseded_flag) ever
                # see this packet. Every other packets_by_channel[ch].append()
                # call in this method is a synthetic is_valid=False placeholder
                # (reason='cross_gap'/'intra_gap'/'out_of_order'/'ch_bounds') -
                # none of those are logged here, so a "raw BER" computed from
                # this log can never silently score a placeholder's empty bits
                # as if they were real zeros. Same DECODED_BITS_WIDTH truncation
                # bits_rec['v1_bits']/['v2_bits'] apply at the group level (see
                # that assignment's own comment) - kept identical so a raw-BER
                # sample is bit-for-bit the same content that WOULD have become
                # v1_bits/v2_bits had this packet survived group-building.
                if len(selected_bits) >= DECODED_BITS_WIDTH:
                    self._raw_packet_bits_log[ch].append(
                        (_abs_word_pos, selected_bits[:DECODED_BITS_WIDTH].astype(np.uint8))
                    )

            prev_start = start_idx
            prev_packet_num = packet_num
            prev_abs_start = cur_abs_start

        if prev_packet_num is not None:
            self._last_extracted_packet_num = prev_packet_num
            self._last_extracted_frame_abs_word_start = prev_abs_start

        consumed = int(process_until_input)
        if consumed > 0:
            self._extract_lookback_words = data[max(0, consumed - 2):consumed].copy()

        return (packets_by_channel, packet_word_positions), consumed

    def _build_group_with_placeholders(self, channel_idx: int):
        # Positions must be popped in lockstep with pending, not via a
        # separate fixed "always advance by 8" slice elsewhere - pending
        # only actually advances by however many REAL packets matched an
        # expected_packet_num this call (0-8), since a placeholder branch
        # deliberately does NOT consume from pending. A naive same-width
        # pop of the positions queue desyncs from pending's true
        # consumption rate the first time any placeholder is inserted, and
        # that desync compounds on every subsequent group forever after -
        # this was a real, confirmed bug (word positions drifting further
        # from true time as the recording went on).
        #
        # DISTANCE-ACCEPT BRANCH REVERTED (2026-07-27): a 2026-07-26 change
        # here tried accepting a packet_num mismatch as-is (instead of
        # placeholder-filling) whenever word-distance implied no real gap,
        # to cut down on excessive placeholder insertion. Reverted - user
        # confirmed packets cannot arrive out of order on this link, which
        # means a group can legitimately be phase-offset (e.g. the true
        # arrival sequence is {4,5,6,7,0,1,2,3}, not corrupted, just not
        # starting at 0) - group[s]/group[s+4]'s downstream v1/v2 pairing
        # assumes slot 0 is always the true start of a paired octet, so
        # relabeling OR blindly accepting a mismatched packet_num into a
        # fixed slot both corrupt real, distinct packets into looking like
        # false "copies" of each other. That's a deeper phase-tracking
        # problem (expected_packet_num needs to carry forward across calls
        # instead of resetting to 0 every time), not something this
        # function can safely paper over with either accept-as-is or
        # relabel. Back to the simple, safe rule: any mismatch, regardless
        # of distance, gets a placeholder - matches every version of this
        # file before 2026-07-26. GROUP_BUILDER_LOW_CONF_CUTOFF (in
        # _decode_packet_groups, filtering unreliable packets before they
        # ever reach pending) is unrelated to this branch and stays.
        pending = self._pending_packets_by_channel[channel_idx]
        positions = self._pending_packet_word_positions[channel_idx]
        group = []
        group_positions = []
        log_ch1 = channel_idx == 1 and len(self._group_builder_decision_log) < 100_000
        for expected_packet_num in range(8):
            pending_front_pkt = int(pending[0].packet_num) if len(pending) > 0 else None

            if len(pending) > 0 and int(pending[0].packet_num) == expected_packet_num:
                pos = positions.popleft() if len(positions) > 0 else None
                group.append(pending.popleft())
                group_positions.append(pos)
                if log_ch1:
                    self._group_builder_decision_log.append({
                        'expected': expected_packet_num, 'pending_front_pkt': pending_front_pkt,
                        'action': 'match', 'accepted_pkt_num': expected_packet_num,
                    })
                continue

            group.append(
                DecodedPacket(
                    packet_num=expected_packet_num,
                    is_valid=False,
                    bits=np.array([], dtype=np.uint8),
                    reason='group_builder',
                )
            )
            group_positions.append(None)
            self.placeholder_inserts_group_builder += 1
            if log_ch1:
                self._group_builder_decision_log.append({
                    'expected': expected_packet_num, 'pending_front_pkt': pending_front_pkt,
                    'action': 'placeholder', 'accepted_pkt_num': None,
                })
        return group, group_positions

    def _decode_packet_groups(self, packets_by_channel, packet_word_positions=None, word_timestamps=None):
        # Extend pending packets for each channel (no packet dropping/resync)
        for channel_idx, packets in packets_by_channel.items():
            pending = self._pending_packets_by_channel[channel_idx]
            positions_in = (packet_word_positions or {}).get(channel_idx, [])

            # LOW-CONFIDENCE FILTER (2026-07-26, user hypothesis): the raw
            # packet_num sequence reaching group_builder was found to be
            # scrambled/duplicate-laden in hard-RF snapshots (e.g. "4,4",
            # "7,7", "0,0" repeated back-to-back, non-monotonic jumps) -
            # not primarily real gaps. Cause: a packet whose bits decoded
            # with a high low_conf_count is also more likely to have its
            # packet_num field itself corrupted (same USB word, see
            # DecodedPacket.low_conf_count) - group_builder was treating
            # that corrupted number as a trustworthy sequence anchor.
            # Excluding low_conf_count>=GROUP_BUILDER_LOW_CONF_CUTOFF packets
            # from ever entering the pending queue (not just from being
            # trusted for matching) means they can't seed a false "match",
            # a false "no gap justified" distance decision, or a phantom
            # duplicate. Uses its own dedicated, separately-calibrated
            # constant (=5, see its docstring) - NOT LOW_CONF_COUNT_THRESHOLD
            # (=10, a different job with a much softer cost for a wrong
            # call: v1/v2 arbitration just picks the less-bad of two copies
            # you already have, this decision throws real data away
            # outright). Placeholders (cross_gap/intra_gap/out_of_order)
            # default low_conf_count=0 and are unaffected. Positions are
            # filtered in lockstep so a dropped packet's word position
            # doesn't dangle out of sync with pending (same lockstep
            # requirement noted in _build_group_with_placeholders above).
            #
            # PACKETNUM-ANOMALY FILTER (2026-08-02): the low-confidence filter
            # above only catches packet_num corruption that correlates with a
            # high low_conf_count - but packet_num sits at "the least-timing-
            # settled point in the packet" (see packetnum_anomaly_flag's own
            # doc) and can be independently unreliable even when the payload
            # bits themselves decoded cleanly. Confirmed on a real recording
            # (10meters_newsoftdecode, sample idx=177): packetnum_anomaly_flag
            # was True on BOTH copies while low_conf_count was 0 on both, and
            # group_builder trusted the anomalous packet_num anyway - pairing
            # a v1 sample with a v2 sample only 2.5ms away (one packet period)
            # instead of the normal ~10ms diversity gap, an internally-
            # inconsistent group that each copy's own ground-truth BER
            # couldn't catch (each copy matched the true PRBS sequence
            # perfectly on its own, just at different circular offsets).
            # Same treatment as the low-confidence filter: exclude before
            # `pending`, not just before being trusted for slot-matching, so
            # an anomalous packet_num can't seed a false "match" either.
            #
            # SUPERSEDED FILTER (2026-08-02): same rationale again - a
            # superseded packet's own accept was abandoned mid-payload for a
            # stronger competing peak (searchPeak2_hdl.m's accept_cand/
            # superseded_pulse), so its packet_num/content is exactly as
            # untrustworthy as a corrupted one, even though it isn't flagged
            # low-confidence or packetnum-anomalous by either of the other
            # two checks.
            filtered_packets = []
            filtered_positions = []
            for i, p in enumerate(packets):
                if p.low_conf_count >= self.group_builder_low_conf_cutoff:
                    self.low_conf_filtered_before_builder += 1
                    continue
                if p.packetnum_anomaly_flag:
                    if (PACKETNUM_ANOMALY_LOW_CONF_RESCUE_CUTOFF is not None
                            and p.low_conf_count < PACKETNUM_ANOMALY_LOW_CONF_RESCUE_CUTOFF):
                        self.packetnum_anomaly_rescued_by_low_conf += 1
                    else:
                        self.packetnum_anomaly_filtered_before_builder += 1
                        continue
                if p.superseded_flag:
                    self.superseded_filtered_before_builder += 1
                    continue
                filtered_packets.append(p)
                filtered_positions.append(positions_in[i] if i < len(positions_in) else None)

            if channel_idx == 1 and len(self._group_builder_input_log) < 100_000:
                for p in filtered_packets:
                    self._group_builder_input_log.append((int(p.packet_num), bool(p.is_valid)))
            pending.extend(filtered_packets)

            # Also extend word positions if available
            if packet_word_positions is not None:
                self._pending_packet_word_positions[channel_idx].extend(filtered_positions)

            if not self._synced_to_packet0_by_channel[channel_idx]:
                if len(pending) > 0:
                    self._synced_to_packet0_by_channel[channel_idx] = True

        # Decode synchronously: all synced channels advance together one group at a time
        synced = [ch for ch in range(1, 5) if self._synced_to_packet0_by_channel[ch]]
        while synced:
            if not all(len(self._pending_packets_by_channel[ch]) >= 8 for ch in synced):
                break

            # Discard the first group — it's a startup artifact where only v2 packets
            # (4-7) are available because the decoder joined mid-stream.
            if not self._first_group_skipped:
                self._first_group_skipped = True
                for channel_idx in synced:
                    self._build_group_with_placeholders(channel_idx)
                continue

            group_values = {}
            group_raw_values = {}
            group_quality = {}
            group_sample_timestamps = None  # NEW: 4 timestamps, one per sample

            # Calculate timestamps for the 4 samples BEFORE processing channels
            first_channel = synced[0]
            if (word_timestamps is not None and
                    len(self._pending_packet_word_positions[first_channel]) >= 8):

                # deque doesn't support slicing - islice() peeks the first 8
                # without mutating (unlike pop/popleft elsewhere in this class)
                group_positions = list(itertools.islice(self._pending_packet_word_positions[first_channel], 8))
                group_packets = list(itertools.islice(self._pending_packets_by_channel[first_channel], 8))

                group_sample_timestamps = np.full(4, np.nan, dtype=np.float64)

                # Find packet 0's reception time as our reference
                packet_0_timestamp = None
                for i, pkt in enumerate(group_packets):
                    if pkt.packet_num == 0 and i < len(group_positions):
                        pos = group_positions[i]
                        if pos is not None and 0 <= pos < len(word_timestamps):
                            packet_0_timestamp = word_timestamps[pos]
                            break

                # If packet 0 not available, use packet 7 and adjust
                if packet_0_timestamp is None:
                    for i, pkt in enumerate(group_packets):
                        if pkt.packet_num == 7 and i < len(group_positions):
                            pos = group_positions[i]
                            if pos is not None and 0 <= pos < len(word_timestamps):
                                packet_7_timestamp = word_timestamps[pos]
                                # Packet 7 is sent 17.5ms after packet 0
                                packet_0_timestamp = packet_7_timestamp - 0.0175
                                break

                if packet_0_timestamp is not None:
                    # Sample timestamps accounting for FPGA pipeline delay
                    # Sample 0 was captured 15ms before packet 0 was sent
                    group_sample_timestamps[0] = packet_0_timestamp - 0.015
                    group_sample_timestamps[1] = packet_0_timestamp - 0.010
                    group_sample_timestamps[2] = packet_0_timestamp - 0.005
                    group_sample_timestamps[3] = packet_0_timestamp - 0.000

            for channel_idx in synced:
                group, group_positions = self._build_group_with_placeholders(channel_idx)

                values = np.full(4, np.nan, dtype=np.float64)
                quality = np.zeros(4, dtype=np.int8)
                sample_indices = np.zeros(4, dtype=np.int64)
                mismatch_v1 = np.full(4, np.nan, dtype=np.float64)
                mismatch_v2 = np.full(4, np.nan, dtype=np.float64)
                mismatch_pending = np.zeros(4, dtype=bool)
                raw_ints = np.zeros(4, dtype=np.int32)
                mismatch_r1 = np.zeros(4, dtype=np.int32)
                mismatch_r2 = np.zeros(4, dtype=np.int32)
                error_occurred = np.zeros(4, dtype=bool)

                for s in range(4):
                    p1 = group[s]
                    p2 = group[s + 4]
                    sample_idx = self.decoded_sample_count_by_channel[channel_idx]
                    self.decoded_sample_count_by_channel[channel_idx] += 1
                    sample_indices[s] = int(sample_idx)

                    payload_v1 = p1.bits[:20] if p1.is_valid and p1.bits is not None and len(p1.bits) >= 20 else None
                    payload_v2 = p2.bits[:20] if p2.is_valid and p2.bits is not None and len(p2.bits) >= 20 else None

                    if payload_v1 is not None and payload_v2 is not None:
                        diff_positions = np.flatnonzero(payload_v1 != payload_v2)
                        if diff_positions.size > 0:
                            self.bit_mismatch_events_by_channel[channel_idx].append(
                                {
                                    'sample_idx': int(sample_idx),
                                    'packet_v1': int(p1.packet_num),
                                    'packet_v2': int(p2.packet_num),
                                    'hamming': int(diff_positions.size),
                                    'diff_positions': diff_positions.astype(np.int16).tolist(),
                                    'bits_v1': ''.join(str(int(x)) for x in payload_v1),
                                    'bits_v2': ''.join(str(int(x)) for x in payload_v2),
                                }
                            )

                    v1 = self._decode_value_from_packet_bits(p1.bits) if p1.is_valid else None
                    v2 = self._decode_value_from_packet_bits(p2.bits) if p2.is_valid else None
                    r1 = self._decode_raw_int_from_packet_bits(p1.bits) if p1.is_valid else None
                    r2 = self._decode_raw_int_from_packet_bits(p2.bits) if p2.is_valid else None

                    if self.debug_packet_window is not None:
                        _dbg_t = sample_idx / self.output_rate_hz
                        _dbg_lo, _dbg_hi = self.debug_packet_window
                        if _dbg_lo <= _dbg_t <= _dbg_hi and channel_idx == 3:
                            _b1 = ''.join(str(int(x)) for x in p1.bits) if p1.bits is not None else ''
                            _b2 = ''.join(str(int(x)) for x in p2.bits) if p2.bits is not None else ''
                            print(
                                f'[dbg] ch{channel_idx} s={s} idx={sample_idx} t={_dbg_t:.4f}s '
                                f'| v1 pkt#{p1.packet_num} valid={p1.is_valid} err={p1.error_flag} '
                                f'bits={len(p1.bits) if p1.bits is not None else 0} raw={r1} [{_b1}] '
                                f'| v2 pkt#{p2.packet_num} valid={p2.is_valid} err={p2.error_flag} '
                                f'bits={len(p2.bits) if p2.bits is not None else 0} raw={r2} [{_b2}]'
                            )

                    # Match check first — error flags don't matter if both copies agree.
                    # q=3 always means the received data matched, regardless of error flags.
                    if v1 is not None and v2 is not None and r1 == r2:
                        values[s] = v1
                        quality[s] = 3
                        raw_ints[s] = r1
                        # error_occurred stays False — no error bit for matched samples
                        if p1.error_flag and p2.error_flag:
                            self.only_side_cause_counts_by_channel[channel_idx]['both_error_matched'] += 1
                        elif p1.error_flag != p2.error_flag:
                            self.only_side_cause_counts_by_channel[channel_idx]['error_flag_mismatch_data_matched'] += 1
                    else:
                        # Not a match (mismatch or one/both sides missing).
                        # low_conf_count takes priority over error_flag for now:
                        # error_flag is really bitThreshold_hdl's
                        # Changeofstrengthflag (avg0 > avg1_at_peak at a payload
                        # sample) - a weak/indirect signal on its own, not a
                        # direct per-bit error indicator, unlike low_conf_count
                        # which this session's offline duplicate-pair margin
                        # analysis validated as reliably picking the correct
                        # side. Falls back to error_flag only when
                        # low_conf_count ties (including both sides at 0).
                        error_occurred[s] = p1.error_flag or p2.error_flag
                        suppressed_side = None      # 'v1' or 'v2'
                        suppression_reason = None   # 'low_conf_count' or 'error_flag'
                        if v1 is not None and v2 is not None and p1.low_conf_count != p2.low_conf_count:
                            if p1.low_conf_count > p2.low_conf_count:
                                v1, r1 = None, None
                                suppressed_side, suppression_reason = 'v1', 'low_conf_count'
                            else:
                                v2, r2 = None, None
                                suppressed_side, suppression_reason = 'v2', 'low_conf_count'
                        elif p1.error_flag and not p2.error_flag and v2 is not None:
                            v1, r1 = None, None
                            suppressed_side, suppression_reason = 'v1', 'error_flag'
                        elif p2.error_flag and not p1.error_flag and v1 is not None:
                            v2, r2 = None, None
                            suppressed_side, suppression_reason = 'v2', 'error_flag'

                        if v1 is not None and v2 is not None:
                            # Mismatch with no clear error-flag preference (both or neither errored)
                            mismatch_v1[s] = float(v1)
                            mismatch_v2[s] = float(v2)
                            mismatch_r1[s] = r1
                            mismatch_r2[s] = r2
                            mismatch_pending[s] = True
                        elif v1 is not None:
                            values[s] = v1
                            quality[s] = 1
                            raw_ints[s] = r1
                            if not p2.is_valid:
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v1_v2_packet_missing'] += 1
                                self.only_side_missing_packetnum_by_channel[channel_idx]['for_only_v1'][int(p2.packet_num) % 8] += 1
                            elif suppression_reason == 'low_conf_count':
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v1_v2_low_conf_suppressed'] += 1
                            elif p2.error_flag:
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v1_v2_error_suppressed'] += 1
                            else:
                                # Genuinely short: p2 is valid, no error flag, but bits < 20
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v1_v2_payload_short'] += 1
                                ev = {
                                    'sample_idx': int(sample_idx),
                                    'ch': channel_idx,
                                    'short_side': 'v2',
                                    'short_pkt': int(p2.packet_num),
                                    'short_bits_len': len(p2.bits) if p2.bits is not None else 0,
                                    'short_bits': ''.join(str(int(x)) for x in p2.bits) if p2.bits is not None else '',
                                    'short_error_flag': p2.error_flag,
                                    'good_pkt': int(p1.packet_num),
                                    'good_bits_len': len(p1.bits) if p1.bits is not None else 0,
                                    'good_error_flag': p1.error_flag,
                                }
                                self.payload_short_log_by_channel[channel_idx].append(ev)
                                total_short = self.only_side_cause_counts_by_channel[channel_idx]['only_v1_v2_payload_short']
                                if total_short <= 20:
                                    t = sample_idx / self.output_rate_hz
                                    print(
                                        f'⚠️  payload_short ch{channel_idx} sample={sample_idx} t={t:.3f}s '
                                        f'v2(short) pkt#{ev["short_pkt"]} bits={ev["short_bits_len"]} [{ev["short_bits"]}] | '
                                        f'v1(good) pkt#{ev["good_pkt"]} bits={ev["good_bits_len"]}'
                                    )
                        elif v2 is not None:
                            values[s] = v2
                            quality[s] = 2
                            raw_ints[s] = r2
                            if not p1.is_valid:
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v2_v1_packet_missing'] += 1
                                self.only_side_missing_packetnum_by_channel[channel_idx]['for_only_v2'][int(p1.packet_num) % 8] += 1
                            elif suppression_reason == 'low_conf_count':
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v2_v1_low_conf_suppressed'] += 1
                            elif p1.error_flag:
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v2_v1_error_suppressed'] += 1
                            else:
                                # Genuinely short: p1 is valid, no error flag, but bits < 20
                                self.only_side_cause_counts_by_channel[channel_idx]['only_v2_v1_payload_short'] += 1
                                ev = {
                                    'sample_idx': int(sample_idx),
                                    'ch': channel_idx,
                                    'short_side': 'v1',
                                    'short_pkt': int(p1.packet_num),
                                    'short_bits_len': len(p1.bits) if p1.bits is not None else 0,
                                    'short_bits': ''.join(str(int(x)) for x in p1.bits) if p1.bits is not None else '',
                                    'short_error_flag': p1.error_flag,
                                    'good_pkt': int(p2.packet_num),
                                    'good_bits_len': len(p2.bits) if p2.bits is not None else 0,
                                    'good_error_flag': p2.error_flag,
                                }
                                self.payload_short_log_by_channel[channel_idx].append(ev)
                                total_short = self.only_side_cause_counts_by_channel[channel_idx]['only_v2_v1_payload_short']
                                if total_short <= 20 and not self.quiet:
                                    t = sample_idx / self.output_rate_hz
                                    print(
                                        f'⚠️  payload_short ch{channel_idx} sample={sample_idx} t={t:.3f}s '
                                        f'v1(short) pkt#{ev["short_pkt"]} bits={ev["short_bits_len"]} [{ev["short_bits"]}] | '
                                        f'v2(good) pkt#{ev["good_pkt"]} bits={ev["good_bits_len"]}'
                                    )
                        else:
                            quality[s] = 0
                            # raw_ints[s] stays 0; will be overwritten by carry-forward in _append_gcs_group
                            if self._carry_forward_log_count < self._carry_forward_log_max and not self.quiet:
                                self._carry_forward_log_count += 1
                                _r1_attempt = self._decode_raw_int_from_packet_bits(p1.bits) if p1.bits is not None and len(p1.bits) >= 20 else None
                                _r2_attempt = self._decode_raw_int_from_packet_bits(p2.bits) if p2.bits is not None and len(p2.bits) >= 20 else None
                                print(
                                    f'[carry-fwd #{self._carry_forward_log_count}] ch{channel_idx} slot{s} '
                                    f'idx={sample_idx} t={sample_idx/self.output_rate_hz:.3f}s → both copies invalid, carry-forward will be used. '
                                    f'v1: pkt#{p1.packet_num} valid={p1.is_valid} err={p1.error_flag} reason={p1.reason!r} '
                                    f'bits={len(p1.bits) if p1.bits is not None else 0} raw_attempt={_r1_attempt} | '
                                    f'v2: pkt#{p2.packet_num} valid={p2.is_valid} err={p2.error_flag} reason={p2.reason!r} '
                                    f'bits={len(p2.bits) if p2.bits is not None else 0} raw_attempt={_r2_attempt}'
                                )
                                if self._carry_forward_log_count == self._carry_forward_log_max:
                                    print(f'[carry-fwd] log limit ({self._carry_forward_log_max}) reached, suppressing further carry-forward logs')

                prev_neighbor = None
                if len(self.decoded_groups_by_channel[channel_idx]) > 0:
                    prev_group = self.decoded_groups_by_channel[channel_idx][-1]
                    if len(prev_group) > 0 and np.isfinite(prev_group[-1]):
                        prev_neighbor = float(prev_group[-1])

                for s in range(4):
                    if not mismatch_pending[s]:
                        continue

                    left_neighbor = values[s - 1] if s > 0 and np.isfinite(values[s - 1]) else prev_neighbor
                    right_neighbor = values[s + 1] if s < 3 and np.isfinite(values[s + 1]) else None
                    chosen_value, chosen_quality, picked, pick_basis = self._pick_mismatch_value(
                        float(mismatch_v1[s]),
                        float(mismatch_v2[s]),
                        left_neighbor,
                        right_neighbor,
                    )
                    values[s] = chosen_value
                    quality[s] = chosen_quality
                    raw_ints[s] = mismatch_r1[s] if picked == 'v1' else mismatch_r2[s]

                    p1 = group[s]
                    p2 = group[s + 4]
                    delta = abs(float(mismatch_v1[s]) - float(mismatch_v2[s]))
                    self.mismatch_events_by_channel[channel_idx].append(
                        {
                            'sample_idx': int(sample_indices[s]),
                            'packet_v1': int(p1.packet_num),
                            'packet_v2': int(p2.packet_num),
                            'v1': float(mismatch_v1[s]),
                            'v2': float(mismatch_v2[s]),
                            'delta': float(delta),
                            'picked': picked,
                            'pick_basis': pick_basis,
                            'left_neighbor': None if left_neighbor is None else float(left_neighbor),
                            'right_neighbor': None if right_neighbor is None else float(right_neighbor),
                        }
                    )

                quality[error_occurred] |= np.int8(0x08)

                # Characterization-only diagnostics, parallel to quality[] -
                # NOT part of the persisted quality_packed/GCS path. See
                # get_decoded_flags() and LOW_CONF_COUNT_THRESHOLD.
                flags = np.zeros(4, dtype=DECODED_FLAGS_DTYPE)
                bits_rec = np.zeros(4, dtype=DECODED_BITS_DTYPE)
                for s in range(4):
                    p1, p2 = group[s], group[s + 4]
                    flags['changeofstrength_v1'][s] = p1.changeofstrength_flag
                    flags['changeofstrength_v2'][s] = p2.changeofstrength_flag
                    flags['packetnum_anomaly_v1'][s] = p1.packetnum_anomaly_flag
                    flags['packetnum_anomaly_v2'][s] = p2.packetnum_anomaly_flag
                    flags['low_conf_v1'][s] = p1.low_conf_count >= LOW_CONF_COUNT_THRESHOLD
                    flags['low_conf_v2'][s] = p2.low_conf_count >= LOW_CONF_COUNT_THRESHOLD
                    flags['superseded_v1'][s] = p1.superseded_flag
                    flags['superseded_v2'][s] = p2.superseded_flag

                    v1_ok = p1.is_valid and p1.bits is not None and len(p1.bits) >= DECODED_BITS_WIDTH
                    v2_ok = p2.is_valid and p2.bits is not None and len(p2.bits) >= DECODED_BITS_WIDTH
                    bits_rec['v1_missing'][s] = not v1_ok
                    bits_rec['v2_missing'][s] = not v2_ok
                    if v1_ok:
                        bits_rec['v1_bits'][s] = p1.bits[:DECODED_BITS_WIDTH]
                    if v2_ok:
                        bits_rec['v2_bits'][s] = p2.bits[:DECODED_BITS_WIDTH]

                    pos_v1 = group_positions[s] if s < len(group_positions) else None
                    pos_v2 = group_positions[s + 4] if s + 4 < len(group_positions) else None
                    bits_rec['v1_word_pos'][s] = pos_v1 if pos_v1 is not None else -1
                    bits_rec['v2_word_pos'][s] = pos_v2 if pos_v2 is not None else -1
                    bits_rec['v1_low_conf'][s] = p1.low_conf_count
                    bits_rec['v2_low_conf'][s] = p2.low_conf_count
                    bits_rec['v1_packet_num'][s] = p1.packet_num
                    bits_rec['v2_packet_num'][s] = p2.packet_num

                self.decoded_groups_by_channel[channel_idx].append(values)
                # if channel_idx == 2:
                    # print(f"Decoded group for channel {channel_idx}, sample_idx={sample_indices[0]}-{sample_indices[-1]}: values={values} quality={quality} raw_ints={raw_ints}")
                self.decoded_quality_by_channel[channel_idx].append(quality)
                self.decoded_flags_by_channel[channel_idx].append(flags)
                self.decoded_bits_by_channel[channel_idx].append(bits_rec)
                if channel_idx == self.channel_to_decode:
                    self._last_decoded_sample_time = time.time()
                group_values[channel_idx] = values
                group_quality[channel_idx] = quality
                group_raw_values[channel_idx] = raw_ints

            if any(ch in self.gcs_channels for ch in synced):
                self._append_gcs_group(group_values, group_raw_values, group_quality, group_sample_timestamps)

    def processing_thread(self):
        while True:
            try:
                item = self.data_queue.get(timeout=0.5)
                # print("got item from queue", "data shape" if isinstance(item, tuple) else "data", item[0].shape if isinstance(item, tuple) else item.shape,self.secondary_reader is not None)
            except queue.Empty:
                continue

            if item is None:
                break

            # Unpack data and timestamp
            if isinstance(item, tuple):
                chunk, chunk_first_word_timestamp = item
            else:
                chunk = item
                chunk_first_word_timestamp = None

            if len(chunk) == 0:
                continue
            
            # Build timestamp array for this chunk
            # Each word's timestamp based on its position in the chunk at 100 kHz
            chunk_timestamps = None
            if chunk_first_word_timestamp is not None:
                chunk_timestamps = chunk_first_word_timestamp + np.arange(len(chunk)) / 100e3
            
            # Append chunk to decode buffer
            self._decode_buffer = np.concatenate([self._decode_buffer, chunk])
            
            # Append timestamps
            if chunk_timestamps is not None:
                if not hasattr(self, '_word_timestamps') or self._word_timestamps is None:
                    self._word_timestamps = []
                self._word_timestamps.extend(chunk_timestamps)
            
            self._current_chunk_first_word_timestamp = chunk_first_word_timestamp
            result, consumed = self._extract_channel_packets(self._decode_buffer)

            if consumed > 0:
                # Unpack packets and positions
                packets_by_channel, packet_word_positions = result
                self._dump_raw_packets(packets_by_channel, packet_word_positions)

                # Extract timestamps for consumed words
                consumed_word_timestamps = None
                if hasattr(self, '_word_timestamps') and self._word_timestamps is not None and len(self._word_timestamps) >= consumed:
                    consumed_word_timestamps = self._word_timestamps[:consumed]
                    self._word_timestamps = self._word_timestamps[consumed:]

                self._decode_packet_groups(
                    packets_by_channel,
                    packet_word_positions=packet_word_positions,
                    word_timestamps=consumed_word_timestamps
                )
                self._words_processed_total += consumed
                self._decode_buffer = self._decode_buffer[consumed:]
                if chunk_first_word_timestamp is not None:
                    self._last_chunk_end_word_timestamp = chunk_first_word_timestamp + consumed / self.bit_clock_hz

        # Final flush
        if len(self._decode_buffer) > 0:
            packets, consumed = self._extract_channel_packets(
                np.concatenate([self._decode_buffer, np.array([0], dtype=np.uint16)])
            )
            if consumed > 0:
                packets_by_channel, packet_word_positions = packets
                self._dump_raw_packets(packets_by_channel, packet_word_positions)
                consumed_word_timestamps = None
                if hasattr(self, '_word_timestamps') and self._word_timestamps is not None and len(self._word_timestamps) >= consumed:
                    consumed_word_timestamps = self._word_timestamps[:consumed]
                self._decode_packet_groups(
                    packets_by_channel,
                    packet_word_positions=packet_word_positions,
                    word_timestamps=consumed_word_timestamps
                )
                self._words_processed_total += consumed

    def reset_decoder_state(self):
        self.decoded_groups_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        self.decoded_quality_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        # Per-sample characterization diagnostics (changeofstrength/packetnum
        # anomaly/low_conf, both copies) - parallel to decoded_quality_by_channel,
        # same group-append cadence, not part of the persisted quality_packed/GCS
        # path. See get_decoded_flags().
        self.decoded_flags_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        # Raw pre-decode bits for both copies, for ground-truth (PRBS) BER
        # comparison - same parallel structure as decoded_flags_by_channel.
        # See get_decoded_bits().
        self.decoded_bits_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        self.decoded_sample_count_by_channel = {ch: 0 for ch in range(1, 5)}
        self.mismatch_events_by_channel = {
            ch: deque(maxlen=2000) for ch in range(1, 5)
        }
        self.bit_mismatch_events_by_channel = {
            ch: deque(maxlen=2000) for ch in range(1, 5)
        }
        self.payload_short_log_by_channel = {
            ch: deque(maxlen=500) for ch in range(1, 5)
        }
        self.valid_flag_bitcount_hist_by_channel = {
            ch: Counter() for ch in range(1, 5)
        }
        self.only_side_cause_counts_by_channel = {
            ch: Counter() for ch in range(1, 5)
        }
        self.only_side_missing_packetnum_by_channel = {
            ch: {
                'for_only_v1': np.zeros(8, dtype=np.int64),
                'for_only_v2': np.zeros(8, dtype=np.int64),
            }
            for ch in range(1, 5)
        }
        self.resync_drops_by_channel = {ch: 0 for ch in range(1, 5)}
        self.packet_sequence_events = deque(maxlen=2000)
        self.packet_sequence_anomaly_count = 0
        self.packet_sequence_header_drops = 0
        self.prefix_overlap_frames_skipped = 0
        self.placeholder_inserts_cross_chunk = 0
        self.placeholder_inserts_intra_chunk = 0
        self.placeholder_inserts_group_builder = 0
        self.low_conf_filtered_before_builder = 0
        self.packetnum_anomaly_filtered_before_builder = 0
        self.packetnum_anomaly_rescued_by_low_conf = 0
        self.superseded_filtered_before_builder = 0
        self.packet_num_distance_corrections = 0  # frames relabeled from noisy pkt# to distance value
        self._phase_locked = False           # when locked, distance overrides noisy pkt# field
        self._phase_agreement_run = 0        # consecutive detect-vs-distance agreements toward lock
        self._phase_mismatch_run = 0         # leaky mismatch run toward re-acquire unlock
        self.phase_relocks = 0               # times the phase lock was dropped to re-acquire
        self._group_builder_input_log = []   # (packet_num, is_valid) for ch1, capped at 2000
        self._group_builder_decision_log = []
        self.gap_estimate_agree_count = 0
        self.gap_estimate_disagree_count = 0
        self.decoded_groups = self.decoded_groups_by_channel[self.channel_to_decode]
        self.decoded_quality = self.decoded_quality_by_channel[self.channel_to_decode]
        self._decode_buffer = np.array([], dtype=np.uint16)
        # deque, not list (2026-08-03): _build_group_with_placeholders drains
        # these from the FRONT every group (pending.pop(0)/positions.pop(0)) -
        # on a plain list that's O(n) per pop, shifting every remaining
        # element down. Profiled as 72% of total decode() runtime (57.6s of
        # 79.3s on a 229s recording) via cProfile - deque.popleft() is O(1).
        self._pending_packets_by_channel = {ch: deque() for ch in range(1, 5)}
        self._synced_to_packet0_by_channel = {ch: False for ch in range(1, 5)}
        self._last_extracted_packet_num = None  # persists across chunk calls for cross-chunk gap detection
        self._last_extracted_frame_abs_word_start = None  # absolute word start of last extracted valid frame
        self._extract_lookback_words = np.array([], dtype=np.uint16)  # preserves 2-word context for -2 bit alignment
        self._words_processed_total = 0       # absolute word offset for timestamps
        self._raw_frame_log = []              # list of (abs_word_idx, packet_num, frame_length, passed_valid)
        self._raw_packet_bits_log = {ch: [] for ch in range(1, 5)}  # list of (abs_word_idx, 20-bit array) per
            # channel, real (is_valid=True) packets ONLY, logged before group_builder/any pre-filter - see the
            # single append site's own comment (2026-08-03, for "raw BER" computed independent of packet-level
            # accept/reject decisions)
        self._pending_packet_word_positions = {ch: deque() for ch in range(1, 5)}
        self._first_group_skipped = False
        self.gcs_write_buffer = []

    def decode_from_word_stream(self, word_stream, reset=True):
        if reset:
            self.reset_decoder_state()

        words = np.asarray(word_stream).reshape(-1)
        if words.size == 0:
            return self.get_decoded_arrays()

        if np.iscomplexobj(words):
            words = np.real(words)

        words_u16 = words.astype(np.uint16, copy=False)
        self._decode_buffer = np.concatenate([self._decode_buffer, words_u16])

        result, consumed = self._extract_channel_packets(self._decode_buffer)
        if consumed > 0:
            packets_by_channel, packet_word_positions = result
            self._decode_packet_groups(packets_by_channel, packet_word_positions=packet_word_positions)
            self._words_processed_total += consumed
            self._decode_buffer = self._decode_buffer[consumed:]

        if len(self._decode_buffer) > 0:
            padded = np.concatenate([self._decode_buffer, np.array([0], dtype=np.uint16)])
            result, consumed = self._extract_channel_packets(padded)
            if consumed > 0:
                packets_by_channel, packet_word_positions = result
                self._decode_packet_groups(packets_by_channel, packet_word_positions=packet_word_positions)
                self._words_processed_total += consumed
            self._decode_buffer = np.array([], dtype=np.uint16)

        return self.get_decoded_arrays()

    @staticmethod
    def _unwrap_mat_scalar(value):
        while isinstance(value, np.ndarray) and value.size == 1:
            value = value.item()
        return value

    def _extract_matlab_word_stream(self, mat_data, variable_name=None):
        if variable_name and variable_name in mat_data:
            candidate = mat_data[variable_name]
            return np.asarray(candidate).reshape(-1)

        for key in ('output_data', 'data_words', 'packet_words'):
            if key in mat_data:
                return np.asarray(mat_data[key]).reshape(-1)

        sim_out = mat_data.get('simOut')
        if sim_out is not None:
            sim_out = self._unwrap_mat_scalar(sim_out)
            sim_concat = getattr(sim_out, 'sim_concat', None)
            if sim_concat is not None:
                sim_concat = self._unwrap_mat_scalar(sim_concat)
                sim_data = getattr(sim_concat, 'Data', None)
                if sim_data is not None:
                    return np.asarray(sim_data).reshape(-1)

        sim_concat = mat_data.get('sim_concat')
        if sim_concat is not None:
            sim_concat = self._unwrap_mat_scalar(sim_concat)
            sim_data = getattr(sim_concat, 'Data', None)
            if sim_data is not None:
                return np.asarray(sim_data).reshape(-1)

        raise ValueError(
            'Could not find MATLAB word stream. Provide variable_name or export one of: output_data, data_words, packet_words.'
        )

    def decode_from_mat_file(self, mat_path, variable_name=None, reset=True):
        try:
            mat_data = loadmat(mat_path, squeeze_me=True, struct_as_record=False)
            words = self._extract_matlab_word_stream(mat_data, variable_name=variable_name)
        except NotImplementedError:
            import h5py

            with h5py.File(mat_path, 'r') as mat_file:
                key = variable_name if variable_name else 'output_data'
                if key not in mat_file:
                    raise ValueError(
                        f'MAT v7.3 file requires dataset name. Could not find "{key}" in {mat_path}.'
                    )
                words = np.array(mat_file[key]).reshape(-1)

        return self.decode_from_word_stream(words, reset=reset)

    def decode_from_bin_file(
        self,
        bin_path,
        reset=True,
        file_format='auto',
        dtype=np.uint16,
        byteorder='little',
        matlab_scale=2048.0,
        matlab_select='first_row',
        iq_plot_file=None,
    ):
        raw = np.fromfile(bin_path, dtype=dtype)
        print(len(raw), 'raw samples read from binary file.')
        if raw.size == 0:
            return self.decode_from_word_stream(raw, reset=reset)

        if byteorder == 'big':
            raw = raw.byteswap().newbyteorder()

        fmt = file_format.lower()
        if fmt not in ('auto', 'word_stream', 'rx_x2_interleaved', 'matlab_float32_2xn', 'gnuradio_cf32'):
            raise ValueError("file_format must be 'auto', 'word_stream', 'rx_x2_interleaved', 'matlab_float32_2xn', or 'gnuradio_cf32'")

        if fmt == 'gnuradio_cf32':
            # Dual-channel BladeRF layout (float32): [RX0_I, RX0_Q, RX1_I, RX1_Q, ...]  stride-4.
            # The decode word stream uses stride-2 (same behaviour as before IQ plotting was added).
            # The IQ plot uses stride-4 per _extract_output_stream to correctly isolate each
            # rx_channel's I and Q components.
            float_raw = np.fromfile(bin_path, dtype=np.float32)
            if float_raw.size < 2:
                return self.decode_from_word_stream(np.array([], dtype=np.uint16), reset=reset)
            usable = (float_raw.size // 4) * 4
            float_raw = float_raw[:usable]
            # ── Decode word stream (stride-2, unchanged) ───────────────────────────
            i_float  = float_raw[0::2]
            i_int16  = np.round(i_float * float(matlab_scale)).astype(np.int16)
            words    = i_int16.view(np.uint16)
            print(f'[file decode] gnuradio_cf32: {float_raw.size} float32 → '
                  f'{len(i_float)} stride-2 samples → {len(words)} words (scale={matlab_scale})')
            print(f'[file decode] I float range: '
                  f'min={float(i_float.min()):.4f} max={float(i_float.max()):.4f} '
                  f'mean={float(i_float.mean()):.4f}')
            print(f'[file decode] word (int16) range: '
                  f'min={int(i_int16.min())} max={int(i_int16.max())} '
                  f'first8={i_int16[:8].tolist()}')
            # ── IQ window data — stored for plot_channel to combine into one figure ──
            if iq_plot_file is not None and self.debug_packet_window is not None:
                iq_raw = np.fromfile(iq_plot_file, dtype=np.float32)
                iq_usable = (iq_raw.size // 2) * 2
                i_ch_float = iq_raw[:iq_usable][0::2]
                q_ch_float = iq_raw[:iq_usable][1::2]
                t_s = np.arange(len(i_ch_float)) / self.sample_rate
                win_lo, win_hi = self.debug_packet_window
                mask = (t_s >= win_lo) & (t_s <= win_hi)
                self._iq_plot_data = (
                    t_s[mask], i_ch_float[mask], q_ch_float[mask],
                    win_lo, win_hi, os.path.basename(iq_plot_file),
                )
            else:
                self._iq_plot_data = None
            return self.decode_from_word_stream(words[0::80], reset=reset)

        if fmt == 'matlab_float32_2xn':
            float_raw = np.fromfile(bin_path, dtype=np.float32)
            if float_raw.size < 2:
                return self.decode_from_word_stream(np.array([], dtype=np.uint16), reset=reset)

            usable = (float_raw.size // 2) * 2
            float_raw = float_raw[:usable]

            # Match MATLAB: int16(round(fread(..., [2 N], 'float32') * 2048))
            mat_2xn = float_raw.reshape((2, -1), order='F')
            mat_i16 = np.round(mat_2xn * float(matlab_scale)).astype(np.int16)

            select_mode = str(matlab_select).lower()
            if select_mode in ('first_column', 'col0', 'column0'):
                selected = mat_i16[:, 0]
            elif select_mode in ('first_row', 'row0', 'channel1'):
                selected = mat_i16[0, :]
            elif select_mode in ('second_row', 'row1', 'channel2'):
                selected = mat_i16[1, :]
            else:
                raise ValueError("matlab_select must be 'first_column', 'first_row', or 'second_row'")

            words = selected.astype(np.uint16, copy=False)
            return self.decode_from_word_stream(words, reset=reset)

        if fmt == 'word_stream':
            words = raw.astype(np.uint16, copy=False)
            return self.decode_from_word_stream(words, reset=reset)

        if fmt == 'rx_x2_interleaved':
            if raw.size < 4:
                return self.decode_from_word_stream(np.array([], dtype=np.uint16), reset=reset)
            usable = (raw.size // 4) * 4
            interleaved = raw[:usable].astype(np.uint16, copy=False)
            words = self._extract_output_stream(interleaved)
            print(f'[file decode] rx_x2_interleaved: {raw.size} raw uint16 → {len(words)} words '
                  f'(offset={self.rx_channel * 2 + (self.device_num - 1)}, stride=4)')
            print(f'[file decode] word range: min={int(words.min())} max={int(words.max())} '
                  f'mean={float(words.astype(np.int16).mean()):.1f} '
                  f'first8={words[:8].astype(np.int16).tolist()}')
            return self.decode_from_word_stream(words, reset=reset)

        # auto: if divisible by 4, assume raw BladeRF RX_X2 dump; otherwise direct word stream
        if raw.size % 4 == 0:
            interleaved = raw.astype(np.uint16, copy=False)
            words = self._extract_output_stream(interleaved)
        else:
            words = raw.astype(np.uint16, copy=False)

        return self.decode_from_word_stream(words, reset=reset)

    def decode_from_file(
        self,
        input_path,
        variable_name=None,
        reset=True,
        bin_file_format='auto',
        matlab_scale=2048.0,
        matlab_select='first_row',
        iq_plot_file=None,
    ):
        lower = input_path.lower()
        if lower.endswith('.mat'):
            return self.decode_from_mat_file(input_path, variable_name=variable_name, reset=reset)
        if lower.endswith('.bin'):
            return self.decode_from_bin_file(
                input_path,
                reset=reset,
                file_format=bin_file_format,
                matlab_scale=matlab_scale,
                matlab_select=matlab_select,
                iq_plot_file=iq_plot_file,
            )
        if lower.endswith('.npy'):
            words = np.load(input_path)
            return self.decode_from_word_stream(words, reset=reset)
        if lower.endswith('.csv'):
            words = np.loadtxt(input_path, delimiter=',')
            return self.decode_from_word_stream(words, reset=reset)
        raise ValueError('Unsupported file type. Use .bin, .mat, .npy, or .csv')

    @staticmethod
    def _normalize_channel_index(channel_idx, default_channel):
        idx = default_channel if channel_idx is None else int(channel_idx)
        if idx < 1 or idx > 4:
            raise ValueError('channel index must be 1..4')
        return idx

    def get_decoded_arrays(self, channel_idx=None):
        idx = self._normalize_channel_index(channel_idx, self.channel_to_decode)
        channel_groups = self.decoded_groups_by_channel[idx]
        channel_quality = self.decoded_quality_by_channel[idx]
        if len(channel_groups) == 0:
            return np.empty((0, 4), dtype=np.float64), np.empty((0, 4), dtype=np.int8)
        values = np.vstack(channel_groups)
        quality = np.vstack(channel_quality)
        return values, quality

    def get_decoded_flags(self, channel_idx=None):
        """Characterization-only diagnostics: changeofstrength_flag,
        packetnum_anomaly_flag, and low_conf_count>=LOW_CONF_COUNT_THRESHOLD,
        for both duplicate copies, per decided sample. Same shape/ordering as
        get_decoded_arrays()'s quality array (ravel and zip directly - no
        separate alignment needed), but NOT part of the persisted
        quality_packed/GCS output - see DecodedPacket's field comments and
        this session's plan for why these are kept separate from that path.
        """
        idx = self._normalize_channel_index(channel_idx, self.channel_to_decode)
        channel_flags = self.decoded_flags_by_channel[idx]
        if len(channel_flags) == 0:
            return np.empty((0, 4), dtype=DECODED_FLAGS_DTYPE)
        return np.vstack(channel_flags)

    def get_decoded_bits(self, channel_idx=None):
        """Raw pre-decode bits for both duplicate copies, per decided sample -
        for ground-truth (e.g. PRBS) BER comparison, as opposed to the
        duplicate-copy-agreement proxy the quality byte encodes. Same
        shape/ordering as get_decoded_arrays()/get_decoded_flags() - zip
        directly, no separate alignment needed. v1_bits/v2_bits are
        zero-filled (not meaningful) wherever v1_missing/v2_missing is True.
        v1_packet_num/v2_packet_num (2026-08-02) are each copy's own raw,
        self-reported packet_num - NOT derived from the output slot index,
        so they can be used to independently verify the group-builder's own
        slot-assignment invariant instead of trusting it.
        """
        idx = self._normalize_channel_index(channel_idx, self.channel_to_decode)
        channel_bits = self.decoded_bits_by_channel[idx]
        if len(channel_bits) == 0:
            return np.empty((0, 4), dtype=DECODED_BITS_DTYPE)
        return np.vstack(channel_bits)

    def get_channel_series(self, channel_idx=None):
        idx = self._normalize_channel_index(channel_idx, self.channel_to_decode)
        values, quality = self.get_decoded_arrays(idx)
        if values.size == 0:
            return np.array([]), np.array([])

        # MATLAB parity: final_out = reshape(samples, [], 1)
        # Here, values has shape (num_groups, 4) where 4 are sample positions in time,
        # not separate channels. Flatten in row-major order to recover 200 Hz stream.
        series = values.reshape(-1).copy()
        quality_series = quality.reshape(-1).copy()

        # Replace non-qual-3 samples using a carry-forward anchored to qual=3 only.
        # qual=1/2/5/6 (partial/mismatch) are kept if the jump from the last qual=3
        # value is within 300 uV; otherwise replaced with carry-forward.
        # qual=0 (no packet / NaN) are always replaced with carry-forward.
        # The error flag (bit 3) is stripped before the decision so that 9/10/11/13/14
        # are treated the same as their non-error counterparts (1/2/3/5/6).
        _SPIKE_THRESHOLD = 20e-3  # volts — max allowed jump from last qual=3 value
        _PARTIAL_QUALS = (1, 2, 5, 6)
        carry = np.nan
        for i in range(len(series)):
            base_q = int(quality_series[i]) & 0x07  # strip error flag
            if base_q == 3:
                carry = series[i]
            elif base_q in _PARTIAL_QUALS:
                v = series[i]
                if np.isnan(v) or (not np.isnan(carry) and abs(v - carry) > _SPIKE_THRESHOLD):
                    series[i] = carry
                # else: plausible value, leave it; carry stays at last qual=3
            else:
                # base_q == 0: no packet — carry-forward
                series[i] = carry

        # MATLAB parity: subtract mean before visualization/spectral analysis
        if len(series) > 0:
            series = series - np.nanmean(series)

        if self.enable_bandpass_filter and len(series) > 16:
            series, self.filter_zi[idx - 1] = signal.lfilter(
                self.b_bandpass,
                self.a_bandpass,
                series,
                zi=self.filter_zi[idx - 1],
            )

        return series, quality_series

    def get_all_channel_series(self):
        return {ch: self.get_channel_series(ch) for ch in range(1, 5)}

    def save_raw_frame_log(self, path: str = 'raw_frame_log.csv') -> None:
        """Write every detected frame (before valid-length gate) to a CSV.

        Columns:
          abs_word_idx  - absolute word position in the incoming stream
          timestamp_s   - abs_word_idx / sample_rate
          packet_num    - 3-bit header value (0-7)
          frame_length  - measured frame length in words
          passed_valid  - 1 if frame_length is in accepted_frame_lengths, else 0
        """
        log = self._raw_frame_log
        if not log:
            print('Raw frame log is empty — nothing to save.')
            return
        import csv
        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['abs_word_idx', 'timestamp_s', 'packet_num', 'frame_length', 'passed_valid'])
            for abs_word, pkt_n, fl, passed in log:
                writer.writerow([abs_word, abs_word / self.sample_rate, pkt_n, fl, int(passed)])
        print(f'Raw frame log saved: {path}  ({len(log)} frames)')

    def print_stats(self, channel_idx=None):
        idx = self._normalize_channel_index(channel_idx, self.channel_to_decode)
        values, quality = self.get_decoded_arrays(idx)
        total = quality.size
        if total == 0:
            print(f'No samples decoded for channel {idx}.')
            return

        base_q = quality & np.int8(0x07)
        err_flag = (quality & np.int8(0x08)) != 0

        print('\n=== Decoded Sample Statistics ===')
        print(f'Channel: {idx}')
        print(f'Total samples: {total}')
        print(f'  No packet (0):               {np.sum(base_q == 0):6d} ({100*np.sum(base_q == 0)/total:5.1f}%)')
        print(f'  Only v1 (1):                 {np.sum(base_q == 1):6d} ({100*np.sum(base_q == 1)/total:5.1f}%)')
        print(f'  Only v2 (2):                 {np.sum(base_q == 2):6d} ({100*np.sum(base_q == 2)/total:5.1f}%)')
        print(f'  Match both (3):              {np.sum(base_q == 3):6d} ({100*np.sum(base_q == 3)/total:5.1f}%)')
        print(f'  Mismatch picked v1 (5):      {np.sum(base_q == 5):6d} ({100*np.sum(base_q == 5)/total:5.1f}%)')
        print(f'  Mismatch picked v2 (6):      {np.sum(base_q == 6):6d} ({100*np.sum(base_q == 6)/total:5.1f}%)')
        err_total = int(np.sum(err_flag))
        err_v2_errored = int(np.sum(quality == np.int8(9)))   # base=1 (only_v1) + error → v2 had error
        err_v1_errored = int(np.sum(quality == np.int8(10)))  # base=2 (only_v2) + error → v1 had error
        err_other      = err_total - err_v2_errored - err_v1_errored
        print(f'  Error flag set (bit 3):      {err_total:6d} ({100*err_total/total:5.1f}%)')
        if err_total > 0:
            print(f'    v2 errored → used v1 only:  {err_v2_errored:6d}')
            print(f'    v1 errored → used v2 only:  {err_v1_errored:6d}')
            print(f'    both/no-fallback errored:   {err_other:6d}')
            both_err_matched = int(self.only_side_cause_counts_by_channel[idx].get('both_error_matched', 0))
            print(f'    both errored but matched:   {both_err_matched:6d}')
            flag_mismatch_data_matched = int(self.only_side_cause_counts_by_channel[idx].get('error_flag_mismatch_data_matched', 0))
            print(f'    flag mismatch but data matched: {flag_mismatch_data_matched:6d}')
        print(f'  Packet drops (resync off):   {self.resync_drops_by_channel[idx]:6d}')
        print(f'  Sequence anomalies:          {self.packet_sequence_anomaly_count:6d}')
        print(f'  Header anomaly drops:        {self.packet_sequence_header_drops:6d}')
        print(f'  Prefix-overlap frames skipped: {self.prefix_overlap_frames_skipped:4d}')
        print(f'  Placeholder inserts (cross): {self.placeholder_inserts_cross_chunk:6d}')
        print(f'  Placeholder inserts (intra): {self.placeholder_inserts_intra_chunk:6d}')
        print(f'  Placeholder inserts (group): {self.placeholder_inserts_group_builder:6d}')
        print(f'  Low-conf packets excluded before group_builder (cutoff={self.group_builder_low_conf_cutoff}): {self.low_conf_filtered_before_builder:6d}')
        print(
            f'  Gap estimate agree/disagree: {self.gap_estimate_agree_count:6d}/{self.gap_estimate_disagree_count:6d}'
        )
        print(f"  Accepted frame lengths:      {','.join(str(v) for v in self.accepted_frame_lengths)}")

        match_count = int(np.sum(base_q == 3))
        if (
            match_count == int(total)
            and (
                self.resync_drops_by_channel[idx] > 0
                or self.packet_sequence_anomaly_count > 0
                or self.packet_sequence_header_drops > 0
            )
        ):
            print('  Note: quality percentages are computed after resync/sequence handling; dropped packets are not included in these percentages.')

        only_v1_count = int(np.sum(base_q == 1))
        only_v2_count = int(np.sum(base_q == 2))
        one_sided_total = only_v1_count + only_v2_count
        if one_sided_total > 0:
            causes = self.only_side_cause_counts_by_channel[idx]
            print('  One-sided decode causes:')
            c1 = int(causes.get('only_v1_v2_packet_missing', 0))
            c2 = int(causes.get('only_v1_v2_payload_short', 0))
            c2e = int(causes.get('only_v1_v2_error_suppressed', 0))
            c3 = int(causes.get('only_v2_v1_packet_missing', 0))
            c4 = int(causes.get('only_v2_v1_payload_short', 0))
            c4e = int(causes.get('only_v2_v1_error_suppressed', 0))
            print(f'    only_v1 <- v2 packet missing  : {c1:6d} ({100*c1/max(one_sided_total,1):5.1f}% of one-sided)')
            print(f'    only_v1 <- v2 error suppressed: {c2e:6d} ({100*c2e/max(one_sided_total,1):5.1f}% of one-sided)')
            print(f'    only_v1 <- v2 payload short   : {c2:6d} ({100*c2/max(one_sided_total,1):5.1f}% of one-sided)')
            print(f'    only_v2 <- v1 packet missing  : {c3:6d} ({100*c3/max(one_sided_total,1):5.1f}% of one-sided)')
            print(f'    only_v2 <- v1 error suppressed: {c4e:6d} ({100*c4e/max(one_sided_total,1):5.1f}% of one-sided)')
            print(f'    only_v2 <- v1 payload short   : {c4:6d} ({100*c4/max(one_sided_total,1):5.1f}% of one-sided)')
            short_events = list(self.payload_short_log_by_channel[idx])
            if short_events:
                print(f'\n  Payload-short events (up to 20 of {len(short_events)} logged):')
                print(f'    {"#":>4}  {"ch":>2}  {"side":>4}  {"pkt":>3}  {"bits":>4}  {"err":>5}  {"t(s)":>8}  bit_pattern')
                for i, ev in enumerate(short_events[:20], 1):
                    t = ev['sample_idx'] / self.output_rate_hz
                    good_side = 'v2' if ev['short_side'] == 'v1' else 'v1'
                    print(
                        f'    {i:>4}  ch{ev["ch"]}  {ev["short_side"]:>4}  '
                        f'#{ev["short_pkt"]:>1}  {ev["short_bits_len"]:>4}  {str(ev["short_error_flag"]):>5}  {t:>8.3f}  '
                        f'[{ev["short_bits"]}]  ({good_side} pkt#{ev["good_pkt"]} bits={ev["good_bits_len"]} err={ev["good_error_flag"]})'
                    )

            hist = self.valid_flag_bitcount_hist_by_channel[idx]
            if hist:
                total_pkts = sum(hist.values())
                expected = self.bits_per_channel // 2
                off_count = sum(cnt for bits, cnt in hist.items() if bits != expected)
                print(f'\n  Valid-flag bit-count histogram (ch{idx}, expected={expected}, total packets={total_pkts}):')
                for bits in sorted(hist):
                    cnt = hist[bits]
                    bar = '█' * min(40, int(40 * cnt / total_pkts))
                    marker = ' ← expected' if bits == expected else (' ← SHORT (<20, payload lost)' if bits < 20 else ' ← EXCESS')
                    print(f'    bits={bits:3d}: {cnt:7d} ({100*cnt/total_pkts:5.1f}%) {bar}{marker}')
                if off_count:
                    print(f'    {off_count} packets ({100*off_count/total_pkts:.1f}%) had bit count != {expected}')

            miss_hist = self.only_side_missing_packetnum_by_channel[idx]
            miss_for_only_v1 = miss_hist['for_only_v1']
            miss_for_only_v2 = miss_hist['for_only_v2']
            if np.any(miss_for_only_v1) or np.any(miss_for_only_v2):
                fmt_v1 = ', '.join([f"{pn}:{int(cnt)}" for pn, cnt in enumerate(miss_for_only_v1) if int(cnt) > 0])
                fmt_v2 = ', '.join([f"{pn}:{int(cnt)}" for pn, cnt in enumerate(miss_for_only_v2) if int(cnt) > 0])
                if fmt_v1:
                    print(f'    missing pkt num seen by only_v1 (v2 side): {fmt_v1}')
                if fmt_v2:
                    print(f'    missing pkt num seen by only_v2 (v1 side): {fmt_v2}')

        known = np.array([0, 1, 2, 3, 5, 6, 8, 9, 10, 11, 13, 14], dtype=np.int8)
        unknown_count = np.sum(~np.isin(quality, known))
        if unknown_count > 0:
            print(f'  Unknown quality codes:       {unknown_count:6d} ({100*unknown_count/total:5.1f}%)')

        bit_mismatch_events = list(self.bit_mismatch_events_by_channel[idx])
        if bit_mismatch_events:
            print('\nBit-level mismatch details (latest events, before float conversion):')
            max_rows = 20
            events_to_show = bit_mismatch_events[-max_rows:]
            if len(bit_mismatch_events) > len(events_to_show):
                print(f'  Showing last {len(events_to_show)} of {len(bit_mismatch_events)} bit mismatch events')
            for ev in events_to_show:
                shown_pos = ev['diff_positions'][:12]
                suffix = '...' if len(ev['diff_positions']) > len(shown_pos) else ''
                ts = ev['sample_idx'] / self.output_rate_hz
                print(
                    f"  sample_idx={ev['sample_idx']:6d} @ t={ts:.3f}s "
                    f"pkt(v1,v2)=({ev['packet_v1']},{ev['packet_v2']}) "
                    f"hamming={ev['hamming']:2d} diff_pos={shown_pos}{suffix}"
                )
                print(f"    bits_v1={ev['bits_v1']}")
                print(f"    bits_v2={ev['bits_v2']}")

        seq_events = list(self.packet_sequence_events)
        if seq_events:
            print('\nPacket sequence anomalies (latest events):')
            max_rows = 20
            events_to_show = seq_events[-max_rows:]
            if len(seq_events) > len(events_to_show):
                print(f'  Showing last {len(events_to_show)} of {len(seq_events)} sequence anomalies')
            for ev in events_to_show:
                ts = ev.get('timestamp_s')
                ts_str = f" @ t={ts:.3f}s" if ts is not None else ''
                print(
                    f"  idx={ev['start_idx']:7d}{ts_str} prev={ev['prev_packet']} "
                    f"expected_next={ev['expected_next']} observed={ev['observed_packet']} "
                    f"expected_at_current={ev['expected_at_current']} "
                    f"distance={ev['distance_words']} (~{ev['expected_frames_in_gap']} frame(s))"
                )

        mismatch_events = list(self.mismatch_events_by_channel[idx])
        if mismatch_events:
            print('\nValue mismatch details (latest events):')
            basis_counts = Counter(ev.get('pick_basis', 'unknown') for ev in mismatch_events)
            print(
                f"  pick basis counts: neighbors={basis_counts.get('neighbors', 0)}, "
                f"magnitude={basis_counts.get('magnitude', 0)}, "
                f"unknown={basis_counts.get('unknown', 0)}"
            )
            max_rows = 30
            events_to_show = mismatch_events[-max_rows:]
            if len(mismatch_events) > len(events_to_show):
                print(f'  Showing last {len(events_to_show)} of {len(mismatch_events)} mismatch events')
            for ev in events_to_show:
                ts = ev['sample_idx'] / self.output_rate_hz
                pick_basis = ev.get('pick_basis', 'unknown')
                left_neighbor = ev.get('left_neighbor', None)
                right_neighbor = ev.get('right_neighbor', None)
                left_str = 'None' if left_neighbor is None else f"{float(left_neighbor):+.6f}"
                right_str = 'None' if right_neighbor is None else f"{float(right_neighbor):+.6f}"
                print(
                    f"  sample_idx={ev['sample_idx']:6d} @ t={ts:.3f}s "
                    f"pkt(v1,v2)=({ev['packet_v1']},{ev['packet_v2']}) "
                    f"v1={ev['v1']:+.6f} v2={ev['v2']:+.6f} "
                    f"delta={ev['delta']:.6f} picked={ev['picked']} "
                    f"basis={pick_basis} neighbors(L,R)=({left_str},{right_str})"
                )

    def _multitaper_spectrogram(self, series, fs, window_sec=2.0, step_sec=0.5, time_bandwidth=3.0, num_tapers=5):
        nperseg = max(32, int(round(window_sec * fs)))
        step = max(1, int(round(step_sec * fs)))
        if len(series) < nperseg:
            return np.array([]), np.array([]), np.empty((0, 0))

        nfft = max(256, nperseg)

        if _somata is not None:
            try:
                if hasattr(_somata, 'multitaper_spectrogram') and callable(_somata.multitaper_spectrogram):
                    f, t, sxx = _somata.multitaper_spectrogram(
                        series,
                        fs=fs,
                        window_length=window_sec,
                        step=step_sec,
                        time_bandwidth=time_bandwidth,
                        num_tapers=num_tapers,
                    )
                    return np.asarray(f), np.asarray(t), np.asarray(sxx)
            except Exception:
                pass

                # DPSS multitaper fallback with proper one-sided PSD normalization.
        tapers = signal.windows.dpss(nperseg, NW=time_bandwidth, Kmax=num_tapers, sym=False)
        starts = np.arange(0, len(series) - nperseg + 1, step)
        freqs = np.fft.rfftfreq(nfft, d=1.0 / fs)
        times = (starts + (nperseg // 2)) / fs
        sxx = np.zeros((len(freqs), len(starts)), dtype=np.float64)

        for idx, start in enumerate(starts):
            segment = series[start:start + nperseg]
            tapered_fft_power = []
            for taper in tapers:
                tapered = (segment - np.mean(segment)) * taper
                spec = np.fft.rfft(tapered, n=nfft)
                # PSD normalization: V^2/Hz (similar convention to scipy/matlab one-sided PSD)
                scale = fs * np.sum(taper ** 2)
                psd = (np.abs(spec) ** 2) / scale
                if nfft % 2 == 0:
                    psd[1:-1] *= 2.0
                else:
                    psd[1:] *= 2.0
                tapered_fft_power.append(psd)
            sxx[:, idx] = np.mean(np.vstack(tapered_fft_power), axis=0)

        return freqs, times, sxx

    def _multitaper_psd(self, series, fs, time_bandwidth=3.5, num_tapers=None, nfft=None):
        if len(series) < 32:
            return np.array([]), np.array([])

        data = np.asarray(series, dtype=np.float64)
        data = data - np.mean(data)

        if num_tapers is None:
            num_tapers = max(3, int(2 * time_bandwidth) - 1)

        if nfft is None:
            nfft = max(256, 1 << int(np.ceil(np.log2(len(data)))))

        tapers = signal.windows.dpss(len(data), NW=time_bandwidth, Kmax=num_tapers, sym=False)
        freqs = np.fft.rfftfreq(nfft, d=1.0 / fs)
        psd_accum = np.zeros(len(freqs), dtype=np.float64)

        for taper in tapers:
            tapered = data * taper
            spec = np.fft.rfft(tapered, n=nfft)
            scale = fs * np.sum(taper ** 2)
            psd = (np.abs(spec) ** 2) / scale
            if nfft % 2 == 0:
                psd[1:-1] *= 2.0
            else:
                psd[1:] *= 2.0
            psd_accum += psd

        return freqs, psd_accum / len(tapers)

    # Shared quality→colour styling for the decoded time-series scatter.
    _QUAL_STYLE = {
        0: ('red',       'q=0 no_packet'),
        1: ('orange',    'q=1 only_v1'),
        2: ('gold',      'q=2 only_v2'),
        3: ('steelblue', 'q=3 both_match'),
        5: ('magenta',   'q=5 mismatch→v1'),
        6: ('purple',    'q=6 mismatch→v2'),
    }

    def _draw_channel_timeseries(self, ax, idx, save_csv=True):
        """Draw one channel's quality-coloured time series onto ``ax``.

        When ``save_csv`` is True the per-channel CSV artifact is also exported.
        Returns True if data was plotted, False if the channel had no decoded
        samples.
        """
        series, quality_series = self.get_channel_series(idx)
        if len(series) == 0:
            print(f'No decoded data to plot for channel {idx}.')
            return False

        fs_output = self.output_rate_hz
        t = np.arange(len(series)) / fs_output

        if save_csv:
            csv_path = f'/home/joannas/ueeg-recordings/chip23_shorttogroundnoise{self.reader_label}ch{idx}.csv'
            export_matrix = np.column_stack((t, series, quality_series))
            np.savetxt(
                csv_path,
                export_matrix,
                delimiter=',',
                header=(
                    'time_s,amplitude,quality_code | '
                    'quality mapping: bits[2:0]: 0=no_packet, 1=only_v1(v2_errored), 2=only_v2(v1_errored), '
                    '3=both_match, 5=mismatch_picked_v1, 6=mismatch_picked_v2; '
                    'bit3(0x08)=error_flag (OR\'d into base code)'
                ),
                comments='',
            )
            print(f'Exported decoded time-domain CSV: {csv_path}')

        base_quality_series = quality_series & np.int8(0x07)
        err_mask = (quality_series & np.int8(0x08)) != 0

        for q, (color, label) in self._QUAL_STYLE.items():
            mask = base_quality_series == q
            if not np.any(mask):
                continue
            ax.scatter(t[mask], series[mask], c=color, label=label,
                       s=30, linewidths=0, alpha=0.7, zorder=2 if q == 3 else 3)
        if np.any(err_mask):
            ax.scatter(t[err_mask], series[err_mask], marker='x', c='black',
                       s=30, linewidths=0.8, alpha=0.8, zorder=4, label='error_flag')
        ax.axhline( 300e-6, color='gray', linewidth=0.8, linestyle='--', alpha=0.6, label='±300 µV')
        ax.axhline(-300e-6, color='gray', linewidth=0.8, linestyle='--', alpha=0.6)
        ax.set_title(f'Time series – Channel {idx}')
        ax.set_ylabel('Amplitude (V)')
        ax.legend(loc='upper right', markerscale=3, fontsize=9)
        ax.grid(True, alpha=0.4)
        print(f'ch{idx}: max={max(series)} min={min(series)}')
        return True

    def plot_channel(self, channel_idx=None, matlab_compare_path=None, matlab_var='data', save_csv=True):
        # Accept a single channel or a list/tuple of channels → stacked subplots.
        if isinstance(channel_idx, (list, tuple, set)):
            channels = [self._normalize_channel_index(c, self.channel_to_decode)
                        for c in channel_idx]
        else:
            channels = [self._normalize_channel_index(channel_idx, self.channel_to_decode)]
        if not channels:
            print('No channels requested to plot.')
            return

        # Multi-channel: one stacked subplot per channel, shared time axis.
        # (IQ overlay is single-channel only; it is skipped for multi-channel.)
        if len(channels) > 1:
            fig, axes = plt.subplots(len(channels), 1, sharex=True,
                                     figsize=(14, 3 * len(channels)), squeeze=False)
            axes = axes[:, 0]
            any_drawn = False
            for ax, idx in zip(axes, channels):
                any_drawn |= self._draw_channel_timeseries(ax, idx, save_csv=save_csv)
            if not any_drawn:
                plt.close(fig)
                return
            axes[-1].set_xlabel('Time (s)')
            fig.tight_layout()
            plt.show()
            return

        # Single channel: preserve the optional raw-I/Q subplots.
        idx = channels[0]
        iq_data = getattr(self, '_iq_plot_data', None)
        if iq_data is not None:
            iq_t, iq_i, iq_q, win_lo, win_hi, iq_name = iq_data
            fig, (ax, ax_i, ax_q) = plt.subplots(3, 1, sharex=True,
                                                  figsize=(14, 8),
                                                  gridspec_kw={'height_ratios': [2, 1, 1]})
            ax.set_xlim(win_lo, win_hi)
        else:
            fig, ax = plt.subplots(figsize=(14, 4))
            ax_i = ax_q = None

        if not self._draw_channel_timeseries(ax, idx, save_csv=save_csv):
            plt.close(fig)
            return

        if ax_i is not None:
            ax_i.plot(iq_t, iq_i, linewidth=0.4, color='steelblue')
            ax_i.set_ylabel('I')
            ax_i.set_title(f'Raw I/Q — {iq_name}')
            ax_i.grid(True, linewidth=0.3)
            ax_q.plot(iq_t, iq_q, linewidth=0.4, color='darkorange')
            ax_q.set_ylabel('Q')
            ax_q.set_xlabel('Time (s)')
            ax_q.grid(True, linewidth=0.3)
        else:
            ax.set_xlabel('Time (s)')

        fig.tight_layout()
        plt.show()

    def start_capture(self, duration_seconds=None):
        if self.enable_gcs:
            self._init_gcs_clients()
            self._check_resume_recording()

        # Initialise secondary reader's GCS clients before opening the device
        if self.secondary_reader is not None:
            if self.secondary_reader.enable_gcs:
                self.secondary_reader._init_gcs_clients()
                self.secondary_reader._check_resume_recording()

        self.setup_device()

        self.running = True
        self._rx_running = True
        self.capture_start_time = time.time()

        rx_t = threading.Thread(target=self.rx_thread, daemon=True)
        self._rx_thread_ref = rx_t
        proc_t = threading.Thread(target=self.processing_thread, daemon=True)
        self._proc_thread_ref = proc_t
        watchdog_t = threading.Thread(target=self._watchdog_thread_func, daemon=True)
        trig_t = None

        if self.enable_gcs and self.enable_gcs_trigger:
            trig_t = threading.Thread(target=self._poll_gcs_triggers, daemon=True)

        # Dual-antenna: start secondary reader's processing thread.
        # Trigger start/stop is forwarded from the primary reader's _handle_gcs_trigger_message,
        # so no separate trigger poller is needed for the secondary.
        sec_proc_t = None
        if self.secondary_reader is not None:
            self.secondary_reader.running = True
            sec_proc_t = threading.Thread(target=self.secondary_reader.processing_thread, daemon=True)
            self._secondary_proc_thread = sec_proc_t

        rx_t.start()
        proc_t.start()
        watchdog_t.start()
        if trig_t is not None:
            trig_t.start()
        if sec_proc_t is not None:
            sec_proc_t.start()

        print('Capture started (MATLAB-style timestamp/frame decoder active).')
        if self.secondary_reader is not None:
            print('Dual-antenna mode active: secondary reader decoding antenna 2 stream in parallel.')

        try:
            end_time = None if duration_seconds is None else time.time() + float(duration_seconds)
            while self.running:
                if end_time is not None and time.time() >= end_time:
                    break
                time.sleep(0.25)
        except KeyboardInterrupt:
            print('Stopping capture (KeyboardInterrupt).')
        finally:
            self.running = False
            self._rx_running = False
            self._stop_gcs_recording()
            if self.secondary_reader is not None:
                self.secondary_reader._stop_gcs_recording()
            # Let the rx_thread finish its current buffer and send None to both
            # processing queues via its own finally block — do NOT inject None here,
            # which would cause the primary to stop before the rx_thread's last buffer
            # is enqueued, giving the secondary one extra buffer.
            rx_t.join(timeout=5.0)
            if trig_t is not None:
                trig_t.join(timeout=1.0)
            proc_t.join(timeout=5.0)

            # Shut down secondary reader
            if self.secondary_reader is not None:
                self.secondary_reader.running = False
                if sec_proc_t is not None:
                    sec_proc_t.join(timeout=5.0)

            print('\n--- Antenna 1 ---')
            self.print_stats()
            self.save_raw_frame_log(
                os.path.join(os.path.dirname(__file__), 'raw_frame_log.csv')
            )
            if self.secondary_reader is not None:
                print('\n--- Antenna 2 ---')
                self.secondary_reader.print_stats()

            if self.enable_plotting:
                self.plot_channel(self.plot_channels, save_csv=self.save_plot_csv)
                if self.secondary_reader is not None and self.secondary_reader.enable_plotting:
                    self.secondary_reader.plot_channel(
                        self.secondary_reader.plot_channels,
                        save_csv=self.secondary_reader.save_plot_csv,
                    )
            elif self.enable_gcs:
                series, quality_series = self.get_channel_series(self.channel_to_decode)
                self._upload_series_to_gcs_binary(series, quality_series)

            try:
                if self.device is not None:
                    self.device.close()
            except Exception:
                pass


if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'board_config.json')) as _f:
        _board_cfg = json.load(_f)
    _board = _board_cfg['boards'][_board_cfg['active_board']]

    # In dual_rx_antenna mode the BladeRF is tuned to active_board.frequency_hz (the LOWER of the
    # two uEEG carriers) and both antennas share that single LO. The two uEEGs then separate by I/Q:
    #   primary (ant1)   = RX0 I (real) → higher-freq uEEG, uses dual_rx_primary_board.decode_scale
    #   secondary (ant2) = RX0 Q (imag) → lower-freq  uEEG, uses active_board.decode_scale
    # So active_board governs both the RX LO frequency and ant2's scale; dual_rx_primary_board
    # governs only ant1's scale. If dual_rx_primary_board is null, the primary also uses active_board.
    _dual_primary_key = _board_cfg.get('dual_rx_primary_board')
    _primary_board = (
        _board_cfg['boards'][str(_dual_primary_key)]
        if _board_cfg.get('dual_rx_antenna') and _dual_primary_key is not None
        else _board
    )

    # Shared configuration for both antennas
    _COMMON = dict(
        sample_rate=8e6,
        frequency=_board['frequency_hz'],
        decode_scale=_primary_board['decode_scale'],
        gain_mode='manual',
        gain=40,
        block_resume_after_unclean_exit=_board_cfg.get('block_resume_after_unclean_exit', False),
        counter=False,
        raw=False,
        bandwidth=2e6,
        enable_plotting=True,
        enable_bandpass_filter=False,
        frame_length=250,
        accepted_frame_lengths=(248, 250),
        frame_length_counts={250: 18, 248: 1},
        bits_per_channel=40,
        channel_to_decode=2,
        plot_channels=[1,2,3],
        save_plot_csv=True,
        gcs_bucket="ueegbucket",
        gcs_buffer_size=400,
        gcs_channels=[2, 3],
        gcs_format='binary',
        enable_gcs_trigger=True,
        enable_gcs=True,
        gcs_trigger_topic_id="sdr-commands",
        gcs_trigger_subscription_id="sdr-commands-pi-sub",
    )
    if _board_cfg.get('dual_rx_antenna'):
        _COMMON['sample_rate'] = 32e6  # for dual-antenna, use 32 MHz sample rate
        _COMMON['bandwidth'] = 8e6
    # Antenna 1 — RX0 I channel (device=1). This reader owns the BladeRF device.
    # Set device1_identifier in board_config.json to a serial/instance string when
    # multiple BladeRF devices are connected, e.g. "*:serial=abc123" or "*:instance=0".
    reader = TimeStampBasedReader(
        **_COMMON,
        device=1,
        gcs_blob_name="ada_eyesclosed_ant1.bin",
        bladerf_identifier=_board_cfg.get('device1_identifier'),
    )

    # Antenna 2 — two modes, mutually exclusive:
    #   dual_rx_antenna=true  → RX0 Q (imaginary) stream of the same device; the primary's rx_thread
    #                           feeds it rx_samples[1::4] (shares BladeRF with reader 1)
    #   device2_board != null → independent BladeRF; set device2_settings.bladerf_identifier
    #                           in board_config.json to select which physical device to open.
    _reader2 = None
    if _board_cfg.get('dual_rx_antenna'):
        # Secondary reader uses active_board decode_scale regardless of dual_rx_primary_board.
        reader.secondary_reader = TimeStampBasedReader(
            **{
                **_COMMON,
                'enable_gcs_trigger': False,
                'decode_scale': _board['decode_scale'],
            },
            device=1,
            rx_channel=1,  # vestigial: the secondary never runs its own rx_thread/_extract_output_stream;
                           # the primary feeds it RX0 Q via rx_samples[1::4], so this value is unused.
            gcs_blob_name="ada_eyesclosed_ant2.bin",
            quiet=True,
            reader_label='rx1',
        )
    elif _board_cfg.get('device2_board') is not None:
        _board2 = _board_cfg['boards'][_board_cfg['device2_board']]
        # device2_settings can override any kwarg, including bladerf_identifier
        _dev2_overrides = _board_cfg.get('device2_settings', {})
        _reader2 = TimeStampBasedReader(
            **{
                **_COMMON,
                'enable_gcs_trigger': False,
                'device': 1,
                'frequency': _board2['frequency_hz'],
                'decode_scale': _board2['decode_scale'],
                'gcs_blob_name': 'ada_eyesclosed_ant2.bin',
                'quiet': True,
                'reader_label': 'device2',
                **_dev2_overrides,  # wins over all defaults above, including bladerf_identifier
            }
        )

    if _reader2 is not None:
        _t2 = threading.Thread(target=_reader2.start_capture, kwargs={'duration_seconds': None}, daemon=True)
        _t2.start()

    reader.start_capture(duration_seconds=None)

    if _reader2 is not None:
        _t2.join()
    # reader.decode_from_file(
    #     '/home/joannas/joannacheckalpha.bin',
    #     bin_file_format='matlab_float32_2xn',
    #     matlab_scale=2048.0,
    #     matlab_select='first_row',
    # )
    # reader.debug_packet_window = (9.5,10)
    # reader.decode_from_file(
    #     '/home/joannas/8MHz_datareadout.bin',
    #     bin_file_format='gnuradio_cf32',
    #     matlab_scale=2048.0,
    #     iq_plot_file="/home/joannas/8MHz_antennaraw.bin",
    # )
    # reader.debug_packet_window = None
    # reader.print_stats()
    # reader.plot_channel(3)
