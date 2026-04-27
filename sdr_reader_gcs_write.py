import threading
import queue
import time
import json
from collections import deque, Counter
from dataclasses import dataclass
import os
import numpy as np
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
    error_flag: bool = False

# Set Google Cloud credentials (required for GCS access)
# Path is relative to this script's directory
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'ueegproject-aea2731f9c3a.json')
if os.path.exists(CREDENTIALS_FILE):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_FILE
    print(f"✓ GCS credentials loaded from: {CREDENTIALS_FILE}")
else:
    print(f"⚠️  GCS credentials file not found: {CREDENTIALS_FILE}")
    print("   GCS functionality will be disabled unless credentials are set via gcloud auth")

class TimeStampBasedReader:
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
        mismatch_threshold=0.001,
        decode_scale=1/512*.3,#0.03 / 256 * 2,
        decoded_group_maxlen=5000,
        quiet=False,
        reader_label='ant1',
        rx_channel=0,
    ):
        self.quiet = bool(quiet)  # suppress per-buffer decode warnings (e.g. for secondary reader)
        self.reader_label = str(reader_label)
        self.sample_rate = int(sample_rate)
        self.frequency = int(frequency)
        self.gain = gain
        self.gain_mode = gain_mode
        self.is_counter = bool(counter)
        self.is_raw = bool(raw)
        self.device_num = int(device)
        self.rx_channel = int(rx_channel)
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
        self.gcs_session_id = None
        self.gcs_trigger_thread = None
        self._gcs_trigger_duration = None
        self._gcs_recording_start_time = None
        self._gcs_buffer_lock = threading.RLock()
        self.gcs_temp_name = f"{self.gcs_blob_name}.temp" if self.gcs_blob_name else None
        self.gcs_samples_written = 0
        # Per-channel last-good value for NaN carry-forward (shape (4,) per channel)
        self._gcs_last_good_values = {ch: np.zeros(4, dtype=np.int32) for ch in range(1, 5)}

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

        self.running = False
        self._rx_running = False
        self._sdr_restart_requested = threading.Event()
        self._sdr_restart_log = []  # list of {sample_idx, timestamp_utc, drop_rate, reason}
        self._rx_thread_ref = None
        self.sdr_watchdog_window_seconds = 20
        self.sdr_restart_drop_threshold = 0.50
        self.restart_cooldown_s = 120.0
        self.post_restart_quality_window_s = 5.0
        self.post_restart_quality_threshold = 0.50
        self.post_restart_max_gate_s = 120.0

        self._last_restart_time = None
        self._post_restart_hold = False
        self._post_restart_hold_buffer = []
        self._post_restart_hold_timestamps = []
        self._post_restart_hold_start = None

        self.device = None
        self.channel = None
        self.channel2 = None

        self.data_queue = queue.Queue(maxsize=64)
        self.decoded_group_maxlen = int(decoded_group_maxlen)
        self.decoded_groups_by_channel = {
            ch: deque(maxlen=self.decoded_group_maxlen) for ch in range(1, 5)
        }
        self.decoded_quality_by_channel = {
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
        self.gap_estimate_agree_count = 0
        self.gap_estimate_disagree_count = 0
        self.decoded_groups = self.decoded_groups_by_channel[self.channel_to_decode]
        self.decoded_quality = self.decoded_quality_by_channel[self.channel_to_decode]

        self._decode_buffer = np.array([], dtype=np.uint16)
        self._pending_packets_by_channel = {ch: [] for ch in range(1, 5)}
        self._synced_to_packet0_by_channel = {ch: False for ch in range(1, 5)}
        self._last_extracted_packet_num = None  # persists across chunk calls for cross-chunk gap detection
        self._last_extracted_frame_abs_word_start = None  # absolute word start of last extracted valid frame
        self._extract_lookback_words = np.array([], dtype=np.uint16)  # preserves 2-word context for -2 bit alignment
        self._words_processed_total = 0       # absolute word offset for timestamps
        self._raw_frame_log = []              # list of (abs_word_idx, packet_num, frame_length, passed_valid)
        self._word_timestamps = []  # Stores timestamp for each word in decode buffer
        # Set to (start_s, end_s) to print raw packet bits and decoded ints for every
        # sample whose time (sample_idx / output_rate_hz) falls in the window.
        self.debug_packet_window = None
        self.gcs_timestamp_log = []  # List of {gcs_sample_idx, timestamp_utc, system_time_s}
        self.gcs_timestamp_log_interval = 12000  # Log timestamp every 12000 samples (60 seconds at 200 Hz)
        self._pending_packet_word_positions = {ch: [] for ch in range(1, 5)}  # Track word positions for packets
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
        # Unique per reader so primary and secondary don't overwrite each other.
        safe = (self.gcs_blob_name or 'default').replace('/', '_').replace('.', '_')
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

        if blob_name:
            self.gcs_blob_name = blob_name
            self.gcs_temp_name = f'{blob_name}.temp'
        if duration is not None and start_unix is not None:
            self._gcs_trigger_duration = duration
            self._gcs_recording_start_time = start_unix  # preserve original start for duration tracking

        print(f'Resuming recording from state file (session={session_id}, blob={blob_name}).')
        self._start_gcs_recording()
        # Override session_id so we append to the same blob rather than creating a new session
        with self._gcs_buffer_lock:
            self.gcs_session_id = session_id
        self._sdr_restart_log.append({
            'timestamp_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'sample_idx_at_restart': 0,
            'drop_rate_pct': None,
            'reason': 'process_restart_resume',
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
            self._gcs_last_good_values = {ch: np.zeros(4, dtype=np.int32) for ch in range(1, 5)}
        # Update blob/temp names from trigger message if blob was updated
        self.gcs_temp_name = f"{self.gcs_blob_name}.temp" if self.gcs_blob_name else None
        print(f'GCS recording started (session={self.gcs_session_id}, blob={self.gcs_blob_name}).')
        self._write_recording_state_file()

    def _stop_gcs_recording(self):
        with self._gcs_buffer_lock:
            was_active = bool(self.gcs_recording_active)
            self.gcs_recording_active = False
            self._gcs_recording_start_time = None
        if was_active:
            try:
                self._flush_gcs_buffer(force=True)
            except Exception as exc:
                print(f'GCS flush error during stop: {exc}')
            self._delete_recording_state_file()
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
        if 'duration_seconds' in msg:
            try:
                self._gcs_trigger_duration = float(msg['duration_seconds'])
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
            self._stop_gcs_recording()
            if self.secondary_reader is not None:
                self.secondary_reader._stop_gcs_recording()

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
            except Exception as exc:
                print(f'GCS trigger poll error: {exc}')
                time.sleep(0.5)

            # Duration check: auto-stop if recording has run past the requested duration
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
                last = self._gcs_last_good_values[ch]
                if ch in group_raw_values and q != 0:
                    r = int(group_raw_values[ch][s])
                    last[s] = np.int32(r)
                else:
                    r = int(last[s])  # carry-forward (0 at very start)
                vals_row.append(np.int32(r))
                packed_quality = np.uint16(packed_quality | ((q & 0xF) << (4 * ch_idx)))
            rows_to_add.append((np.asarray(vals_row, dtype=np.int32), packed_quality))

        if self._post_restart_hold:
            # Check if we've been stuck in the gate too long — force a new restart
            if (time.time() - self._post_restart_hold_start) > self.post_restart_max_gate_s:
                print(f'Post-restart quality gate exceeded {self.post_restart_max_gate_s:.0f}s — requesting SDR restart.')
                self._post_restart_hold_buffer.clear()
                self._post_restart_hold_timestamps.clear()
                self._post_restart_hold = False
                self._sdr_restart_requested.set()
                return

            # Buffer rows and timestamps during gate
            self._post_restart_hold_buffer.extend(rows_to_add)
            if group_sample_timestamps is not None:
                self._post_restart_hold_timestamps.append(group_sample_timestamps)

            # Evaluate rolling window once we have enough samples
            window_samples = int(self.post_restart_quality_window_s * self.output_rate_hz)
            total = len(self._post_restart_hold_buffer)
            if total >= window_samples:
                recent = self._post_restart_hold_buffer[-window_samples:]
                good = sum(1 for _, pq in recent if pq != 0)
                ratio = good / window_samples
                if ratio >= self.post_restart_quality_threshold:
                    self._post_restart_hold = False
                    print(
                        f'Post-restart quality gate cleared ({ratio*100:.0f}% good over {window_samples} samples) '
                        f'— flushing {total} buffered samples and resuming GCS writes.'
                    )
                    with self._gcs_buffer_lock:
                        if self.gcs_recording_active:
                            if self._post_restart_hold_timestamps:
                                first_ts = self._post_restart_hold_timestamps[0]
                                self.gcs_timestamp_log.append({
                                    'gcs_sample_idx': int(self.gcs_samples_written + len(self.gcs_write_buffer)),
                                    'sample_timestamp_s': float(first_ts[0]),
                                    'system_time_s': time.time(),
                                    'reason': 'post_restart_quality_gate_cleared',
                                })
                            self.gcs_write_buffer.extend(self._post_restart_hold_buffer)
                    self._post_restart_hold_buffer.clear()
                    self._post_restart_hold_timestamps.clear()
                    self._force_timestamp_after_restart = False  # already logged above
                # If ratio failed, keep accumulating — window re-evaluates next group
            return  # don't fall through to normal write path while gate active

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
            buffer_snapshot = list(self.gcs_write_buffer)
            self.gcs_write_buffer = []

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
        """Fraction of samples in the last watchdog window that have quality 0 (no packet)."""
        window = int(self.sdr_watchdog_window_seconds * self.output_rate_hz)
        quality_deque = self.decoded_quality_by_channel[self.channel_to_decode]
        if len(quality_deque) == 0:
            return 0.0
        recent = list(quality_deque)[-window:]
        q = np.concatenate([np.asarray(g, dtype=np.int8) for g in recent]).reshape(-1)
        if q.size == 0:
            return 0.0
        return float(np.sum(q == 0)) / float(q.size)

    def _watchdog_thread_func(self):
        """Checks drop rate every watchdog_window_seconds; requests SDR restart if too high."""
        while self.running:
            for _ in range(int(self.sdr_watchdog_window_seconds * 4)):
                if not self.running:
                    return
                time.sleep(0.25)
            if not self.running:
                return
            # Skip this cycle if we are still within the post-restart cooldown window
            if self._last_restart_time is not None:
                if (time.time() - self._last_restart_time) < self.restart_cooldown_s:
                    continue
            drop_rate = self._recent_drop_rate()
            if drop_rate > self.sdr_restart_drop_threshold:
                if self.secondary_reader is not None:
                    sec_has_data = any(len(q) > 0 for q in self.secondary_reader.decoded_quality_by_channel.values())
                    if sec_has_data:
                        sec_drop_rate = self.secondary_reader._recent_drop_rate()
                        if sec_drop_rate <= self.sdr_restart_drop_threshold:
                            print(
                                f'⚠️  Watchdog: primary drop rate {drop_rate*100:.1f}% is high but '
                                f'secondary is healthy ({sec_drop_rate*100:.1f}%) — not restarting.'
                            )
                            continue
                print(
                    f'⚠️  Watchdog: drop rate {drop_rate*100:.1f}% > '
                    f'{self.sdr_restart_drop_threshold*100:.0f}% threshold — requesting SDR restart.'
                )
                self._sdr_restart_requested.set()

    def _restart_sdr(self):
        """Stop the rx thread, close the device, wait 10s, then reinitialise and restart."""
        sample_idx = self.decoded_sample_count_by_channel[self.channel_to_decode]
        drop_rate = self._recent_drop_rate()
        ts = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        entry = {
            'timestamp_utc': ts,
            'sample_idx_at_restart': int(sample_idx),
            'drop_rate_pct': round(drop_rate * 100, 2),
            'reason': 'watchdog_drop_threshold',
        }
        self._sdr_restart_log.append(entry)
        print(f'⚠️  SDR restart #{len(self._sdr_restart_log)} @ {ts}  drop_rate={drop_rate*100:.1f}%')

        # Update metadata immediately so the restart is recorded even if we crash later
        self._write_gcs_metadata()

        # Stop rx thread cleanly
        self._rx_running = False
        rx_t = self._rx_thread_ref
        if rx_t is not None:
            rx_t.join(timeout=5.0)

        # Close device
        try:
            if self.channel is not None:
                self.channel.enable = False
            if self.channel2 is not None:
                self.channel2.enable = False
        except Exception:
            pass
        try:
            if self.device is not None:
                self.device.close()
        except Exception:
            pass
        self.device = None

        # Drain queues so processing threads don't stall on old data
        while not self.data_queue.empty():
            try:
                self.data_queue.get_nowait()
            except Exception:
                break
        if self.secondary_reader is not None:
            while not self.secondary_reader.data_queue.empty():
                try:
                    self.secondary_reader.data_queue.get_nowait()
                except Exception:
                    break

        print('SDR stopped. Waiting 10 seconds before restart...')
        for _ in range(40):
            if not self.running:
                return
            time.sleep(0.25)

        if not self.running:
            return

        try:
            self.setup_device()
            self._rx_running = True
            rx_t = threading.Thread(target=self.rx_thread, daemon=True)
            self._rx_thread_ref = rx_t
            rx_t.start()
            self._force_timestamp_after_restart = True
            self._last_restart_time = time.time()
            self._post_restart_hold = True
            self._post_restart_hold_buffer = []
            self._post_restart_hold_timestamps = []
            self._post_restart_hold_start = time.time()
            if self.secondary_reader is not None:
                self.secondary_reader._force_timestamp_after_restart = True
                self.secondary_reader._post_restart_hold = True
                self.secondary_reader._post_restart_hold_buffer = []
                self.secondary_reader._post_restart_hold_timestamps = []
                self.secondary_reader._post_restart_hold_start = time.time()
            print(f'SDR restarted (restart #{len(self._sdr_restart_log)}). Post-restart quality gate active.')
        except Exception as exc:
            print(f'⚠️  SDR restart failed: {exc}')

    def setup_device(self):
        if _bladerf is None:
            raise ImportError('bladerf Python module is not available.')
        self.device = bladerf.BladeRF()

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
        transitions = np.where(np.diff(packet_nums_for_edges) != 0)[0]
        frame_starts = np.concatenate(([prefix_len], transitions+1))
        frame_ends = np.concatenate((transitions, [len(packet_nums_for_edges) - 1]))
        frame_lengths = frame_ends - frame_starts + 1

        starts_in_fresh_data = frame_starts >= prefix_len 
        self.prefix_overlap_frames_skipped += int(np.sum(~starts_in_fresh_data))
        valid_mask = np.isin(frame_lengths, self.accepted_frame_lengths) & starts_in_fresh_data
        valid_frame_starts = frame_starts[valid_mask]

        # Log every detected frame before the valid-length gate
        for _fs, _fl, _fv, _fresh in zip(frame_starts, frame_lengths, valid_mask, starts_in_fresh_data):
            if not bool(_fresh):
                continue
            abs_word = self._words_processed_total + int(_fs) - prefix_len
            pkt_n = int(packet_nums_for_edges[_fs])
            self._raw_frame_log.append((int(abs_word), pkt_n, int(_fl), bool(_fv)))

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

        for start_idx in valid_frame_starts:
            packet_num = int(packet_nums_for_edges[start_idx])
            cur_abs_start = int(self._words_processed_total + int(start_idx) - prefix_len)

            # Cross-chunk gap: first frame of this chunk vs last frame of previous chunk.
            if prev_start is None and prev_packet_num is not None:
                observed_step = (packet_num - prev_packet_num) % 8
                missing_from_numbers = max(0, observed_step - 1)
                missing_from_distance = 0
                if prev_abs_start is not None:
                    distance_words = int(cur_abs_start - int(prev_abs_start))
                    expected_frames_in_gap = self._estimate_frames_in_gap_linear(distance_words)
                    missing_from_distance = max(0, expected_frames_in_gap - 1)
                    if missing_from_distance == missing_from_numbers:
                        self.gap_estimate_agree_count += 1
                    else:
                        self.gap_estimate_disagree_count += 1
                missing = max(missing_from_numbers, missing_from_distance)
                if missing > 0:
                    if not self.quiet:
                        _ts = self._words_processed_total / self.sample_rate
                        print(
                            f"⚠️  Cross-chunk gap @ t={_ts:.3f}s: prev={prev_packet_num}, "
                            f"first_fresh={packet_num}, missing_num={missing_from_numbers}, "
                            f"missing_dist={missing_from_distance}, inserting={missing}"
                        )
                    last_num = prev_packet_num
                    for _ in range(missing):
                        last_num = (last_num + 1) % 8
                        for ch in range(1, 5):
                            packets_by_channel[ch].append(
                                DecodedPacket(packet_num=last_num, is_valid=False, bits=np.array([], dtype=np.uint8))
                            )
                            packet_word_positions[ch].append(None)
                    self.packet_sequence_anomaly_count += missing
                    self.placeholder_inserts_cross_chunk += int(missing)

            if prev_start is not None:
                distance = start_idx - prev_start
                expected_frames_in_gap = self._estimate_frames_in_gap_linear(distance)
                expected_next = (prev_packet_num + 1) % 8 if prev_packet_num is not None else None
                expected_at_current = None
                observed_step = None
                missing_from_distance = 0
                missing_from_numbers = 0
                if prev_packet_num is not None and expected_frames_in_gap >= 1:
                    expected_at_current = (prev_packet_num + expected_frames_in_gap) % 8
                    observed_step = (packet_num - prev_packet_num) % 8
                    missing_from_distance = max(0, expected_frames_in_gap - 1)
                    if observed_step is not None and observed_step > 0:
                        missing_from_numbers = max(0, observed_step - 1)
                if expected_at_current is not None and packet_num != expected_at_current:
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
                            'timestamp_s': (self._words_processed_total + int(start_idx) - prefix_len) / self.sample_rate,
                        }
                    )
                    # print(
                    #     f"⚠️  Packet sequence anomaly: prev={prev_packet_num}, "
                    #     f"expected_next={expected_next}, observed={packet_num}, "
                    #     f"expected_at_current={expected_at_current}, "
                    #     f"distance={distance} words (~{expected_frames_in_gap} frame(s))"
                    # )

                    # If this frame should be the immediate next packet (1 frame apart)
                    # but packet_num disagrees, treat it as a bad header and discard it.
                    # Keep prev_start/prev_packet_num unchanged so the next frame is compared
                    # against the last known-good packet.
                    if expected_frames_in_gap == 1 and expected_next is not None and packet_num != expected_next:
                        self.packet_sequence_header_drops += 1
                        if not self.quiet:
                            ts_drop = (self._words_processed_total + int(start_idx) - prefix_len) / self.sample_rate
                            print(
                                f"⚠️  Dropping out-of-order observed packet @ t={ts_drop:.3f}s: "
                                f"expected {expected_next}, observed {packet_num}"
                            )
                        continue

                # Insert placeholders using the stronger of distance-based and packet-number-based inference.
                # This catches cases where malformed frame lengths make distance-based inference undercount losses.
                missing = max(missing_from_distance, missing_from_numbers)
                if missing > 0 and prev_packet_num is not None:
                    if missing_from_numbers > missing_from_distance and not self.quiet:
                        print(
                            f"⚠️  Packet-gap undercount by frame distance: "
                            f"distance inferred missing={missing_from_distance}, "
                            f"packet-number inferred missing={missing_from_numbers} "
                            f"(prev={prev_packet_num}, observed={packet_num})"
                        )
                    last_num = prev_packet_num
                    for _ in range(missing):
                        last_num = (last_num + 1) % 8
                        for ch in range(1, 5):
                            packets_by_channel[ch].append(
                                DecodedPacket(packet_num=last_num, is_valid=False, bits=np.array([], dtype=np.uint8))
                            )
                            packet_word_positions[ch].append(None)
                    self.placeholder_inserts_intra_chunk += int(missing)
                    if missing_from_distance == missing_from_numbers:
                        self.gap_estimate_agree_count += 1
                    else:
                        self.gap_estimate_disagree_count += 1

            # Packet-level error flag: any valid word across all 4 channels has error set.
            # valid+error window per ch cancels the -4 offset, giving [start_idx : start_idx + 4*bpc].
            _pkt_err_end = start_idx + 4 * self.bits_per_channel
            if start_idx >= 0 and _pkt_err_end <= len(error_flag_arr):
                _all_valid = valid_flag[start_idx:_pkt_err_end]
                _all_error = error_flag_arr[start_idx:_pkt_err_end]
                pkt_error = bool(np.any(_all_error[_all_valid == 1]))
            else:
                pkt_error = False

            for ch in range(1, 5):
                channel_offset = (ch - 1) * self.bits_per_channel
                ch_start = start_idx + channel_offset -4
                ch_end = ch_start + self.bits_per_channel
                if ch_start < 0 or ch_end+5 > len(packet_nums_raw):
                    packets_by_channel[ch].append(
                        DecodedPacket(packet_num=packet_num, is_valid=False, bits=np.array([], dtype=np.uint8))
                    )
                    packet_word_positions[ch].append(start_idx - prefix_len)
                    continue

                bits_block = data_bit[ch_start:ch_end]
                valid_block = valid_flag[ch_start+4:ch_end+4]
                if sum(valid_block) != 20:
                    print(ch,valid_block, len(valid_block), sum(valid_block))
                    print(bits_block)
                selected_bits = bits_block[valid_block == 1]
                self.valid_flag_bitcount_hist_by_channel[ch][len(selected_bits)] += 1
                packets_by_channel[ch].append(
                    DecodedPacket(packet_num=packet_num, is_valid=True, bits=selected_bits.astype(np.uint8), error_flag=pkt_error)
                )
                packet_word_positions[ch].append(start_idx - prefix_len)

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
        pending = self._pending_packets_by_channel[channel_idx]
        group = []
        for expected_packet_num in range(8):
            if len(pending) > 0 and int(pending[0].packet_num) == expected_packet_num:
                group.append(pending.pop(0))
            else:
                group.append(
                    DecodedPacket(
                        packet_num=expected_packet_num,
                        is_valid=False,
                        bits=np.array([], dtype=np.uint8),
                    )
                )
                self.placeholder_inserts_group_builder += 1
        return group

    def _decode_packet_groups(self, packets_by_channel, packet_word_positions=None, word_timestamps=None):
        # Extend pending packets for each channel (no packet dropping/resync)
        for channel_idx, packets in packets_by_channel.items():
            pending = self._pending_packets_by_channel[channel_idx]
            pending.extend(packets)

            # Also extend word positions if available
            if packet_word_positions is not None:
                positions = packet_word_positions.get(channel_idx, [])
                self._pending_packet_word_positions[channel_idx].extend(positions)

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
                    if len(self._pending_packet_word_positions[channel_idx]) >= 8:
                        self._pending_packet_word_positions[channel_idx] = self._pending_packet_word_positions[channel_idx][8:]
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

                group_positions = self._pending_packet_word_positions[first_channel][:8]
                group_packets = self._pending_packets_by_channel[first_channel][:8]

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
                # Pop the word positions for this channel's group
                if len(self._pending_packet_word_positions[channel_idx]) >= 8:
                    self._pending_packet_word_positions[channel_idx] = self._pending_packet_word_positions[channel_idx][8:]

                group = self._build_group_with_placeholders(channel_idx)

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

                    error_occurred[s] = p1.error_flag or p2.error_flag
                    if p1.error_flag and not p2.error_flag and v2 is not None:
                        v1, r1 = None, None
                    elif p2.error_flag and not p1.error_flag and v1 is not None:
                        v2, r2 = None, None

                    if v1 is not None and v2 is not None:
                        if r1 == r2:
                            values[s] = v1
                            quality[s] = 3
                            raw_ints[s] = r1
                        else:
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
                            # print(f"  only_v2 ch{channel_idx} sample_idx={sample_idx} @ t={sample_idx/self.output_rate_hz:.3f}s: p1 pkt#{p1.packet_num} missing, p2 pkt#{p2.packet_num} valid")
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
                            if total_short <= 20:
                                t = sample_idx / self.output_rate_hz
                                print(
                                    f'⚠️  payload_short ch{channel_idx} sample={sample_idx} t={t:.3f}s '
                                    f'v1(short) pkt#{ev["short_pkt"]} bits={ev["short_bits_len"]} [{ev["short_bits"]}] | '
                                    f'v2(good) pkt#{ev["good_pkt"]} bits={ev["good_bits_len"]}'
                                )
                    else:
                        quality[s] = 0
                        # raw_ints[s] stays 0; will be overwritten by carry-forward in _append_gcs_group
                        # print(f"  no_packet ch{channel_idx} sample_idx={sample_idx} @ t={sample_idx/self.output_rate_hz:.3f}s: p1 pkt#{p1.packet_num} valid={p1.is_valid}, p2 pkt#{p2.packet_num} valid={p2.is_valid}")

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

                self.decoded_groups_by_channel[channel_idx].append(values)
                self.decoded_quality_by_channel[channel_idx].append(quality)
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
            
            result, consumed = self._extract_channel_packets(self._decode_buffer)
            
            if consumed > 0:
                # Unpack packets and positions
                packets_by_channel, packet_word_positions = result
                
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

        # Final flush
        if len(self._decode_buffer) > 0:
            packets, consumed = self._extract_channel_packets(
                np.concatenate([self._decode_buffer, np.array([0], dtype=np.uint16)])
            )
            if consumed > 0:
                packets_by_channel, packet_word_positions = packets
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
        self.gap_estimate_agree_count = 0
        self.gap_estimate_disagree_count = 0
        self.decoded_groups = self.decoded_groups_by_channel[self.channel_to_decode]
        self.decoded_quality = self.decoded_quality_by_channel[self.channel_to_decode]
        self._decode_buffer = np.array([], dtype=np.uint16)
        self._pending_packets_by_channel = {ch: [] for ch in range(1, 5)}
        self._synced_to_packet0_by_channel = {ch: False for ch in range(1, 5)}
        self._last_extracted_packet_num = None  # persists across chunk calls for cross-chunk gap detection
        self._last_extracted_frame_abs_word_start = None  # absolute word start of last extracted valid frame
        self._extract_lookback_words = np.array([], dtype=np.uint16)  # preserves 2-word context for -2 bit alignment
        self._words_processed_total = 0       # absolute word offset for timestamps
        self._raw_frame_log = []              # list of (abs_word_idx, packet_num, frame_length, passed_valid)
        self._pending_packet_word_positions = {ch: [] for ch in range(1, 5)}
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
        _SPIKE_THRESHOLD = 2e-3  # volts — max allowed jump from last qual=3 value
        _PARTIAL_QUALS = (1, 2, 5, 6)
        carry = np.nan
        for i in range(len(series)):
            q = int(quality_series[i])
            if q == 3:
                carry = series[i]
            elif q in _PARTIAL_QUALS:
                v = series[i]
                if np.isnan(v) or (not np.isnan(carry) and abs(v - carry) > _SPIKE_THRESHOLD):
                    series[i] = carry
                # else: plausible value, leave it; carry stays at last qual=3
            else:
                # qual=0 — carry-forward
                series[i] = carry

        # MATLAB parity: subtract mean before visualization/spectral analysis
        if len(series) > 0:
            series = series - np.mean(series)

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
        print(f'  Packet drops (resync off):   {self.resync_drops_by_channel[idx]:6d}')
        print(f'  Sequence anomalies:          {self.packet_sequence_anomaly_count:6d}')
        print(f'  Header anomaly drops:        {self.packet_sequence_header_drops:6d}')
        print(f'  Prefix-overlap frames skipped: {self.prefix_overlap_frames_skipped:4d}')
        print(f'  Placeholder inserts (cross): {self.placeholder_inserts_cross_chunk:6d}')
        print(f'  Placeholder inserts (intra): {self.placeholder_inserts_intra_chunk:6d}')
        print(f'  Placeholder inserts (group): {self.placeholder_inserts_group_builder:6d}')
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

    def plot_channel(self, channel_idx=None, matlab_compare_path=None, matlab_var='data'):
        idx = self._normalize_channel_index(channel_idx, self.channel_to_decode)
        series, quality_series = self.get_channel_series(idx)
        if len(series) == 0:
            print('No decoded data to plot.')
            return

        fs_output = self.output_rate_hz
        t = np.arange(len(series)) / fs_output

        csv_path = f'/home/joannas/bladeRF/hdl/pythonscripts/time_domain_python_artifact_{self.reader_label}_ch{idx}.csv'
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

        QUAL_STYLE = {
            0: ('red',       'q=0 no_packet'),
            1: ('orange',    'q=1 only_v1'),
            2: ('gold',      'q=2 only_v2'),
            3: ('steelblue', 'q=3 both_match'),
            5: ('magenta',   'q=5 mismatch→v1'),
            6: ('purple',    'q=6 mismatch→v2'),
        }

        base_quality_series = quality_series & np.int8(0x07)
        err_mask = (quality_series & np.int8(0x08)) != 0

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

        for q, (color, label) in QUAL_STYLE.items():
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
        print(max(series), min(series))

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
        self._sdr_restart_requested.clear()
        self.capture_start_time = time.time()

        rx_t = threading.Thread(target=self.rx_thread, daemon=True)
        self._rx_thread_ref = rx_t
        proc_t = threading.Thread(target=self.processing_thread, daemon=True)
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
                if self._sdr_restart_requested.is_set():
                    self._sdr_restart_requested.clear()
                    self._restart_sdr()
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
                self.plot_channel(self.channel_to_decode)
                if self.secondary_reader is not None and self.secondary_reader.enable_plotting:
                    self.secondary_reader.plot_channel(self.secondary_reader.channel_to_decode)
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

    # Shared configuration for both antennas
    _COMMON = dict(
        sample_rate=8e6,
        frequency=_board['frequency_hz'],
        decode_scale=_board['decode_scale'],
        gain_mode='slow_attack',
        gain=45,
        counter=False,
        raw=False,
        bandwidth=1e6,
        enable_plotting=True,
        enable_bandpass_filter=False,
        frame_length=250,
        accepted_frame_lengths=(248, 250),
        frame_length_counts={250: 18, 248: 1},
        bits_per_channel=40,
        channel_to_decode=3,
        gcs_bucket="ueegbucket",
        gcs_buffer_size=400,
        gcs_channels=[2, 3],
        gcs_format='binary',
        enable_gcs_trigger=True,
        enable_gcs=True,
        gcs_trigger_topic_id="sdr-commands",
        gcs_trigger_subscription_id="sdr-commands-pi-sub",
    )

    # Antenna 1 — RX0 I channel (device=1). This reader owns the BladeRF device.
    reader = TimeStampBasedReader(
        **_COMMON,
        device=1,
        gcs_blob_name="ada_eyesclosed_ant1.bin",
    )

    # Antenna 2 — secondary reader. Two modes, mutually exclusive:
    #   dual_rx_antenna=true  → RX1 I channel of the same device (new two-channel RBF)
    #   device2_board != null → separate BladeRF device on RX0 I/Q (legacy two-device mode)
    # quiet=True suppresses per-packet decode warnings so a noisy antenna 2 doesn't flood stdout.
    if _board_cfg.get('dual_rx_antenna'):
        reader.secondary_reader = TimeStampBasedReader(
            **{**_COMMON, 'enable_gcs_trigger': False},
            device=1,
            rx_channel=1,
            gcs_blob_name="ada_eyesclosed_ant2.bin",
            quiet=True,
            reader_label='rx1',
        )
    elif _board_cfg.get('device2_board') is not None:
        _board2 = _board_cfg['boards'][_board_cfg['device2_board']]
        reader.secondary_reader = TimeStampBasedReader(
            **{**_COMMON, 'enable_gcs_trigger': False},
            device=2,
            frequency=_board2['frequency_hz'],
            decode_scale=_board2['decode_scale'],
            gcs_blob_name="ada_eyesclosed_ant2.bin",
            quiet=True,
            reader_label='device2',
        )

    reader.start_capture(duration_seconds=None)
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
