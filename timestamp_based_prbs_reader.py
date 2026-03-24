import threading
import queue
import time
import json
from collections import deque
from dataclasses import dataclass
import os
import numpy as np
try:
    import bladerf
    from bladerf import _bladerf
except Exception:
    _bladerf = None

# Set Google Cloud credentials (required for GCS access)
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'ueegproject-aea2731f9c3a.json')
if os.path.exists(CREDENTIALS_FILE):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_FILE
    print(f"✓ GCS credentials loaded from: {CREDENTIALS_FILE}")
else:
    print(f"⚠️  GCS credentials file not found: {CREDENTIALS_FILE}")
    print("   GCS functionality will be disabled unless credentials are set via gcloud auth")


@dataclass
class PRBSPacket:
    packet_num: int
    is_valid: bool
    bits: np.ndarray   # raw bit array, length up to 4*bits_per_channel


class TimeStampBasedPRBSReader:
    """
    PRBS integrity checker for BladeRF.

    The chip PRBS mode replaces all EEG data with a repeating PRBS pattern.
    This reader does NOT split packets into channels. Instead, each detected
    packet is stored as a raw 80-bit (10-byte) payload. Packets are grouped
    in pairs (v1 = packet 0-3, v2 = packet 4-7) so both copies of each bit
    sequence are preserved for bit-level comparison.

    GCS binary format (per sample, 4 samples per group of 8 packets):
        bytes  0-9  : v1 bits packed MSB-first (np.packbits)
        bytes 10-19 : v2 bits packed MSB-first
        Total       : 20 bytes per sample
    """

    PRBS_BYTES_PER_COPY = 10   # 80 bits packed into 10 bytes
    BYTES_PER_SAMPLE = 20      # v1 + v2

    def __init__(
        self,
        sample_rate=8e6,
        frequency=914.5e6,
        gain=25,
        gain_mode='manual',
        counter=False,
        device=1,
        bandwidth=5e6,
        gcs_bucket=None,
        gcs_blob_name=None,
        gcs_buffer_size=400,       # samples (each 20 bytes)
        enable_gcs_trigger=False,
        gcs_trigger_topic_id='sdr-commands',
        gcs_trigger_subscription_id='sdr-commands-pi-sub',
        gcs_trigger_pull_timeout=0.5,
        enable_gcs=False,
        buffer_size=65536,
        frame_length=250,
        accepted_frame_lengths=None,
        bits_per_channel=20,       # 4 * 20 = 80 bits = 10 bytes per packet
    ):
        self.sample_rate = int(sample_rate)
        self.frequency = int(frequency)
        self.gain = gain
        self.gain_mode = gain_mode
        self.is_counter = bool(counter)
        self.device_num = int(device)
        self.bandwidth = int(bandwidth)
        self.buffer_size = int(buffer_size)

        self.enable_gcs = bool(enable_gcs)
        self.gcs_bucket = gcs_bucket
        self.gcs_blob_name = gcs_blob_name
        self.gcs_buffer_size = gcs_buffer_size
        self.enable_gcs_trigger = bool(enable_gcs_trigger)
        self.gcs_trigger_topic_id = gcs_trigger_topic_id
        self.gcs_trigger_subscription_id = gcs_trigger_subscription_id
        self.gcs_trigger_pull_timeout = gcs_trigger_pull_timeout

        self.gcs_client = None
        self.gcs_bucket_obj = None
        self.gcs_subscriber = None
        self.gcs_recording_active = False
        self.gcs_write_buffer = []   # list of bytes objects (20 bytes each)
        self.gcs_chunk_counter = 0
        self.gcs_session_id = None
        self.gcs_temp_name = f"{self.gcs_blob_name}.temp" if self.gcs_blob_name else None
        self.gcs_samples_written = 0
        self._gcs_buffer_lock = threading.RLock()
        self._gcs_trigger_duration = None
        self._gcs_recording_start_time = None

        self.frame_length = int(frame_length)
        if accepted_frame_lengths is None:
            accepted_frame_lengths = (self.frame_length,)
        self.accepted_frame_lengths = tuple(sorted(set(int(v) for v in accepted_frame_lengths)))
        if len(self.accepted_frame_lengths) == 0:
            raise ValueError('accepted_frame_lengths must contain at least one value')
        self.bits_per_channel = int(bits_per_channel)
        self.prbs_bits = 4 * self.bits_per_channel   # total raw bits per packet (e.g. 80)

        if self.prbs_bits != 80:
            print(f"⚠️  prbs_bits={self.prbs_bits} (expected 80). GCS will still write {self.prbs_bits//8} bytes per copy.")

        self.running = False
        self._rx_running = False
        self._sdr_restart_requested = threading.Event()
        self._sdr_restart_log = []
        self._rx_thread_ref = None
        self.sdr_watchdog_window_seconds = 60
        self.sdr_restart_drop_threshold = 0.30
        self.device = None
        self.channel = None
        self.channel2 = None

        self.data_queue = queue.Queue(maxsize=64)
        self.capture_start_time = None
        self.samples_captured = 0

        # Timestamp tracking
        self.gcs_timestamp_log = []
        self.gcs_timestamp_log_interval = 12000
        self._word_timestamps = []

        # Decoder state (reset on each new decode)
        self._decode_buffer = np.array([], dtype=np.uint16)
        self._pending_packets = []
        self._pending_packet_word_positions = []
        self._synced = False
        self._first_group_skipped = False
        self._last_extracted_packet_num = None
        self._last_extracted_frame_abs_word_start = None
        self._extract_lookback_words = np.array([], dtype=np.uint16)
        self._words_processed_total = 0
        self._raw_frame_log = []

        # Stats
        self.decoded_sample_count = 0
        self.packet_sequence_anomaly_count = 0
        self.packet_sequence_header_drops = 0
        self.prefix_overlap_frames_skipped = 0
        self.placeholder_inserts_cross_chunk = 0
        self.placeholder_inserts_intra_chunk = 0
        self.placeholder_inserts_group_builder = 0
        self.gap_estimate_agree_count = 0
        self.gap_estimate_disagree_count = 0
        self.packet_sequence_events = deque(maxlen=2000)

        if self.enable_gcs and not self.enable_gcs_trigger:
            self.enable_gcs_trigger = True
            print('⚠️  Trigger-only mode enabled for GCS. Recording will start only after a trigger message.')

    # -------------------------------------------------------------------------
    # GCS infrastructure (identical to TimeStampBasedReader)
    # -------------------------------------------------------------------------

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

    def _start_gcs_recording(self):
        with self._gcs_buffer_lock:
            self.gcs_recording_active = True
            self.gcs_write_buffer = []
            self.gcs_chunk_counter = 0
            self.gcs_session_id = time.strftime('%Y%m%d_%H%M%S')
            self.gcs_samples_written = 0
            self._gcs_recording_start_time = time.time()
        self.gcs_temp_name = f"{self.gcs_blob_name}.temp" if self.gcs_blob_name else None
        print(f'GCS recording started (session={self.gcs_session_id}, blob={self.gcs_blob_name}).')

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
            print('GCS recording stopped.')

    def _handle_gcs_trigger_message(self, payload: str):
        text = (payload or '').strip()
        command = text.lower()
        msg = {}
        try:
            msg = json.loads(text)
            command = str(msg.get('command', msg.get('action', command))).lower()
        except Exception:
            pass
        if 'blob' in msg and msg['blob']:
            self.gcs_blob_name = str(msg['blob'])
            print(f'GCS blob name updated to: {self.gcs_blob_name}')
        if 'duration_seconds' in msg:
            try:
                self._gcs_trigger_duration = float(msg['duration_seconds'])
            except Exception:
                pass
        if command in ('start', 'record', 'resume'):
            if not self.gcs_recording_active:
                self._start_gcs_recording()
        elif command in ('stop', 'pause', 'end'):
            self._stop_gcs_recording()

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
                    request={'subscription': subscription_path, 'max_messages': 10},
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
            if (
                self.gcs_recording_active
                and self._gcs_trigger_duration is not None
                and self._gcs_recording_start_time is not None
                and (time.time() - self._gcs_recording_start_time) >= self._gcs_trigger_duration
            ):
                elapsed = time.time() - self._gcs_recording_start_time
                print(f'GCS recording duration ({self._gcs_trigger_duration}s) elapsed (actual={elapsed:.1f}s) — stopping.')
                self._stop_gcs_recording()

    def _append_gcs_samples(self, sample_entries, group_sample_timestamps=None):
        """
        Append up to 4 sample entries to the GCS write buffer.

        Each entry in sample_entries is a 20-byte bytes object:
            bytes  0-9  : v1 bits packed (np.packbits, MSB first)
            bytes 10-19 : v2 bits packed (np.packbits, MSB first)

        group_sample_timestamps: optional np.array of 4 float64 UTC timestamps.
        """
        if not self.enable_gcs:
            return
        should_flush = False
        with self._gcs_buffer_lock:
            if not self.gcs_recording_active:
                return

            # Log timestamps periodically
            if group_sample_timestamps is not None:
                current_total = self.gcs_samples_written + len(self.gcs_write_buffer)
                new_total = current_total + len(sample_entries)
                interval = int(self.gcs_timestamp_log_interval)
                if interval > 0 and (new_total // interval) > (current_total // interval):
                    milestone = (new_total // interval) * interval
                    s_idx = max(0, min(len(sample_entries) - 1, milestone - current_total - 1))
                    ts_val = group_sample_timestamps[s_idx]
                    if not np.isnan(ts_val):
                        self.gcs_timestamp_log.append({
                            'gcs_sample_idx': int(milestone),
                            'sample_timestamp_s': float(ts_val),
                            'system_time_s': time.time(),
                        })

            self.gcs_write_buffer.extend(sample_entries)
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

        # Concatenate all 20-byte entries into one raw blob
        new_bytes = b''.join(buffer_snapshot)

        temp_blob_name = f"{self.gcs_blob_name}.temp.{self.gcs_session_id}.{self.gcs_chunk_counter}"
        temp_blob = self.gcs_bucket_obj.blob(temp_blob_name)
        temp_blob.upload_from_string(new_bytes, content_type='application/octet-stream')

        main_blob = self.gcs_bucket_obj.blob(self.gcs_blob_name)
        try:
            if main_blob.exists():
                main_blob.compose([main_blob, temp_blob])
            else:
                self.gcs_bucket_obj.rename_blob(temp_blob, new_name=self.gcs_blob_name)
                main_blob = self.gcs_bucket_obj.blob(self.gcs_blob_name)
            temp_blob.delete()
        except Exception as exc:
            print(f'GCS compose/append error: {exc}')
            try:
                if temp_blob.exists():
                    temp_blob.delete()
            except Exception:
                pass
            with self._gcs_buffer_lock:
                self.gcs_write_buffer = buffer_snapshot + self.gcs_write_buffer
            return

        n_samples = len(buffer_snapshot)
        self.gcs_samples_written += n_samples
        self.gcs_chunk_counter += 1
        self._write_gcs_metadata()
        print(
            f"GCS append: gs://{self.gcs_bucket}/{self.gcs_blob_name} "
            f"(+{n_samples} samples, total={self.gcs_samples_written} at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})"
        )

    def _write_gcs_metadata(self):
        """Write/update metadata describing the PRBS binary layout."""
        if not self.enable_gcs or self.gcs_bucket_obj is None or not self.gcs_blob_name:
            return
        try:
            meta_blob = self.gcs_bucket_obj.blob(f"{self.gcs_blob_name}.meta")
            prbs_bytes = self.prbs_bits // 8
            metadata = {
                'format': 'prbs_raw_binary',
                'prbs_bits_per_packet': int(self.prbs_bits),
                'bytes_per_copy': int(prbs_bytes),
                'bytes_per_sample': int(self.BYTES_PER_SAMPLE),
                'samples_per_group': 4,
                'bit_packing': 'numpy.packbits MSB-first (big-endian bit order)',
                'layout': {
                    f'bytes_0_{prbs_bytes - 1}': 'v1 PRBS bits packed (packet 0/1/2/3 for samples 0/1/2/3)',
                    f'bytes_{prbs_bytes}_{2 * prbs_bytes - 1}': 'v2 PRBS bits packed (packet 4/5/6/7 for samples 0/1/2/3)',
                },
                'decode_hint': (
                    f'raw = np.frombuffer(blob, dtype=np.uint8).reshape(-1, {self.BYTES_PER_SAMPLE}); '
                    f'v1 = np.unpackbits(raw[:, :{prbs_bytes}], axis=1); '
                    f'v2 = np.unpackbits(raw[:, {prbs_bytes}:], axis=1)'
                ),
                'gcs_samples_written': int(self.gcs_samples_written),
                'gcs_chunk_counter': int(self.gcs_chunk_counter),
                'session_id': self.gcs_session_id,
                'blob_name': self.gcs_blob_name,
                'timestamp_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                'timestamp_log': list(self.gcs_timestamp_log),
                'timestamp_log_interval_samples': int(self.gcs_timestamp_log_interval),
                'sdr_restart_log': list(self._sdr_restart_log),
                'notes': (
                    'PRBS mode: chip replaces EEG data with a repeating PRBS sequence. '
                    'Each sample stores both v1 and v2 copies so individual bit errors can be identified. '
                    'Missing/invalid packets are written as 0x00 bytes. '
                    'timestamp_log provides periodic UTC synchronization checkpoints.'
                ),
            }
            meta_blob.upload_from_string(json.dumps(metadata, indent=2), content_type='application/json')
        except Exception as exc:
            print(f'Error writing GCS metadata: {exc}')

    # -------------------------------------------------------------------------
    # Hardware
    # -------------------------------------------------------------------------

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
            mode = self.gain_mode.lower()
            if mode == 'manual':
                ch.gain_mode = _bladerf.GainMode.Manual
            elif mode == 'fastattack':
                ch.gain_mode = _bladerf.GainMode.FastAttack_AGC
            elif mode == 'slowattack':
                ch.gain_mode = _bladerf.GainMode.SlowAttack_AGC
            elif mode == 'hybrid':
                ch.gain_mode = _bladerf.GainMode.Hybrid_AGC
            else:
                ch.gain_mode = _bladerf.GainMode.Manual
            if mode == 'manual':
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
        print('\n=== BladeRF PRBS Configuration ===')
        print(f'  RX0: {self.channel.sample_rate/1e6:.2f} MSPS @ {self.channel.frequency/1e6:.2f} MHz')
        print(f'  RX1: {self.channel2.sample_rate/1e6:.2f} MSPS @ {self.channel2.frequency/1e6:.2f} MHz')
        print(f'  PRBS bits per packet: {self.prbs_bits}  ({self.prbs_bits // 8} bytes)')
        print('===================================\n')

    @staticmethod
    def _extract_output_stream_static(rx_samples_u16, device_num):
        if device_num == 1:
            return rx_samples_u16[0::4].copy()
        return rx_samples_u16[1::4].copy()

    def rx_thread(self):
        self.channel.enable = True
        self.channel2.enable = True
        rx_buffer = bytearray(self.buffer_size * 4 * 2)
        meta = _bladerf.ffi.new("struct bladerf_metadata *")
        try:
            print("RX thread started. Waiting for samples...")
            while self.running and self._rx_running:
                try:
                    self.device.sync_rx(rx_buffer, self.buffer_size, timeout_ms=3500, meta=meta)
                    buffer_received_time = time.time()
                except _bladerf.TimeoutError:
                    print("⚠️  RX timeout. Retrying...")
                    time.sleep(0.1)
                    continue
                except Exception as e:
                    print(f"⚠️  RX error: {e}. Retrying...")
                    time.sleep(0.1)
                    continue
                actual_count = meta.actual_count
                if actual_count <= 0:
                    continue
                rx_samples = np.frombuffer(rx_buffer, dtype=np.uint16, count=actual_count * 2)
                output_data = self._extract_output_stream_static(rx_samples, self.device_num)
                buffer_duration_s = actual_count / self.sample_rate
                first_word_timestamp = buffer_received_time - buffer_duration_s
                self.samples_captured += len(output_data)
                try:
                    self.data_queue.put((output_data, first_word_timestamp), timeout=0.2)
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

    # -------------------------------------------------------------------------
    # Frame / gap estimation helpers (unchanged from TimeStampBasedReader)
    # -------------------------------------------------------------------------

    def _estimate_frames_in_gap_linear(self, distance_words: int) -> int:
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

    # -------------------------------------------------------------------------
    # Packet extraction  (single stream, not split by channel)
    # -------------------------------------------------------------------------

    def _extract_packets(self, data: np.ndarray):
        """
        Detect packet frames and extract raw PRBS bits.

        Returns ((packets, packet_word_positions), consumed) where:
          packets               - list of PRBSPacket
          packet_word_positions - list of int|None (word offset into consumed region)
          consumed              - number of input words consumed
        """
        data = np.asarray(data, dtype=np.uint16).reshape(-1)
        prefix = self._extract_lookback_words
        prefix_len = int(len(prefix))
        working_data = np.concatenate([prefix, data]) if prefix_len > 0 else data

        data_bit = working_data & 1
        packet_nums_raw = (working_data & ((1 << 4) | (1 << 5) | (1 << 6))) >> 4
        valid_flag = (working_data & (1 << 8)) >> 8

        packet_nums_for_edges = packet_nums_raw.copy()
        valid_words = valid_flag.astype(bool)
        if np.any(valid_words):
            first_valid_idx = int(np.flatnonzero(valid_words)[0])
            if first_valid_idx > 0:
                packet_nums_for_edges[:first_valid_idx] = packet_nums_for_edges[first_valid_idx]

        transitions = np.where(np.diff(packet_nums_for_edges) != 0)[0]
        frame_starts_all = np.concatenate(([0], transitions + 1))
        frame_ends_all = np.concatenate((transitions, [len(packet_nums_for_edges) - 1]))

        valid_start_candidates = frame_starts_all[frame_starts_all >= prefix_len]
        if len(valid_start_candidates) == 0:
            return ({}, []), 0

        last_full_frame_end_idx = int(np.searchsorted(frame_starts_all, valid_start_candidates[-1]))
        process_until = int(frame_starts_all[last_full_frame_end_idx])
        if process_until <= 0:
            return ([], []), 0

        process_until_input = process_until - prefix_len
        if process_until_input <= 0:
            return ([], []), 0

        packet_nums_raw = packet_nums_raw[:process_until]
        packet_nums_for_edges = packet_nums_for_edges[:process_until]
        data_bit = data_bit[:process_until]
        valid_flag = valid_flag[:process_until]

        transitions = np.where(np.diff(packet_nums_for_edges) != 0)[0]
        frame_starts = np.concatenate(([prefix_len], transitions + 1))
        frame_ends = np.concatenate((transitions, [len(packet_nums_for_edges) - 1]))
        frame_lengths = frame_ends - frame_starts + 1

        starts_in_fresh_data = frame_starts >= prefix_len
        self.prefix_overlap_frames_skipped += int(np.sum(~starts_in_fresh_data))
        valid_mask = np.isin(frame_lengths, self.accepted_frame_lengths) & starts_in_fresh_data
        valid_frame_starts = frame_starts[valid_mask]

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
            return ([], []), consumed

        packets = []
        packet_word_positions = []
        prev_start = None
        prev_packet_num = self._last_extracted_packet_num
        prev_abs_start = self._last_extracted_frame_abs_word_start

        for start_idx in valid_frame_starts:
            packet_num = int(packet_nums_for_edges[start_idx])
            cur_abs_start = int(self._words_processed_total + int(start_idx) - prefix_len)

            # Cross-chunk gap
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
                    _ts = self._words_processed_total / self.sample_rate
                    print(
                        f"⚠️  Cross-chunk gap @ t={_ts:.3f}s: prev={prev_packet_num}, "
                        f"first_fresh={packet_num}, inserting={missing}"
                    )
                    last_num = prev_packet_num
                    for _ in range(missing):
                        last_num = (last_num + 1) % 8
                        packets.append(PRBSPacket(packet_num=last_num, is_valid=False, bits=np.array([], dtype=np.uint8)))
                        packet_word_positions.append(None)
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
                    self.packet_sequence_events.append({
                        'prev_packet': int(prev_packet_num),
                        'observed_packet': int(packet_num),
                        'expected_next': int(expected_next),
                        'expected_at_current': int(expected_at_current),
                        'distance_words': int(distance),
                        'expected_frames_in_gap': int(expected_frames_in_gap),
                        'start_idx': int(start_idx),
                        'timestamp_s': (self._words_processed_total + int(start_idx) - prefix_len) / self.sample_rate,
                    })
                    if expected_frames_in_gap == 1 and expected_next is not None and packet_num != expected_next:
                        self.packet_sequence_header_drops += 1
                        continue

                missing = max(missing_from_distance, missing_from_numbers)
                if missing > 0 and prev_packet_num is not None:
                    last_num = prev_packet_num
                    for _ in range(missing):
                        last_num = (last_num + 1) % 8
                        packets.append(PRBSPacket(packet_num=last_num, is_valid=False, bits=np.array([], dtype=np.uint8)))
                        packet_word_positions.append(None)
                    self.placeholder_inserts_intra_chunk += int(missing)
                    if missing_from_distance == missing_from_numbers:
                        self.gap_estimate_agree_count += 1
                    else:
                        self.gap_estimate_disagree_count += 1

            # Extract all PRBS bits for this packet
            prbs_start = start_idx - 2
            prbs_end = start_idx + self.prbs_bits
            if prbs_end > len(packet_nums_raw):
                packets.append(PRBSPacket(packet_num=packet_num, is_valid=False, bits=np.array([], dtype=np.uint8)))
                packet_word_positions.append(start_idx - prefix_len)
            else:
                bits_block = data_bit[prbs_start:prbs_end]
                valid_block = valid_flag[prbs_start:prbs_end]
                selected_bits = bits_block[valid_block == 1].astype(np.uint8)
                packets.append(PRBSPacket(packet_num=packet_num, is_valid=True, bits=selected_bits))
                packet_word_positions.append(start_idx - prefix_len)

            prev_start = start_idx
            prev_packet_num = packet_num
            prev_abs_start = cur_abs_start

        if prev_packet_num is not None:
            self._last_extracted_packet_num = prev_packet_num
            self._last_extracted_frame_abs_word_start = prev_abs_start

        consumed = int(process_until_input)
        if consumed > 0:
            self._extract_lookback_words = data[max(0, consumed - 2):consumed].copy()
        return (packets, packet_word_positions), consumed

    # -------------------------------------------------------------------------
    # Group building
    # -------------------------------------------------------------------------

    def _build_group_with_placeholders(self):
        """Pop 8 packets from _pending_packets, filling gaps with placeholders."""
        group = []
        for expected_num in range(8):
            if self._pending_packets and int(self._pending_packets[0].packet_num) == expected_num:
                group.append(self._pending_packets.pop(0))
            else:
                group.append(PRBSPacket(packet_num=expected_num, is_valid=False, bits=np.array([], dtype=np.uint8)))
                self.placeholder_inserts_group_builder += 1
        return group

    def _decode_packet_groups(self, packets, packet_word_positions=None, word_timestamps=None):
        """
        Extend pending list, then decode complete groups of 8.

        For each group:
          sample s (0-3):
            v1 = group[s]      (packet 0-3)
            v2 = group[s + 4]  (packet 4-7)
          GCS entry: pack(v1.bits[:prbs_bits]) + pack(v2.bits[:prbs_bits])  = 20 bytes
        """
        self._pending_packets.extend(packets)
        if packet_word_positions is not None:
            self._pending_packet_word_positions.extend(packet_word_positions)

        if not self._synced and self._pending_packets:
            self._synced = True

        while self._synced and len(self._pending_packets) >= 8:
            # Discard first group (startup artifact)
            if not self._first_group_skipped:
                self._first_group_skipped = True
                if len(self._pending_packet_word_positions) >= 8:
                    self._pending_packet_word_positions = self._pending_packet_word_positions[8:]
                self._build_group_with_placeholders()
                continue

            # Calculate sample timestamps
            group_sample_timestamps = None
            if word_timestamps is not None and len(self._pending_packet_word_positions) >= 8:
                group_positions = self._pending_packet_word_positions[:8]
                group_packets_peek = self._pending_packets[:8]
                group_sample_timestamps = np.full(4, np.nan, dtype=np.float64)
                packet_0_timestamp = None
                for i, pkt in enumerate(group_packets_peek):
                    if pkt.packet_num == 0 and i < len(group_positions):
                        pos = group_positions[i]
                        if pos is not None and 0 <= pos < len(word_timestamps):
                            packet_0_timestamp = word_timestamps[pos]
                            break
                if packet_0_timestamp is None:
                    for i, pkt in enumerate(group_packets_peek):
                        if pkt.packet_num == 7 and i < len(group_positions):
                            pos = group_positions[i]
                            if pos is not None and 0 <= pos < len(word_timestamps):
                                packet_0_timestamp = word_timestamps[pos] - 0.0175
                                break
                if packet_0_timestamp is not None:
                    group_sample_timestamps[0] = packet_0_timestamp - 0.015
                    group_sample_timestamps[1] = packet_0_timestamp - 0.010
                    group_sample_timestamps[2] = packet_0_timestamp - 0.005
                    group_sample_timestamps[3] = packet_0_timestamp - 0.000

            # Pop word positions
            if len(self._pending_packet_word_positions) >= 8:
                self._pending_packet_word_positions = self._pending_packet_word_positions[8:]

            group = self._build_group_with_placeholders()
            prbs_bytes = self.prbs_bits // 8
            sample_entries = []

            for s in range(4):
                p1 = group[s]
                p2 = group[s + 4]

                if p1.is_valid and len(p1.bits) >= self.prbs_bits:
                    v1_bytes = np.packbits(p1.bits[:self.prbs_bits]).tobytes()
                else:
                    v1_bytes = bytes(prbs_bytes)

                if p2.is_valid and len(p2.bits) >= self.prbs_bits:
                    v2_bytes = np.packbits(p2.bits[:self.prbs_bits]).tobytes()
                else:
                    v2_bytes = bytes(prbs_bytes)

                sample_entries.append(v1_bytes + v2_bytes)
                self.decoded_sample_count += 1

            self._append_gcs_samples(sample_entries, group_sample_timestamps)

    # -------------------------------------------------------------------------
    # Processing thread
    # -------------------------------------------------------------------------

    def processing_thread(self):
        while self.running:
            try:
                item = self.data_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                break

            if isinstance(item, tuple):
                chunk, chunk_first_word_timestamp = item
            else:
                chunk = item
                chunk_first_word_timestamp = None

            if len(chunk) == 0:
                continue

            chunk_timestamps = None
            if chunk_first_word_timestamp is not None:
                chunk_timestamps = chunk_first_word_timestamp + np.arange(len(chunk)) / 100e3

            self._decode_buffer = np.concatenate([self._decode_buffer, chunk])

            if chunk_timestamps is not None:
                self._word_timestamps.extend(chunk_timestamps)

            result, consumed = self._extract_packets(self._decode_buffer)

            if consumed > 0:
                packets, packet_word_positions = result
                consumed_word_timestamps = None
                if self._word_timestamps and len(self._word_timestamps) >= consumed:
                    consumed_word_timestamps = self._word_timestamps[:consumed]
                    self._word_timestamps = self._word_timestamps[consumed:]
                self._decode_packet_groups(packets, packet_word_positions, consumed_word_timestamps)
                self._words_processed_total += consumed
                self._decode_buffer = self._decode_buffer[consumed:]

        # Final flush
        if len(self._decode_buffer) > 0:
            padded = np.concatenate([self._decode_buffer, np.array([0], dtype=np.uint16)])
            result, consumed = self._extract_packets(padded)
            if consumed > 0:
                packets, packet_word_positions = result
                consumed_word_timestamps = None
                if self._word_timestamps and len(self._word_timestamps) >= consumed:
                    consumed_word_timestamps = self._word_timestamps[:consumed]
                self._decode_packet_groups(packets, packet_word_positions, consumed_word_timestamps)
                self._words_processed_total += consumed

    # -------------------------------------------------------------------------
    # State reset
    # -------------------------------------------------------------------------

    def reset_decoder_state(self):
        self._decode_buffer = np.array([], dtype=np.uint16)
        self._pending_packets = []
        self._pending_packet_word_positions = []
        self._synced = False
        self._first_group_skipped = False
        self._last_extracted_packet_num = None
        self._last_extracted_frame_abs_word_start = None
        self._extract_lookback_words = np.array([], dtype=np.uint16)
        self._words_processed_total = 0
        self._raw_frame_log = []
        self._word_timestamps = []
        self.decoded_sample_count = 0
        self.packet_sequence_anomaly_count = 0
        self.packet_sequence_header_drops = 0
        self.prefix_overlap_frames_skipped = 0
        self.placeholder_inserts_cross_chunk = 0
        self.placeholder_inserts_intra_chunk = 0
        self.placeholder_inserts_group_builder = 0
        self.gap_estimate_agree_count = 0
        self.gap_estimate_disagree_count = 0
        self.packet_sequence_events = deque(maxlen=2000)
        self.gcs_write_buffer = []

    # -------------------------------------------------------------------------
    # Capture
    # -------------------------------------------------------------------------

    def start_capture(self, duration_seconds=None):
        if self.enable_gcs:
            self._init_gcs_clients()
        self.setup_device()
        self.running = True
        self._rx_running = True
        self._sdr_restart_requested.clear()
        self.capture_start_time = time.time()

        rx_t = threading.Thread(target=self.rx_thread, daemon=True)
        self._rx_thread_ref = rx_t
        proc_t = threading.Thread(target=self.processing_thread, daemon=True)
        trig_t = None
        if self.enable_gcs and self.enable_gcs_trigger:
            trig_t = threading.Thread(target=self._poll_gcs_triggers, daemon=True)

        rx_t.start()
        proc_t.start()
        if trig_t is not None:
            trig_t.start()

        print('PRBS capture started.')
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
            try:
                self.data_queue.put(None, timeout=0.1)
            except Exception:
                pass
            rx_t.join(timeout=3.0)
            proc_t.join(timeout=3.0)
            if trig_t is not None:
                trig_t.join(timeout=3.0)
            print(f'PRBS capture complete. {self.decoded_sample_count} samples decoded.')

    def print_stats(self):
        print('\n=== PRBS Decoder Statistics ===')
        print(f'  Total samples decoded:       {self.decoded_sample_count}')
        print(f'  Sequence anomalies:          {self.packet_sequence_anomaly_count}')
        print(f'  Header anomaly drops:        {self.packet_sequence_header_drops}')
        print(f'  Prefix-overlap skipped:      {self.prefix_overlap_frames_skipped}')
        print(f'  Placeholder inserts (cross): {self.placeholder_inserts_cross_chunk}')
        print(f'  Placeholder inserts (intra): {self.placeholder_inserts_intra_chunk}')
        print(f'  Placeholder inserts (group): {self.placeholder_inserts_group_builder}')
        print(f'  Gap estimate agree/disagree: {self.gap_estimate_agree_count}/{self.gap_estimate_disagree_count}')
        print(f'  prbs_bits per packet:        {self.prbs_bits}')
        print(f'  GCS samples written:         {self.gcs_samples_written}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    reader = TimeStampBasedPRBSReader(
        sample_rate=8e6,
        frequency=914.5e6,
        gain_mode='slowattack',
        gain=30,
        device=1,
        frame_length=250,
        bits_per_channel=20,    # 4 * 20 = 80 bits = 10 bytes per packet
        gcs_bucket='your-bucket',
        gcs_blob_name='prbs_test/run001',
        enable_gcs=True,
    )
    reader.start_capture()
    reader.print_stats()
