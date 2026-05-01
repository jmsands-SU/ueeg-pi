# main.py - Complete version with all features

import json
import io
import numpy as np
import re
from google.cloud import storage
import traceback
from scipy import signal

LEGACY_ERROR_MASK = 0x0F

# Default PRBS-34 pattern used by the EEG chip
_PRBS_PATTERN = np.array([
    0, 1, 1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0,
    0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1,
], dtype=np.uint8)

def _parse_dtype_from_meta(metadata):
    """
    Parses the human-readable dtype dictionary from metadata into a NumPy dtype object.
    """
    meta_dtype_dict = metadata.get('dtype')
    field_order = metadata.get('fields')
    if not meta_dtype_dict or not field_order:
        return None
    try:
        dtype_list = []
        for name in field_order:
            format_str = meta_dtype_dict.get(name)
            if not format_str: continue
            match = re.match(r'(\w+)\[(\d+)\]', format_str)
            if match:
                dtype_list.append((name, match.group(1), (int(match.group(2)),)))
            else:
                dtype_list.append((name, format_str))
        return np.dtype(dtype_list)
    except Exception as e:
        print(f"WARNING: Could not parse dtype from metadata: {e}")
        return None

def calculate_quality_stats(flags_array):
    """Counts occurrences of each quality flag value and returns a summary.
    Bit 3 (0x08) is the error flag; bits 2:0 are the base quality code.
    """
    if flags_array is None or len(flags_array) == 0:
        return None
    arr = flags_array.astype(int)
    counts = np.bincount(arr, minlength=16)
    def c(code): return int(counts[code]) if code < len(counts) else 0
    total_samples = len(arr)
    stats = {
        "total_samples": total_samples,
        # base quality codes (no error flag)
        "missed_packet": c(0),
        "v1_only":       c(1),
        "v2_only":       c(2),
        "matched":       c(3),
        "mismatch_v1":   c(5),
        "mismatch_v2":   c(6),
        # error-flagged variants (bit 3 set: base code | 0x08)
        "error_missed":     c(8),
        "error_v1_only":    c(9),   # v2 had error, v1 used
        "error_v2_only":    c(10),  # v1 had error, v2 used
        "error_matched":    c(11),
        "error_mismatch_v1": c(13),
        "error_mismatch_v2": c(14),
    }
    stats["error_flagged_total"] = sum(stats[k] for k in (
        "error_missed", "error_v1_only", "error_v2_only",
        "error_matched", "error_mismatch_v1", "error_mismatch_v2"
    ))
    return stats

def find_all_clean_runs(mask):
    """Finds contiguous runs of True values in a boolean mask."""
    runs = []; current_start = None
    for i, val in enumerate(mask):
        if val and current_start is None: current_start = i
        elif not val and current_start is not None:
            runs.append((current_start, i)); current_start = None
    if current_start is not None: runs.append((current_start, len(mask)))
    return runs

def _parse_prbs_metadata(metadata):
    """Returns (bytes_per_sample, bytes_per_copy) if this is a PRBS file, else (None, None)."""
    if metadata.get('format') == 'prbs_raw_binary':
        return int(metadata.get('bytes_per_sample', 20)), int(metadata.get('bytes_per_copy', 10))
    return None, None

def _prbs_errors_for_row(bits_row, pattern):
    """Best-alignment error count for a single 1-D uint8 bit array vs pattern."""
    n, p = len(bits_row), len(pattern)
    idx = np.arange(n)
    best = -1
    for offset in range(p):
        m = int(np.sum(bits_row == pattern[(idx + offset) % p]))
        if m > best:
            best = m
    return n - best


def _prbs_ber_single_copy(v_bits, v_valid, pattern, bit_slice):
    """BER using only one copy; missing samples count as all-wrong."""
    n_bits = bit_slice.stop - bit_slice.start
    total_errors = total_bits = 0
    for i in range(len(v_bits)):
        total_bits += n_bits
        if not v_valid[i]:
            total_errors += n_bits
        else:
            total_errors += _prbs_errors_for_row(v_bits[i, bit_slice], pattern)
    ber = total_errors / total_bits if total_bits else 0.0
    return total_errors, total_bits, ber


def _prbs_ber_diversity(v_primary, v_primary_valid, v_secondary, v_secondary_valid, pattern, bit_slice):
    """BER using primary copy, falling back to secondary; both missing = all-wrong."""
    n_bits = bit_slice.stop - bit_slice.start
    total_errors = total_bits = 0
    for i in range(len(v_primary)):
        total_bits += n_bits
        if v_primary_valid[i]:
            total_errors += _prbs_errors_for_row(v_primary[i, bit_slice], pattern)
        elif v_secondary_valid[i]:
            total_errors += _prbs_errors_for_row(v_secondary[i, bit_slice], pattern)
        else:
            total_errors += n_bits
    ber = total_errors / total_bits if total_bits else 0.0
    return total_errors, total_bits, ber


def _analyze_prbs_data(raw_bytes, bytes_per_sample, bytes_per_copy):
    """
    Parse PRBS binary data; returns (errors_per_sample, ber, quality, prbs_stats).

    Layout: v1 (bytes_per_copy) | v2 (bytes_per_copy) | quality byte (1).
    Quality byte: bit 0 = v1 valid, bit 1 = v2 valid.

    prbs_stats contains per-strategy BER for both full 80-bit and CH2+CH3 (bits 20:60).
    """
    raw = np.frombuffer(raw_bytes, dtype=np.uint8).reshape(-1, bytes_per_sample)
    v1 = np.unpackbits(raw[:, :bytes_per_copy], axis=1)
    v2 = np.unpackbits(raw[:, bytes_per_copy:2 * bytes_per_copy], axis=1)

    if bytes_per_sample > 2 * bytes_per_copy:
        quality = raw[:, 2 * bytes_per_copy]
        v1_valid = (quality & 0x01).astype(bool)
        v2_valid = (quality & 0x02).astype(bool)
    else:
        quality = None
        v1_valid = ~np.all(raw[:, :bytes_per_copy] == 0, axis=1)
        v2_valid = ~np.all(raw[:, bytes_per_copy:2 * bytes_per_copy] == 0, axis=1)

    pattern = _PRBS_PATTERN
    N = len(v1)
    n_bits_full = v1.shape[1]
    full_slice = slice(0, n_bits_full)
    ch23_slice = slice(20, 60)

    # Legacy errors_per_sample (v1 preferred, full bits, for backward compat)
    errors_per_sample = np.full(N, -1, dtype=np.int32)
    total_errors = total_bits = 0
    for i in range(N):
        if v1_valid[i]:
            bits = v1[i]
        elif v2_valid[i]:
            bits = v2[i]
        else:
            errors_per_sample[i] = n_bits_full
            total_errors += n_bits_full
            total_bits += n_bits_full
            continue
        err = _prbs_errors_for_row(bits, pattern)
        errors_per_sample[i] = err
        total_errors += err
        total_bits += n_bits_full
    ber = total_errors / total_bits if total_bits > 0 else 0.0

    # Availability counts
    n_v1_miss = int(np.sum(~v1_valid))
    n_v2_miss = int(np.sum(~v2_valid))
    n_both_miss = int(np.sum(~v1_valid & ~v2_valid))
    n_both_pres = int(np.sum(v1_valid & v2_valid))

    def _stats(t_err, t_bits, n_miss):
        return {"errors": int(t_err), "bits": int(t_bits),
                "ber": float(t_err / t_bits) if t_bits else 0.0,
                "missing_samples": int(n_miss)}

    def _strategy_stats(bit_slice):
        e1, b1, _ = _prbs_ber_single_copy(v1, v1_valid, pattern, bit_slice)
        e2, b2, _ = _prbs_ber_single_copy(v2, v2_valid, pattern, bit_slice)
        ed1, bd1, _ = _prbs_ber_diversity(v1, v1_valid, v2, v2_valid, pattern, bit_slice)
        ed2, bd2, _ = _prbs_ber_diversity(v2, v2_valid, v1, v1_valid, pattern, bit_slice)
        n_miss_v1 = int(np.sum(~v1_valid))
        n_miss_v2 = int(np.sum(~v2_valid))
        n_miss_both = int(np.sum(~v1_valid & ~v2_valid))
        return {
            "v1_only":    _stats(e1,       b1,       n_miss_v1),
            "v2_only":    _stats(e2,       b2,       n_miss_v2),
            "div_v1_v2":  _stats(ed1,      bd1,      n_miss_both),
            "div_v2_v1":  _stats(ed2,      bd2,      n_miss_both),
            "combined":   _stats(e1 + e2,  b1 + b2,  -1),
        }

    prbs_stats = {
        "total_samples": N,
        "n_v1_missing": n_v1_miss,
        "n_v2_missing": n_v2_miss,
        "n_both_missing": n_both_miss,
        "n_both_present": n_both_pres,
        "ch23": _strategy_stats(ch23_slice),
        "full": _strategy_stats(full_slice),
    }

    return errors_per_sample, ber, quality.tolist() if quality is not None else None, prbs_stats

def _raw_values_to_float(values_array, metadata):
    """
    Convert a raw 'values' array to float32 volts, applying sample_encoding if present.

    New files (storage_dtype='int32') store the raw 20-bit signed integer; the
    encoding is:  volts = raw_int / fixed_point_divisor * scale_factor
    Old files store float32 volts directly and are returned unchanged.
    """
    enc = metadata.get("sample_encoding", {})
    storage_dtype = enc.get("storage_dtype", metadata.get("dtype", {}).get("values", "float32"))
    if "int32" in str(storage_dtype):
        fixed_point_divisor = enc.get("fixed_point_divisor", 1 << 12)
        scale_factor = enc.get("scale_factor", metadata.get("decode_scale", 1.0))
        return (values_array.astype(np.float64) / fixed_point_divisor * scale_factor).astype(np.float32)
    return values_array.astype(np.float32)


def _correct_spike_samples(values_np, quality_packed_np, correct_receiver=True,
                           correct_transmitter=True, spike_thresh_multiplier=10):
    """
    Linearly interpolate single-sample amplitude spikes.

    A sample is a spike on channel c if the jump in and the jump back both exceed
    spike_thresh_multiplier × median-absolute-diff, in opposite directions.

    correct_receiver   — fix spikes where channel quality != 3 (dropped/mismatched packet)
    correct_transmitter — fix spikes where channel quality == 3 (ADC glitch, both copies agreed)

    values_np:         (N, n_channels) float array
    quality_packed_np: (N,) uint16/uint32 array; channel ci quality = (qp >> 4*ci) & 0xF

    Returns a corrected copy of values_np and the number of corrections made.
    """
    values = np.array(values_np, dtype=np.float64)
    N, n_ch = values.shape

    diff = np.diff(values, axis=0)                  # (N-1, n_ch)
    mad = np.median(np.abs(diff), axis=0)
    thresh = spike_thresh_multiplier * mad

    n_corrected = 0
    for ci in range(n_ch):
        if thresh[ci] == 0:
            continue
        for i in range(1, N - 1):
            jump_in   = diff[i - 1, ci]
            jump_back = diff[i,     ci]
            if not (abs(jump_in)   > thresh[ci] and
                    abs(jump_back) > thresh[ci] and
                    np.sign(jump_in) != np.sign(jump_back)):
                continue
            q = int((quality_packed_np[i] >> (4 * ci)) & 0xF)
            is_receiver    = (q != 3)
            is_transmitter = (q == 3)
            if (is_receiver and correct_receiver) or (is_transmitter and correct_transmitter):
                values[i, ci] = (values[i - 1, ci] + values[i + 1, ci]) / 2.0
                n_corrected += 1

    print(f"INFO: Spike correction: {n_corrected} sample(s) interpolated.")
    return values.astype(np.float32), n_corrected


def _apply_bandpass_filter(data_array, fs, low_hz=None, high_hz=None, order=4):
    """Applies a zero-phase Butterworth filter: bandpass, lowpass, or highpass depending on which corners are given."""
    if data_array.ndim == 1:
        data_array = data_array.reshape(-1, 1)
    if len(data_array) <= order * 3:
        print(f"WARNING: Data length ({len(data_array)}) is too short for filter. Returning original data.")
        return data_array
    nyq = 0.5 * fs
    has_low = low_hz is not None and low_hz > 0
    has_high = high_hz is not None and 0 < high_hz < nyq
    if has_low and has_high:
        b, a = signal.butter(order, [low_hz / nyq, high_hz / nyq], btype='bandpass', analog=False)
    elif has_high:
        b, a = signal.butter(order, high_hz / nyq, btype='low', analog=False)
    elif has_low:
        b, a = signal.butter(order, low_hz / nyq, btype='high', analog=False)
    else:
        return data_array
    return signal.filtfilt(b, a, data_array, axis=0)

def get_sdr_data(request):
    headers = {'Access-Control-Allow-Origin': '*'}
    if request.method == 'OPTIONS':
        headers.update({'Access-control-allow-methods': 'GET', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '3600'})
        return ('', 204, headers)

    request_args = request.args
    bucket_name = request_args.get("bucket")
    blob_name = request_args.get("blob")
    correct_spikes  = request_args.get("correct_spikes") == 'true'
    bp_low_hz_str  = request_args.get("bp_low_hz")
    bp_high_hz_str = request_args.get("bp_high_hz")
    bp_low_hz  = float(bp_low_hz_str)  if bp_low_hz_str  else None
    bp_high_hz = float(bp_high_hz_str) if bp_high_hz_str else None
    # legacy: apply_lp_filter=true → 30 Hz lowpass
    if request_args.get("apply_lp_filter") == 'true' and bp_high_hz is None:
        bp_high_hz = 30.0
    apply_filter = bp_low_hz is not None or bp_high_hz is not None
    
    if not bucket_name or not blob_name:
        return (json.dumps({"error": "Missing 'bucket' or 'blob' parameters."}), 400, headers)

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        last_seconds_str = request_args.get("last_seconds")
        start_time_sec_str = request_args.get("start_time_sec")
        
        if last_seconds_str or start_time_sec_str:
            print("INFO: Entering Time Range Fetch mode.")
            meta_blob = bucket.blob(f"{blob_name}.meta")
            if not meta_blob.exists(): raise FileNotFoundError("Time range fetch requires a .meta file.")

            metadata = json.loads(meta_blob.download_as_text())
            if metadata.get('format') == 'prbs_raw_binary':
                return (json.dumps({"error": "Time-range fetch is not supported for PRBS data. Use start_index/end_index instead."}), 400, headers)
            sample_rate = metadata.get("sample_rate_hz")
            dtype = _parse_dtype_from_meta(metadata)
            if not dtype or not sample_rate: raise ValueError("Could not construct dtype or find sample_rate.")
            itemsize = dtype.itemsize
            
            total_samples = metadata.get("total_samples") or metadata.get("gcs_samples_written")
            data_blob = bucket.blob(blob_name)
            if not total_samples:
                file_size = data_blob.size
                if file_size is None: raise IOError("Could not retrieve file size.")
                total_samples = file_size // itemsize

            start_sample, end_sample = 0, 0
            if last_seconds_str:
                end_sample = total_samples
                start_sample = max(0, total_samples - int(float(last_seconds_str) * sample_rate))
            elif start_time_sec_str:
                start_sample = int(float(start_time_sec_str) * sample_rate)
                end_sample = min(total_samples, int(float(request_args.get("end_time_sec", total_samples / sample_rate)) * sample_rate))

            if start_sample >= end_sample: return (json.dumps({"sample_rate": sample_rate, "segment_data": []}), 200, headers)

            start_byte, end_byte = (start_sample * itemsize), (end_sample * itemsize) - 1
            raw_bytes = data_blob.download_as_bytes(start=start_byte, end=end_byte)
            data_slice = np.frombuffer(raw_bytes, dtype=dtype)
            
            segment_data_np = _raw_values_to_float(data_slice['values'], metadata) if 'values' in data_slice.dtype.names else np.array([])
            quality_packed_slice = data_slice['quality_packed'] if 'quality_packed' in data_slice.dtype.names else None
            if correct_spikes and segment_data_np.size > 0 and quality_packed_slice is not None:
                print("INFO: Applying spike correction.")
                segment_data_np, _ = _correct_spike_samples(segment_data_np, quality_packed_slice)
            if apply_filter and segment_data_np.size > 0:
                print(f"INFO: Applying filter (hp={bp_low_hz} Hz, lp={bp_high_hz} Hz).")
                segment_data_np = _apply_bandpass_filter(segment_data_np, sample_rate, low_hz=bp_low_hz, high_hz=bp_high_hz)

            segment_quality = (quality_packed_slice & 0x0F).tolist() if quality_packed_slice is not None else None
            return (json.dumps({"sample_rate": sample_rate, "segment_data": segment_data_np.tolist(), "segment_quality": segment_quality}), 200, headers)

        else:
            print("INFO: Entering full download mode.")
            data_blob = bucket.get_blob(blob_name)
            if data_blob is None: raise FileNotFoundError(f"Data blob not found: {blob_name}")

            meta_blob = bucket.blob(f"{blob_name}.meta")
            metadata = json.loads(meta_blob.download_as_text()) if meta_blob.exists() else {}

            sample_rate = metadata.get("sample_rate_hz", 200.0)
            dtype = _parse_dtype_from_meta(metadata)

            # Reject downloads that would be too large to render in the browser.
            # Compute duration from dtype + file size; fall back to a raw byte cap.
            MAX_FULL_LOAD_SECONDS = 600  # 10 minutes
            MAX_FULL_LOAD_BYTES   = 20_000_000  # 20 MB fallback when dtype unknown
            file_size = data_blob.size or 0
            if file_size > 0:
                if dtype:
                    approx_duration_sec = (file_size // dtype.itemsize) / sample_rate
                    too_large = approx_duration_sec > MAX_FULL_LOAD_SECONDS
                    size_desc = f"{approx_duration_sec/60:.1f} min, {file_size/1e6:.1f} MB"
                else:
                    too_large = file_size > MAX_FULL_LOAD_BYTES
                    size_desc = f"{file_size/1e6:.1f} MB"
                if too_large:
                    print(f"INFO: Full-load rejected — file too large ({size_desc}).")
                    return (json.dumps({
                        "error": (
                            f"File is too large to load in full ({size_desc}). "
                            f"Use 'Load last N minutes' or the time-range loader to fetch a smaller portion."
                        ),
                        "file_size_mb": round(file_size / 1e6, 2),
                    }), 413, headers)

            binary_content = data_blob.download_as_bytes()

            bytes_per_sample_prbs, bytes_per_copy_prbs = _parse_prbs_metadata(metadata)
            if bytes_per_sample_prbs is not None:
                print("INFO: Detected PRBS raw binary format.")
                total_samples = len(binary_content) // bytes_per_sample_prbs
                start_idx = int(request_args.get("start_index", 0))
                end_idx = int(request_args.get("end_index", total_samples))
                start_byte = start_idx * bytes_per_sample_prbs
                end_byte = end_idx * bytes_per_sample_prbs
                errors_per_sample, ber, quality, prbs_stats = _analyze_prbs_data(
                    binary_content[start_byte:end_byte], bytes_per_sample_prbs, bytes_per_copy_prbs
                )
                return (json.dumps({
                    "format": "prbs_raw_binary",
                    "total_samples": total_samples,
                    "bytes_per_sample": bytes_per_sample_prbs,
                    "start_index": start_idx,
                    "end_index": start_idx + len(errors_per_sample),
                    "ber": ber,
                    "errors_per_sample": errors_per_sample.tolist(),
                    "quality": quality,
                    "prbs_stats": prbs_stats,
                }), 200, headers)

            if dtype:
                print("INFO: Detected structured .bin format from parsed metadata.")
                loaded_array = np.frombuffer(binary_content, dtype=dtype)
                data_only_array = _raw_values_to_float(loaded_array['values'], metadata)
                first_channel_flags = loaded_array['quality_packed'] & 0x0F
                clean_mask = ((first_channel_flags > 0) & (first_channel_flags < 8)) | (first_channel_flags == 11)
                quality_flags_for_stats = first_channel_flags
            else:
                print("INFO: Could not parse modern dtype. Assuming legacy .npy format.")
                loaded_array = np.load(io.BytesIO(binary_content), allow_pickle=True)
                if loaded_array.ndim == 1:
                    data_only_array = loaded_array.reshape(-1, 1)
                    clean_mask = np.ones(len(data_only_array), dtype=bool)
                    quality_flags_for_stats = None
                else:
                    data_only_array = loaded_array[:, :-1]
                    error_array = loaded_array[:, -1]
                    error_core = (error_array.astype(np.uint16) & (LEGACY_ERROR_MASK & ~0x08))
                    clean_mask = (error_core < 3)
                    quality_flags_for_stats = error_array

            if request_args.get("start_index"):
                start_idx, end_idx = int(request_args.get("start_index")), int(request_args.get("end_index"))
                segment_data_np = data_only_array[start_idx:end_idx]
                quality_slice = None
                if dtype and 'quality_packed' in dtype.names:
                    quality_slice = loaded_array['quality_packed'][start_idx:end_idx]
                    if correct_spikes and segment_data_np.size > 0:
                        print("INFO: Applying spike correction.")
                        segment_data_np, _ = _correct_spike_samples(segment_data_np, quality_slice)
                if apply_filter and segment_data_np.size > 0:
                    print(f"INFO: Applying filter (hp={bp_low_hz} Hz, lp={bp_high_hz} Hz).")
                    segment_data_np = _apply_bandpass_filter(segment_data_np, sample_rate, low_hz=bp_low_hz, high_hz=bp_high_hz)
                segment_quality = (quality_slice & 0x0F).tolist() if quality_slice is not None else None
                return (json.dumps({"sample_rate": sample_rate, "segment_data": segment_data_np.tolist(), "segment_quality": segment_quality}), 200, headers)
            else:
                analysis_mode = request_args.get("analysis_mode", "clean_only")
                valid_runs_sorted = []
                if analysis_mode == "all_data":
                    if len(data_only_array) > 0: valid_runs_sorted = [{"start": 0, "end": len(data_only_array), "duration_sec": len(data_only_array) / sample_rate}]
                else:
                    min_samples = int(float(request_args.get("threshold", "2.0")) * sample_rate)
                    all_runs = find_all_clean_runs(clean_mask)
                    valid_runs = [{"start": int(s), "end": int(e), "duration_sec": float((e - s) / sample_rate)} for s, e in all_runs if (e - s) >= min_samples]
                    valid_runs_sorted = sorted(valid_runs, key=lambda r: r['start'])
                quality_stats = calculate_quality_stats(quality_flags_for_stats)
                return (json.dumps({"sample_rate": sample_rate, "available_segments": valid_runs_sorted, "quality_stats": quality_stats}), 200, headers)

    except Exception as e:
        print(f"Error during data processing: {e}\n{traceback.format_exc()}")
        return (json.dumps({"error": str(e)}), 500, headers)
