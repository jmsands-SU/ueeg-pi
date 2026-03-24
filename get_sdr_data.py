# main.py - Complete version with all features

import json
import io
import numpy as np
import re
from google.cloud import storage
import traceback
from scipy import signal

LEGACY_ERROR_MASK = 0x0F

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
    """Counts occurrences of each quality flag value and returns a summary."""
    if flags_array is None or len(flags_array) == 0:
        return None
    counts = np.bincount(flags_array.astype(int))
    total_samples = len(flags_array)
    stats = {
        "total_samples": total_samples,
        "missed_packet": int(counts[0]) if len(counts) > 0 else 0,
        "v1_only": int(counts[1]) if len(counts) > 1 else 0,
        "v2_only": int(counts[2]) if len(counts) > 2 else 0,
        "matched": int(counts[3]) if len(counts) > 3 else 0,
        "mismatch_v1": int(counts[5]) if len(counts) > 5 else 0,
        "mismatch_v2": int(counts[6]) if len(counts) > 6 else 0
    }
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

def _apply_lowpass_filter(data_array, fs, corner_hz=30.0, order=4):
    """Applies a zero-phase low-pass filter to the data."""
    if data_array.ndim == 1: data_array = data_array.reshape(-1, 1)
    if len(data_array) <= order * 3:
        print(f"WARNING: Data length ({len(data_array)}) is too short for filter. Returning original data.")
        return data_array
    b, a = signal.butter(order, corner_hz / (0.5 * fs), btype='low', analog=False)
    return signal.filtfilt(b, a, data_array, axis=0)

def get_sdr_data(request):
    headers = {'Access-Control-Allow-Origin': '*'}
    if request.method == 'OPTIONS':
        headers.update({'Access-control-allow-methods': 'GET', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '3600'})
        return ('', 204, headers)

    request_args = request.args
    bucket_name = request_args.get("bucket")
    blob_name = request_args.get("blob")
    apply_lp_filter = request_args.get("apply_lp_filter") == 'true'
    
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
            
            segment_data_np = data_slice['values'].astype(np.float32) if 'values' in data_slice.dtype.names else np.array([])
            if apply_lp_filter and segment_data_np.size > 0:
                print("INFO: Applying 30Hz low-pass filter.")
                segment_data_np = _apply_lowpass_filter(segment_data_np, sample_rate)

            return (json.dumps({"sample_rate": sample_rate, "segment_data": segment_data_np.tolist()}), 200, headers)

        else:
            print("INFO: Entering full download mode.")
            data_blob = bucket.blob(blob_name)
            if not data_blob.exists(): raise FileNotFoundError(f"Data blob not found: {blob_name}")
            
            binary_content = data_blob.download_as_bytes()
            meta_blob = bucket.blob(f"{blob_name}.meta")
            metadata = json.loads(meta_blob.download_as_text()) if meta_blob.exists() else {}
            
            sample_rate = metadata.get("sample_rate_hz", 200.0)
            dtype = _parse_dtype_from_meta(metadata)

            if dtype:
                print("INFO: Detected structured .bin format from parsed metadata.")
                loaded_array = np.frombuffer(binary_content, dtype=dtype)
                data_only_array = loaded_array['values'].astype(np.float32)
                first_channel_flags = loaded_array['quality_packed'] & 0x0F
                clean_mask = (first_channel_flags > 0)
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
                if apply_lp_filter and segment_data_np.size > 0:
                    segment_data_np = _apply_lowpass_filter(segment_data_np, sample_rate)
                return (json.dumps({"sample_rate": sample_rate, "segment_data": segment_data_np.tolist()}), 200, headers)
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
