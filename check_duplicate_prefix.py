"""
Check whether the first 400 samples of a .bin file are duplicated at offset 400.
Reads the .meta JSON sidecar to get the correct dtype (n_channels, etc.).

Usage:
    python3 check_duplicate_prefix.py 0329_5Hz25mVpp.bin
"""

import sys
import json
import numpy as np
from pathlib import Path


def load_meta(bin_path: Path) -> dict:
    meta_path = bin_path.with_suffix(bin_path.suffix + '.meta')
    if not meta_path.exists():
        # try without extension
        meta_path = Path(str(bin_path) + '.meta')
    if not meta_path.exists():
        return {}
    with open(meta_path) as f:
        return json.load(f)


def make_dtype(meta: dict) -> np.dtype:
    n_channels = len(meta.get('gcs_channels', [2, 3]))
    return np.dtype([
        ('values', np.float32, (n_channels,)),
        ('quality_packed', np.uint16),
    ])


def check(bin_path: Path):
    meta = load_meta(bin_path)
    dtype = make_dtype(meta)
    bytes_per_sample = dtype.itemsize
    print(f"dtype: {dtype}  ({bytes_per_sample} bytes/sample)")

    data = np.frombuffer(bin_path.read_bytes(), dtype=dtype)
    n = len(data)
    print(f"Total samples in file: {n}  ({n * bytes_per_sample} bytes)")

    if meta.get('gcs_samples_written'):
        print(f"Metadata reports gcs_samples_written: {meta['gcs_samples_written']}")
        if n != meta['gcs_samples_written']:
            print(f"  *** MISMATCH: file has {n} samples but metadata says {meta['gcs_samples_written']}")

    CHUNK = 400
    if n < CHUNK * 2:
        print(f"File has fewer than {CHUNK * 2} samples — nothing to compare.")
        return

    a = data[:CHUNK]
    b = data[CHUNK:CHUNK * 2]

    val_match = np.array_equal(a['values'], b['values'])
    q_match = np.array_equal(a['quality_packed'], b['quality_packed'])

    print(f"\nFirst {CHUNK} vs samples {CHUNK}–{CHUNK*2-1}:")
    print(f"  values field identical:          {val_match}")
    print(f"  quality_packed field identical:  {q_match}")

    if val_match and q_match:
        print(f"\n*** DUPLICATE PREFIX DETECTED: samples 0–{CHUNK-1} are repeated at {CHUNK}–{CHUNK*2-1}.")
        print(f"    Actual unique data starts at sample {CHUNK}.")
        print(f"    Unique sample count: {n - CHUNK}")
    else:
        # Show per-channel mean difference so we can see how different they are
        diff = np.abs(a['values'].astype(np.float64) - b['values'].astype(np.float64))
        print(f"\n  No exact duplicate found.")
        channels = meta.get('channel_names', [f'ch{i}' for i in range(a['values'].shape[1])])
        for i, ch in enumerate(channels):
            print(f"  mean |diff| {ch}: {diff[:, i].mean():.6f}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 check_duplicate_prefix.py <path/to/file.bin>")
        sys.exit(1)
    check(Path(sys.argv[1]))
