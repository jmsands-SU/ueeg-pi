#!/usr/bin/env python3
"""
PRBS binary reader for the new 20-byte-per-sample format.

New format (timestamp_based_prbs_reader.py):
    bytes  0-9  : v1 bits packed MSB-first (80 bits: 4 channels × 20 bits each)
    bytes 10-19 : v2 bits packed MSB-first (same layout)
    Missing copy = all 0x00 bytes for that half.

CH2+CH3 analysis uses bits [20:60] (40 bits per copy).

Two BER metrics
---------------
cross_copy
    v1 vs v2 direct bit comparison — no PRBS pattern required.
    Denominator: samples_with_both_present × bit_width.
    This corresponds to the "400 × 80 / 2" effective rate:
      200 Hz × 2 copies × 80 bits ÷ 2 (both copies carry the same information).
    A mismatch at any bit position means at least one copy has an error there.

diversity
    v1-preferred with v2 fallback (or vice-versa).
    Each received sample is aligned to the known PRBS pattern via an
    exhaustive per-sample circular search.
    Denominator: samples_with_at_least_one_copy × bit_width.

Usage:
    python read_prbs_binary_ch23.py capture.bin [--pattern-file pattern.txt]
                                                [--max-samples 1000]
                                                [--show-first 20]
                                                [--csv-out results.csv]
"""

import argparse
import csv
import json
import os
import sys
from typing import List, Optional, Tuple

import numpy as np

BYTES_PER_SAMPLE = 21   # v1 (10) + v2 (10) + quality (1)
BITS_PER_COPY = 80
CH23_SLICE = slice(20, 60)   # bits [20:60] = CH2 (20 bits) + CH3 (20 bits)
CH23_BITS = 40

DEFAULT_PRBS_34 = np.array([
    0, 1, 1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0,
    0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1,
], dtype=np.uint8)


def load_prbs_pattern(pattern_file: Optional[str]) -> Optional[np.ndarray]:
    if pattern_file is None:
        return DEFAULT_PRBS_34.copy()
    try:
        text = open(pattern_file).read().strip().replace("\n", "").replace(" ", "")
        if not text or not all(c in "01" for c in text):
            print("❌ Pattern file must contain only 0/1 characters")
            return None
        return np.array([int(c) for c in text], dtype=np.uint8)
    except FileNotFoundError:
        print(f"❌ Pattern file not found: {pattern_file}")
        return None
    except Exception as e:
        print(f"❌ Failed to read pattern file: {e}")
        return None


def read_samples(
    capture_file: str, max_samples: Optional[int] = None
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Read the 21-byte-per-sample PRBS binary file.

    Layout: v1 bits (10 bytes) | v2 bits (10 bytes) | quality (1 byte)
    Quality byte: bit 0 = v1 valid, bit 1 = v2 valid.

    Returns
    -------
    v1_bits   : (N, 80) uint8 — unpacked bits for v1 copy (0 or 1)
    v2_bits   : (N, 80) uint8 — unpacked bits for v2 copy
    v1_missing: (N,) bool     — True when v1 was absent/invalid at capture time
    v2_missing: (N,) bool     — True when v2 was absent/invalid at capture time
    """
    with open(capture_file, "rb") as f:
        raw = np.frombuffer(f.read(), dtype=np.uint8)
    print("Raw bytes read:", len(raw))
    n = len(raw) // BYTES_PER_SAMPLE
    if max_samples is not None:
        n = min(n, max_samples)
    print(n)
    data = raw[: n * BYTES_PER_SAMPLE].reshape(n, BYTES_PER_SAMPLE)
    v1_packed = data[:, :10]
    v2_packed = data[:, 10:20]
    quality   = data[:, 20]       # bit 0 = v1 valid, bit 1 = v2 valid

    v1_bits = np.unpackbits(v1_packed, axis=1)   # (N, 80)
    v2_bits = np.unpackbits(v2_packed, axis=1)
    v1_missing = (quality & 0x01) == 0            # (N,) bool
    v2_missing = (quality & 0x02) == 0

    return v1_bits, v2_bits, v1_missing, v2_missing


# ---------------------------------------------------------------------------
# BER metric 1 — cross-copy mismatch  (400 × 80 / 2 rate)
# ---------------------------------------------------------------------------

def compute_cross_copy_ber(
    v1_bits: np.ndarray,
    v2_bits: np.ndarray,
    v1_missing: np.ndarray,
    v2_missing: np.ndarray,
    bit_slice: slice,
) -> Tuple[int, int, float, int]:
    """
    Compare v1 vs v2 directly for samples where both copies are present.

    Returns (mismatch_bits, total_bits_compared, ber, n_samples_with_both).
    A mismatch at a bit position means ≥1 error exists in one of the copies.
    """
    both = ~v1_missing & ~v2_missing
    n_both = int(np.sum(both))
    if n_both == 0:
        return 0, 0, 0.0, 0

    n_bits = bit_slice.stop - bit_slice.start
    mm = int(np.sum(v1_bits[both][:, bit_slice] != v2_bits[both][:, bit_slice]))
    total = n_both * n_bits
    return mm, total, mm / total, n_both


# ---------------------------------------------------------------------------
# BER metric 2 — PRBS alignment match
# ---------------------------------------------------------------------------

def best_prbs_match_np(bits: np.ndarray, pattern: np.ndarray) -> Tuple[int, int, int, float]:
    """
    Find the best circular alignment of `pattern` against `bits`.

    Parameters
    ----------
    bits    : 1-D uint8 array of measured bits
    pattern : 1-D uint8 array with the reference PRBS sequence

    Returns (best_offset, best_matches, errors, match_rate).
    """
    n, p = len(bits), len(pattern)
    best_offset, best_matches = 0, -1
    idx = np.arange(n)
    for offset in range(p):
        m = int(np.sum(bits == pattern[(idx + offset) % p]))
        if m > best_matches:
            best_matches, best_offset = m, offset
    errors = n - best_matches
    return best_offset, best_matches, errors, best_matches / n if n else 0.0


def compute_prbs_ber(
    v1_bits: np.ndarray,
    v2_bits: np.ndarray,
    v1_missing: np.ndarray,
    v2_missing: np.ndarray,
    pattern: np.ndarray,
    bit_slice: slice,
    copy_priority: str = "v1",
) -> Tuple[List[dict], int, int, float, int]:
    """
    Per-sample PRBS alignment BER.

    copy_priority
        'v1'      — v1 preferred, fallback to v2 when v1 is absent
        'v2'      — v2 preferred, fallback to v1 when v2 is absent
        'v1_only' — v1 only, no fallback
        'v2_only' — v2 only, no fallback

    Returns (rows, total_errors, total_bits, ber, n_missing).
    Each row is a dict: idx, missing, copy_used, errors, offset, match_rate.
    """
    N = len(v1_bits)
    n_bits = bit_slice.stop - bit_slice.start
    rows: List[dict] = []
    total_errors = total_bits = n_missing = 0

    for i in range(N):
        m1, m2 = bool(v1_missing[i]), bool(v2_missing[i])

        if copy_priority == "v1_only":
            missing = m1
            bits_used = v1_bits[i, bit_slice] if not m1 else None
            copy_used = 1 if not m1 else 0
        elif copy_priority == "v2_only":
            missing = m2
            bits_used = v2_bits[i, bit_slice] if not m2 else None
            copy_used = 2 if not m2 else 0
        elif copy_priority == "v1":
            if not m1:
                bits_used, copy_used, missing = v1_bits[i, bit_slice], 1, False
            elif not m2:
                bits_used, copy_used, missing = v2_bits[i, bit_slice], 2, False
            else:
                bits_used, copy_used, missing = None, 0, True
        else:  # 'v2'
            if not m2:
                bits_used, copy_used, missing = v2_bits[i, bit_slice], 2, False
            elif not m1:
                bits_used, copy_used, missing = v1_bits[i, bit_slice], 1, False
            else:
                bits_used, copy_used, missing = None, 0, True

        if missing:
            n_missing += 1
            total_bits += n_bits
            total_errors += n_bits  # all bits lost
            rows.append({"idx": i, "missing": True, "copy_used": 0,
                         "errors": n_bits, "offset": None, "match_rate": 0.0})
            continue

        offset, _, errors, match_rate = best_prbs_match_np(bits_used, pattern)
        total_bits += n_bits
        total_errors += errors
        rows.append({"idx": i, "missing": False, "copy_used": copy_used,
                     "errors": errors, "offset": offset, "match_rate": match_rate})

    ber = total_errors / total_bits if total_bits else 0.0
    return rows, total_errors, total_bits, ber, n_missing


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read new-format PRBS binary (20 bytes/sample, v1+v2 packed)"
    )
    parser.add_argument("capture_file", help="Path to .bin capture file")
    parser.add_argument("--pattern-file", default=None,
                        help="Text file with PRBS pattern (0/1 chars)")
    parser.add_argument("--max-samples", type=int, default=None,
                        help="Read at most this many samples")
    parser.add_argument("--show-first", type=int, default=20,
                        help="Print this many per-sample rows per scenario")
    parser.add_argument("--csv-out", default=None,
                        help="Write per-sample diversity-BER results to CSV")
    args = parser.parse_args()

    if not os.path.exists(args.capture_file):
        print(f"❌ File not found: {args.capture_file}")
        sys.exit(1)

    pattern = load_prbs_pattern(args.pattern_file)
    if pattern is None:
        sys.exit(1)

    v1, v2, v1_missing, v2_missing = read_samples(args.capture_file, args.max_samples)
    N = len(v1)

    n_v1_miss = int(np.sum(v1_missing))
    n_v2_miss = int(np.sum(v2_missing))
    n_both_miss = int(np.sum(v1_missing & v2_missing))
    n_both_pres = int(np.sum(~v1_missing & ~v2_missing))
    n_either = N - n_both_miss

    # ------------------------------------------------------------------
    # Run all four selection strategies
    # ------------------------------------------------------------------
    scenarios = [
        ("v1_only", "v1_only"),
        ("v2_only", "v2_only"),
        ("v1→v2",   "v1"),
        ("v2→v1",   "v2"),
    ]
    scenario_results = {}
    diversity_rows = None
    for label, priority in scenarios:
        rows, t_err, t_bits, ber, n_miss = compute_prbs_ber(
            v1, v2, v1_missing, v2_missing, pattern, CH23_SLICE,
            copy_priority=priority,
        )
        scenario_results[priority] = (rows, t_err, t_bits, ber, n_miss)
        if priority == "v1":
            diversity_rows = rows

    # ------------------------------------------------------------------
    # Q1: Overall BER — how many bits wrong in CH2+CH3 over all bits received?
    #     Use diversity (v1→v2 fallback) to maximise coverage.
    # ------------------------------------------------------------------
    # Pick the better diversity direction for the headline number
    _, t_err_d1, t_bits_d1, ber_d1, n_miss_d1 = scenario_results["v1"]
    _, t_err_d2, t_bits_d2, ber_d2, n_miss_d2 = scenario_results["v2"]
    if ber_d1 <= ber_d2:
        t_err_div, t_bits_div, ber_div, n_miss_div = t_err_d1, t_bits_d1, ber_d1, n_miss_d1
    else:
        t_err_div, t_bits_div, ber_div, n_miss_div = t_err_d2, t_bits_d2, ber_d2, n_miss_d2
    n_received = N - n_miss_div

    # Combined BER treating v1 and v2 as independent signals
    _, t_err_v1_raw, t_bits_v1_raw, _, _ = scenario_results["v1_only"]
    _, t_err_v2_raw, t_bits_v2_raw, _, _ = scenario_results["v2_only"]
    t_err_combined = t_err_v1_raw + t_err_v2_raw
    t_bits_combined = t_bits_v1_raw + t_bits_v2_raw
    ber_combined = t_err_combined / t_bits_combined if t_bits_combined else 0.0

    print(f"\n=== CH2+CH3 bit error rate  ({args.capture_file}) ===")
    print(f"  Samples:        {N}  ({n_received} with ≥1 copy,  {n_miss_div} fully missing)")
    print(f"  Combined BER    (v1+v2 as independent signals):  {t_err_combined}/{t_bits_combined}  BER = {ber_combined:.6f}  ({ber_combined*100:.4f}%)")
    print(f"  Diversity BER   (best copy per sample):          {t_err_div}/{t_bits_div}  BER = {ber_div:.6f}  ({ber_div*100:.4f}%)")

    # ------------------------------------------------------------------
    # Q2: Does selecting v1 vs v2 help?  Show all strategies vs baseline.
    # ------------------------------------------------------------------
    _, t_err_v1, t_bits_v1, ber_v1, _ = scenario_results["v1_only"]
    _, t_err_v2, t_bits_v2, ber_v2, _ = scenario_results["v2_only"]
    _, t_err_d1, t_bits_d1, ber_d1, _ = scenario_results["v1"]
    _, t_err_d2, t_bits_d2, ber_d2, _ = scenario_results["v2"]

    best_single = min(ber_v1, ber_v2)
    best_single_label = "v1" if ber_v1 <= ber_v2 else "v2"
    best_diversity = min(ber_d1, ber_d2)
    best_diversity_label = "v1→v2" if ber_d1 <= ber_d2 else "v2→v1"

    def _improvement(ber_new, ber_ref):
        if ber_ref == 0:
            return "—"
        return f"{(ber_ref - ber_new) / ber_ref * 100:+.1f}%"

    baseline = max(ber_v1, ber_v2)   # worst single copy = natural baseline
    baseline_label = "v2" if ber_v1 <= ber_v2 else "v1"

    print(f"\n=== Copy-selection strategies  (relative to worst single copy = {baseline_label}) ===")
    print(f"  {'Strategy':<22}  {'errors/bits':>18}  {'BER':>10}  {'vs worst single':>16}")
    print(f"  {'-'*70}")
    for lbl, t_err, t_bits, ber in [
        ("v1 only",         t_err_v1, t_bits_v1, ber_v1),
        ("v2 only",         t_err_v2, t_bits_v2, ber_v2),
        ("v1→v2 diversity", t_err_d1, t_bits_d1, ber_d1),
        ("v2→v1 diversity", t_err_d2, t_bits_d2, ber_d2),
    ]:
        counts = f"{t_err}/{t_bits}"
        print(f"  {lbl:<22}  {counts:>18}  {ber:>10.6f}  {_improvement(ber, baseline):>16}")

    print(f"\n  Best single copy:    {best_single_label}  (BER {best_single:.6f})")
    print(f"  Best diversity:      {best_diversity_label}  (BER {best_diversity:.6f})")
    if best_single > 0:
        gain = (best_single - best_diversity) / best_single * 100
        print(f"  Diversity gain over best single copy: {gain:+.1f}%")

    # Per-sample detail (optional)
    if args.show_first > 0 and diversity_rows is not None:
        received = [r for r in diversity_rows if not r["missing"]]
        show = received[: args.show_first]
        if show:
            print(f"\n  First {len(show)} received samples (v1→v2 diversity):")
            print(f"  {'idx':>5}  {'copy':>4}  {'errors':>9}  {'match%':>8}  {'offset':>6}")
            for r in show:
                print(
                    f"  {r['idx']:5d}  {r['copy_used']:4d}  "
                    f"  {r['errors']:4d}/{CH23_BITS}  {r['match_rate']*100:7.2f}%  {r['offset']:6d}"
                )

    # ------------------------------------------------------------------
    # Optional CSV output (diversity v1-priority results)
    # ------------------------------------------------------------------
    if args.csv_out and diversity_rows is not None:
        try:
            with open(args.csv_out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["idx", "missing", "copy_used", "errors", "offset", "match_rate"],
                )
                writer.writeheader()
                writer.writerows(diversity_rows)
            print(f"\nCSV written: {args.csv_out}")
        except Exception as e:
            print(f"⚠️  CSV write failed: {e}")

    # Print metadata if present
    meta_path = f"{args.capture_file}.meta"
    if os.path.exists(meta_path):
        try:
            meta = json.load(open(meta_path))
            print(f"\nMetadata: format={meta.get('format')}, "
                  f"samples_written={meta.get('gcs_samples_written')}, "
                  f"prbs_bits_per_packet={meta.get('prbs_bits_per_packet')}")
        except Exception:
            pass


if __name__ == "__main__":
    main()
