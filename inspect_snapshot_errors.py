#!/usr/bin/env python3
"""
inspect_snapshot_errors.py

Breaks a single snapshot .mat file's PRBS BER down into "packet drop"
(neither copy present at all - the decoder found nothing to sync on) vs
"BER issue" (a packet WAS received/decoded but its payload bits don't
match the known PRBS pattern) - answers "is this a detection problem or a
data-corruption problem" for a specific bad window flagged by
plot_snapshot_variation.py, rather than just reporting an aggregate BER
number that conflates the two.

Usage:
    python inspect_snapshot_errors.py <mat_path> [--pattern-file FILE] [--priority v1|v2]
"""
import argparse
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE  # noqa: E402
from read_prbs_binary_ch23 import compute_prbs_ber, DEFAULT_PRBS_34  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mat_path")
    parser.add_argument("--pattern-file", default=None)
    parser.add_argument("--priority", default="v1", choices=["v1", "v2"],
                         help="Diversity combining direction (v1: prefer v1, fallback v2)")
    args = parser.parse_args()

    if args.pattern_file:
        text = open(args.pattern_file).read().strip().replace("\n", "").replace(" ", "")
        pattern = np.array([int(c) for c in text], dtype=np.uint8)
    else:
        pattern = DEFAULT_PRBS_34.copy()

    reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf, has_low_conf, chunk_edge = decode(args.mat_path)
    N = len(v1_bits)
    n_bits = BIT_SLICE.stop - BIT_SLICE.start

    rows, t_err, t_bits, ber, n_miss = compute_prbs_ber(
        v1_bits, v2_bits, v1_missing, v2_missing, pattern, BIT_SLICE, copy_priority=args.priority)

    n_received = N - n_miss
    received_rows = [r for r in rows if not r["missing"]]
    n_perfect = sum(1 for r in received_rows if r["errors"] == 0)
    n_corrupted = n_received - n_perfect
    corrupted_errors = [r["errors"] for r in received_rows if r["errors"] > 0]

    print(f"{args.mat_path}")
    print(f"  N (sample-slots in this snapshot): {N}")
    print(f"  packet drop (neither copy present, no sync at all): {n_miss} ({100*n_miss/N:.1f}%)")
    print(f"  received (a packet WAS decoded):                    {n_received} ({100*n_received/N:.1f}%)")
    print(f"    - received with 0 bit errors (clean):    {n_perfect} ({100*n_perfect/max(n_received,1):.1f}% of received)")
    print(f"    - received with >0 bit errors (BER hit): {n_corrupted} ({100*n_corrupted/max(n_received,1):.1f}% of received)")
    if corrupted_errors:
        print(f"      error-count distribution among corrupted rows (out of {n_bits} bits/row): "
              f"mean={np.mean(corrupted_errors):.2f} median={np.median(corrupted_errors):.1f} "
              f"min={min(corrupted_errors)} max={max(corrupted_errors)}")
        hist = np.bincount(corrupted_errors, minlength=n_bits + 1)
        for e in range(len(hist)):
            if hist[e]:
                print(f"        {e:>2} bit errors: {hist[e]} row(s)")
    print(f"  overall BER (drops counted as all-bits-wrong, matches compare_prbs_ber.py convention): {ber:.4f}")
    print(f"  BER among received-only rows (excludes drops): "
          f"{sum(r['errors'] for r in received_rows) / (n_received * n_bits) if n_received else 0:.4f}")
    print(f"  packet_sequence_anomaly_count: {reader.packet_sequence_anomaly_count}   "
          f"header_drops: {reader.packet_sequence_header_drops}")


if __name__ == "__main__":
    main()
