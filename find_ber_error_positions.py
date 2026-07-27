#!/usr/bin/env python3
"""
find_ber_error_positions.py

Prints the exact position (index, raw word_pos, real time) of every
sample with a nonzero bit-error count from compute_prbs_ber, for direct
inspection - so a reported BER number can be checked against the actual
recording rather than trusted as an aggregate. Flags whether each error
sample is also chunk-edge/chain-slip (known simulation-harness artifacts
this session already confirmed can distort individual samples - see
project_prbs_chain_slip_at_chunk_boundary.md /
project_gnuradio_usb_drop_resync_gaps.md), so a real error isn't confused
with an already-understood artifact.

Usage:
    python find_ber_error_positions.py <mat_file> [--priority v1_only]
"""
import argparse
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE, DEFAULT_PRBS_34, _find_chain_slips, PATTERN_LEN  # noqa: E402
from read_prbs_binary_ch23 import compute_prbs_ber  # noqa: E402

WORD_CLOCK_HZ = 100_000.0  # word_pos/100000 + START_TIME convention used throughout this project


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mat_file")
    ap.add_argument("--priority", default="v1_only",
                     choices=["v1_only", "v2_only", "v1->v2", "v2->v1"])
    args = ap.parse_args()

    (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf,
     has_low_conf, chunk_edge, v1_word_pos, v2_word_pos, start_time) = decode(args.mat_file)

    pattern = DEFAULT_PRBS_34
    rows, _, _, _, _ = compute_prbs_ber(v1_bits, v2_bits, v1_missing, v2_missing,
                                         pattern, BIT_SLICE, copy_priority=args.priority)

    slip_v1, dom_v1 = _find_chain_slips(rows, PATTERN_LEN)

    word_pos = v1_word_pos if "v1" in args.priority.split("->")[0] or args.priority == "v1_only" else v2_word_pos
    # for v1->v2/v2->v1 fallback priorities, the ACTUAL copy used per-sample
    # can differ - approximate with whichever copy isn't missing there
    if args.priority in ("v1->v2", "v2->v1"):
        word_pos = np.where(~v1_missing, v1_word_pos, v2_word_pos)

    error_rows = [r for r in rows if r["errors"] > 0]
    print(f"{args.mat_file}  priority={args.priority}")
    print(f"{len(error_rows)} samples with bit errors (of {len(rows)} total decoded)\n")

    print(f"{'idx':>5}  {'word_pos':>9}  {'time (s)':>10}  {'errors':>6}  {'chunk_edge':>10}  {'chain_slip':>10}")
    for r in error_rows:
        idx = r["idx"]
        wp = int(word_pos[idx]) if idx < len(word_pos) else -1
        t = wp / WORD_CLOCK_HZ + start_time if wp >= 0 else float("nan")
        ce = bool(chunk_edge[idx]) if idx < len(chunk_edge) else False
        cs = bool(slip_v1[idx]) if idx < len(slip_v1) else False
        print(f"{idx:5d}  {wp:9d}  {t:10.4f}  {r['errors']:6d}  {str(ce):>10}  {str(cs):>10}")


if __name__ == "__main__":
    main()
