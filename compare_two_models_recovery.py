#!/usr/bin/env python3
"""
compare_two_models_recovery.py

Three-way BER/reception breakdown between two model runs on the SAME
recording/window, correcting for a real measurement bias: a model that
detects MORE packets (higher reception rate) will naturally show a worse
AGGREGATE BER if some of those extra detections are inherently
harder/weaker packets the other model never attempted at all -
collapsing "detects more" and "quality per detection" into one BER
number unfairly penalizes the more-sensitive model for being more
ambitious. Splits into:

  1. Reception rate for each model (fixed-rate denominator - see below).
  2. BER on the INTERSECTION (packets BOTH models detected) - isolates
     "did we hurt decode quality on packets that were already
     detectable" from "we're now attempting harder ones too".
  3. BER on "model A only" / "model B only" recoveries (packets one
     model detected that the other missed entirely) - expected to be
     worse than the intersection's BER, and that's not a regression -
     that's what "recovering marginal packets" means. Reporting this
     separately (not folded into the intersection number) is the whole
     point of this script.

EXPECTED-SAMPLE-COUNT FIX: the packet rate is a FIXED protocol constant,
not something to re-derive per file. Confirmed with the user: for a
window of duration_seconds, the correct expected packet-slot count is
simply duration_seconds * 200 (200Hz), always - not
TimeStampBasedReader.output_rate_hz, which computes an EMPIRICAL average
frame length from that specific file's own observed gaps
(bit_clock_hz*4/(8*avg_frame_length)) and wobbles slightly file-to-file
as a result (it lands near 200Hz with the class's own defaults -
bit_clock_hz=100kHz, nominal frame_length=250 - but "near" is exactly
the "expecting different amounts" problem being fixed here). This script
uses the fixed PACKET_RATE_HZ constant directly, never reader.output_rate_hz.

Alignment between the two models' decoded positions uses each sample's
own v1_word_pos/v2_word_pos converted to real time (word_pos/100000 +
START_TIME - the same anchoring convention documented in
load_snapshot_to_workspace.m's REMINDER, chosen specifically because
idx-based timing drifts as drops accumulate), NOT a naive synthetic
grid - real packets can be slightly clock-skewed from a perfectly
regular 200Hz grid, so matching against each model's own observed
anchors is more robust than assuming one.

Usage:
    python compare_two_models_recovery.py <mat_file_A> <mat_file_B> \
        --duration SECONDS [--labels A B] [--priority v1->v2]
"""
import argparse
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE, DEFAULT_PRBS_34, _tally  # noqa: E402
from read_prbs_binary_ch23 import compute_prbs_ber  # noqa: E402

PACKET_RATE_HZ = 200.0     # fixed protocol constant - see module docstring
WORD_CLOCK_HZ = 100_000.0  # matches the word_pos/100000 + START_TIME convention
                            # used throughout this project
MATCH_TOLERANCE_S = 1.0 / PACKET_RATE_HZ / 2  # half a packet period (2.5ms)


def _effective_time_and_present(v1_word_pos, v2_word_pos, v1_missing, v2_missing, start_time):
    """Per-decoded-index real-world time anchor, preferring whichever
    copy is actually present. `present` is False only where BOTH copies
    are missing (nothing was decoded there at all)."""
    wp = np.where(~v1_missing, v1_word_pos, v2_word_pos).astype(np.float64)
    present = ~(v1_missing & v2_missing)
    t = wp / WORD_CLOCK_HZ + start_time
    return t, present


def _model_data(mat_path, pattern, priority):
    (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf,
     has_low_conf, chunk_edge, v1_word_pos, v2_word_pos, start_time) = decode(mat_path)
    n_bits = BIT_SLICE.stop - BIT_SLICE.start
    rows, _, _, _, _ = compute_prbs_ber(v1_bits, v2_bits, v1_missing, v2_missing,
                                         pattern, BIT_SLICE, copy_priority=priority)
    t, present = _effective_time_and_present(v1_word_pos, v2_word_pos, v1_missing, v2_missing, start_time)
    return dict(reader=reader, rows=rows, n_bits=n_bits, t=t, present=present, N=len(v1_bits))


def _match_mask(t_a, present_a, t_b, present_b, tol):
    """For each present index in A (in order), whether B has a present
    sample within `tol` seconds of it. Returns (a_present_indices,
    matched_bool_array), same length, aligned."""
    order = np.argsort(t_b)
    b_times_sorted = t_b[order]
    b_present_sorted = present_b[order]
    b_times_present = b_times_sorted[b_present_sorted]
    b_times_present = np.sort(b_times_present)

    a_idx = np.flatnonzero(present_a)
    matched = np.zeros(len(a_idx), dtype=bool)
    if len(b_times_present) == 0:
        return a_idx, matched
    for k, i in enumerate(a_idx):
        ta = t_a[i]
        j = np.searchsorted(b_times_present, ta)
        close = False
        if j < len(b_times_present) and abs(b_times_present[j] - ta) <= tol:
            close = True
        if j > 0 and abs(b_times_present[j - 1] - ta) <= tol:
            close = True
        matched[k] = close
    return a_idx, matched


def _reception_rate(present, duration_s):
    expected_n = duration_s * PACKET_RATE_HZ
    return (int(np.sum(present)) / expected_n) if expected_n > 0 else 0.0


def _report_ber(name, rows, n_bits):
    if not rows:
        print(f"  {name:38s} n=0")
        return
    _, _, ber, n_miss = _tally(rows, n_bits, exclude_missing=True)
    print(f"  {name:38s} n={len(rows):5d}  bit BER={ber:.6f} ({100*ber:6.3f}%)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mat_a")
    ap.add_argument("mat_b")
    ap.add_argument("--labels", nargs=2, default=["A", "B"])
    ap.add_argument("--duration", type=float, required=True,
                     help="window duration in seconds - used for the fixed-rate "
                          "(duration * 200Hz) expected-sample-count denominator")
    ap.add_argument("--priority", default="v1->v2",
                     choices=["v1_only", "v2_only", "v1->v2", "v2->v1"])
    args = ap.parse_args()

    label_a, label_b = args.labels
    pattern = DEFAULT_PRBS_34

    da = _model_data(args.mat_a, pattern, args.priority)
    db = _model_data(args.mat_b, pattern, args.priority)

    expected_n = args.duration * PACKET_RATE_HZ
    rate_a = _reception_rate(da["present"], args.duration)
    rate_b = _reception_rate(db["present"], args.duration)

    print(f"expected packet slots ({args.duration:.3f}s x {PACKET_RATE_HZ:.0f}Hz, fixed): {expected_n:.0f}")
    print(f"  {label_a:10s} {int(np.sum(da['present'])):5d} present  reception={100*rate_a:5.1f}%")
    print(f"  {label_b:10s} {int(np.sum(db['present'])):5d} present  reception={100*rate_b:5.1f}%")

    idx_a, a_in_b = _match_mask(da["t"], da["present"], db["t"], db["present"], MATCH_TOLERANCE_S)
    idx_b, b_in_a = _match_mask(db["t"], db["present"], da["t"], da["present"], MATCH_TOLERANCE_S)

    rows_a_by_idx = {r["idx"]: r for r in da["rows"]}
    rows_b_by_idx = {r["idx"]: r for r in db["rows"]}

    a_inter_rows = [rows_a_by_idx[i] for i, m in zip(idx_a, a_in_b) if m and i in rows_a_by_idx]
    a_only_rows = [rows_a_by_idx[i] for i, m in zip(idx_a, a_in_b) if not m and i in rows_a_by_idx]
    b_inter_rows = [rows_b_by_idx[i] for i, m in zip(idx_b, b_in_a) if m and i in rows_b_by_idx]
    b_only_rows = [rows_b_by_idx[i] for i, m in zip(idx_b, b_in_a) if not m and i in rows_b_by_idx]

    print(f"\n--- intersection (both {label_a} and {label_b} detected) ---")
    print(f"  {'intersection size:':38s} {len(a_inter_rows)} (from {label_a}'s side), "
          f"{len(b_inter_rows)} (from {label_b}'s side - should be close, not "
          f"necessarily identical, since matching is nearest-in-time not exact)")
    _report_ber(f"{label_a} BER on intersection", a_inter_rows, da["n_bits"])
    _report_ber(f"{label_b} BER on intersection", b_inter_rows, db["n_bits"])
    print(f"  (if these two are close, the fixes aren't hurting decode quality on "
          f"packets both models already handle)")

    print(f"\n--- {label_a}-only recoveries ({label_b} missed these entirely) ---")
    _report_ber(f"{label_a} BER on {label_a}-only", a_only_rows, da["n_bits"])
    print(f"  (expected to be worse than the intersection BER - these are the "
          f"marginal packets {label_a} is newly attempting, not a regression)")

    print(f"\n--- {label_b}-only recoveries ({label_a} missed these entirely) ---")
    _report_ber(f"{label_b} BER on {label_b}-only", b_only_rows, db["n_bits"])


if __name__ == "__main__":
    main()
