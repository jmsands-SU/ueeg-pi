#!/usr/bin/env python3
"""
calibrate_low_conf_threshold.py

Finds a low_conf_count threshold for "reject this packet copy" using the
PRBS ground-truth recordings, so the signal_loss_rate metric (missing +
flagged-unreliable, see project conversation 2026-07-26) has a real,
calibrated rejection rule instead of an arbitrary cutoff.

low_conf_count is a saturating unsigned 6-bit counter (0-63, see
softCombine_hdl.m's cntType) accumulated per packet copy during soft
decode - already available on real (non-PRBS) recordings, unlike true
bit-level BER which needs a known reference pattern. This script uses the
PRBS files (known ground truth) purely to CALIBRATE the threshold; the
resulting rule is meant to be applied on real recordings afterward.

Only switchtosoftdecode .mat files carry a low_conf_count signal at all
(plainmodel has no bitThreshold_hdl/softCombine_hdl) - so calibration data
is necessarily limited to whatever switchtosoftdecode PRBS captures exist.

Two failure directions to balance when picking a threshold:
  - False negative (not flagged, but actually wrong) - the dangerous case:
    silently-wrong data reported as trustworthy in signal_loss_rate.
  - False positive (flagged, but actually correct) - the safe-but-wasteful
    case: good data thrown away, inflating signal_loss_rate.
A threshold that drives false negatives to ~0 (catch every real error)
while minimizing false positives is the target - missing a real error is
categorically worse for a "how much of my signal can I trust" plot than
discarding some good data alongside it.

Sample-size caveat: real bit-error EVENTS in the calibration data are rare
(a handful of samples across ~3000 total in the current captures) - the
same overfitting caution that applied to the peak-detection calibration
this session applies here. Treat the recommended threshold as a starting
point, not a final answer, until more PRBS captures with real errors are
available.

Usage:
    python calibrate_low_conf_threshold.py <mat_file> [<mat_file> ...]
"""
import argparse
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE, DEFAULT_PRBS_34, _find_chain_slips, PATTERN_LEN  # noqa: E402
from read_prbs_binary_ch23 import compute_prbs_ber  # noqa: E402

CNT_MAX = 63  # cntType = numerictype(0,6,0), softCombine_hdl.m


def _collect(mat_path):
    """Returns (low_conf_values, is_wrong) pooled over v1+v2, present
    samples only, with chunk-edge/chain-slip artifact samples excluded
    (simulation-harness noise, not real link degradation - same exclusion
    convention as compare_prbs_ber.py)."""
    (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf,
     has_low_conf, chunk_edge, v1_word_pos, v2_word_pos, start_time) = decode(mat_path)

    if not has_low_conf:
        print(f"  (skipping {mat_path}: no low_conf_count signal in this .mat)")
        return np.array([], dtype=np.int64), np.array([], dtype=bool)

    pattern = DEFAULT_PRBS_34
    rows_v1, _, _, _, _ = compute_prbs_ber(v1_bits, v2_bits, v1_missing, v2_missing,
                                            pattern, BIT_SLICE, copy_priority="v1_only")
    rows_v2, _, _, _, _ = compute_prbs_ber(v1_bits, v2_bits, v1_missing, v2_missing,
                                            pattern, BIT_SLICE, copy_priority="v2_only")
    slip_v1, _ = _find_chain_slips(rows_v1, PATTERN_LEN)
    slip_v2, _ = _find_chain_slips(rows_v2, PATTERN_LEN)

    vals, wrong = [], []
    for i in range(len(v1_bits)):
        if not v1_missing[i] and not chunk_edge[i] and not slip_v1[i]:
            vals.append(int(v1_low_conf[i]))
            wrong.append(rows_v1[i]["errors"] > 0)
        if not v2_missing[i] and not chunk_edge[i] and not slip_v2[i]:
            vals.append(int(v2_low_conf[i]))
            wrong.append(rows_v2[i]["errors"] > 0)

    return np.array(vals, dtype=np.int64), np.array(wrong, dtype=bool)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mat_files", nargs="+")
    args = ap.parse_args()

    all_vals, all_wrong = [], []
    for mp in args.mat_files:
        print(f"Loading {mp}")
        vals, wrong = _collect(mp)
        all_vals.append(vals)
        all_wrong.append(wrong)
    low_conf = np.concatenate(all_vals) if all_vals else np.array([], dtype=np.int64)
    is_wrong = np.concatenate(all_wrong) if all_wrong else np.array([], dtype=bool)

    n = len(low_conf)
    n_wrong = int(np.sum(is_wrong))
    print(f"\n{n} present (copy, sample) pairs pooled, {n_wrong} actually wrong "
          f"({100*n_wrong/n:.3f}% of present) - chunk-edge/chain-slip samples excluded")
    if n_wrong == 0:
        print("No real bit errors in this calibration set - cannot calibrate a "
              "threshold (any threshold trivially has 0 false negatives). "
              "Add PRBS captures with weaker-link conditions and re-run.")
        return
    if n_wrong < 20:
        print(f"WARNING: only {n_wrong} real-error samples - threshold below is "
              f"fit to very few events, treat as a starting point (see module "
              f"docstring's overfitting caveat), not a final calibrated rule.")

    print(f"\n{'thresh':>6} {'flagged':>8} {'TP':>5} {'FP':>5} {'FN':>5} {'TN':>6} "
          f"{'recall':>7} {'precision':>9} {'FP rate':>8}")
    best_t = None
    for t in range(0, CNT_MAX + 1):
        flagged = low_conf > t
        tp = int(np.sum(flagged & is_wrong))
        fp = int(np.sum(flagged & ~is_wrong))
        fn = int(np.sum(~flagged & is_wrong))
        tn = int(np.sum(~flagged & ~is_wrong))
        recall = tp / (tp + fn) if (tp + fn) else 1.0
        precision = tp / (tp + fp) if (tp + fp) else float("nan")
        fp_rate = fp / (fp + tn) if (fp + tn) else 0.0
        marker = ""
        if fn == 0 and best_t is None:
            best_t = t
            marker = "  <- lowest threshold that catches every real error (fn=0)"
        print(f"{t:6d} {int(np.sum(flagged)):8d} {tp:5d} {fp:5d} {fn:5d} {tn:6d} "
              f"{recall:7.3f} {precision:9.3f} {fp_rate:8.3f}{marker}")

    print()
    if best_t is not None:
        print(f"Recommended threshold: reject if low_conf_count > {best_t} "
              f"(lowest threshold with zero missed real errors in this calibration set)")
    else:
        print(f"No threshold in [0,{CNT_MAX}] caught every real error - even "
              f"low_conf_count > 0 (flag everything with any low-confidence bit) "
              f"still misses some. low_conf_count alone may not be a sufficient "
              f"signal; consider it a partial filter, not a complete one.")


if __name__ == "__main__":
    main()
