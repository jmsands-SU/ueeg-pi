#!/usr/bin/env python3
"""
sweep_floor_threshold.py

Tests candidate absolute/adaptive floor thresholds for peak_detect2 (a
possible new 19th AND term, avg1 > k * packet_average, evaluated on the
same cycle as the existing 18-tap relative-max check - see
block_notes/peak_detect2.md's "if revisited" section) against BOTH
labeled windows at once, to find where (if anywhere) a floor can cut
false positives without costing real peaks - rather than assuming a
clean threshold exists.

For each candidate k, reports:
  - recall: fraction of labeled real peaks that would STILL fire
    raw_pk_flag (avg1 at the real rpf cycle > k * packet_average there)
  - false positives eliminated: fraction of the measured false-positive
    pulses that would now be suppressed (avg1 <= k * packet_average)

Usage:
    python sweep_floor_threshold.py <w1_signal> <w1_labels> <w2_signal> <w2_labels>
"""
import argparse
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0].rsplit("\\", 1)[0])
from score_peak_labels import load_signal_csv, load_labels_csv, score  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("w1_signal")
    ap.add_argument("w1_labels")
    ap.add_argument("w2_signal")
    ap.add_argument("w2_labels")
    ap.add_argument("--tolerance-samples", type=int, default=3)
    args = ap.parse_args()

    windows = []
    for sig_path, lab_path in [(args.w1_signal, args.w1_labels), (args.w2_signal, args.w2_labels)]:
        sig = load_signal_csv(sig_path)
        labels = load_labels_csv(lab_path)
        result = score(sig, labels, args.tolerance_samples)
        real_hits = [r for r in result["per_label"] if r["rpf_hit"]]
        windows.append((sig_path, real_hits, result["false_positives"]))

    print(f"{'k (x packet_average)':>22s}  {'w1 recall':>10s}  {'w1 FP cut':>10s}  {'w2 recall':>10s}  {'w2 FP cut':>10s}")
    for k in [0.5, 0.8, 1.0, 1.2, 1.5, 1.8, 2.0, 2.5, 3.0]:
        row = []
        for _, real_hits, fps in windows:
            n_real = len(real_hits)
            n_survive = sum(1 for r in real_hits if r["avg1_at_rpf"] > k * r["packet_average"])
            recall = n_survive / n_real if n_real else float("nan")
            n_fp = len(fps)
            n_cut = sum(1 for f in fps if f["avg1"] <= k * f["packet_average"])
            fp_cut = n_cut / n_fp if n_fp else float("nan")
            row.append((recall, fp_cut))
        print(f"{k:22.2f}  {row[0][0]*100:9.1f}%  {row[0][1]*100:9.1f}%  {row[1][0]*100:9.1f}%  {row[1][1]*100:9.1f}%")


if __name__ == "__main__":
    main()
