#!/usr/bin/env python3
"""
sweep_shape_threshold.py

Sweeps a fall_linearity threshold (from compare_peak_shapes.py's shape
features) against both labeled windows, reporting recall (real peaks
kept) and false-positive cut at each level - the shape-based equivalent
of sweep_floor_threshold.py's amplitude sweep, to find a real operating
point rather than assuming one.

Usage:
    python sweep_shape_threshold.py <w1_shapes_csv> <w2_shapes_csv>
    (shapes CSVs are compare_peak_shapes.py's --out-csv output)
"""
import argparse
import csv


def load_shapes(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("w1_shapes_csv")
    ap.add_argument("w2_shapes_csv")
    ap.add_argument("--feature", default="fall_flips", choices=["fall_linearity", "fall_flips", "total_flips"])
    args = ap.parse_args()

    windows = [("window 1", load_shapes(args.w1_shapes_csv)), ("window 2", load_shapes(args.w2_shapes_csv))]

    feature = args.feature
    is_flip_count = feature.endswith("flips")

    print(f"feature: {feature}")
    print(f"{'threshold':>10s}  {'w1 recall':>10s}  {'w1 FP cut':>10s}  {'w2 recall':>10s}  {'w2 FP cut':>10s}")
    thresholds = range(0, 60, 4) if is_flip_count else [0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12]
    for thresh in thresholds:
        row = []
        for name, rows in windows:
            real = [r for r in rows if r["kind"].startswith("real")]
            fps = [r for r in rows if r["kind"] == "false_positive"]
            n_survive = sum(1 for r in real if float(r[feature]) <= thresh)
            n_cut = sum(1 for r in fps if float(r[feature]) > thresh)
            recall = n_survive / len(real) if real else float("nan")
            fp_cut = n_cut / len(fps) if fps else float("nan")
            row.append((recall, fp_cut))
        fmt = "{:10d}" if is_flip_count else "{:10.3f}"
        print(fmt.format(thresh) + f"  {row[0][0]*100:9.1f}%  {row[0][1]*100:9.1f}%  {row[1][0]*100:9.1f}%  {row[1][1]*100:9.1f}%")


if __name__ == "__main__":
    main()
