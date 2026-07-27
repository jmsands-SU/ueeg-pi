#!/usr/bin/env python3
"""
plot_snapshot_variation.py

Per-snapshot variation plot across one or more recording directories (e.g.
the four AGC-setting/walk-speed snapshot surveys from
run_snapshot_survey_model_comparison.m). AVERAGE-row summaries (as printed
by compare_snapshot_reception.py) can be dominated by a couple of bad
segments (dead zones, total-outage windows) - this plots every snapshot's
own value vs time so that's visible directly, and prints MEDIAN alongside
MEAN for the same reason (median is robust to a handful of outlier
windows, mean isn't).

Usage:
    python plot_snapshot_variation.py <base_dir> --recordings R1 R2 R3 R4 --labels A B --prbs
        Expects <base_dir>/<recording>/<label>_s##.mat for each recording
        in --recordings (run_snapshot_survey_model_comparison.m's
        per-recording subdirectory convention). One subplot per recording,
        PRBS-BER (--prbs) or reception% (default) vs snapshot time, both
        labels overlaid.
"""
import argparse
import os
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_snapshot_reception import summarize_snapshot, find_snapshot_series  # noqa: E402
from read_prbs_binary_ch23 import DEFAULT_PRBS_34  # noqa: E402


def _snapshot_times(files, step_s, start_s):
    """run_snapshot_survey_model_comparison.m names files <label>_s##.mat
    with ## a 1-indexed snapshot number at SNAPSHOT_START + (idx-1)*STEP."""
    times = []
    for f in files:
        idx = int(os.path.splitext(f)[0].rsplit("_s", 1)[1])
        times.append(start_s + (idx - 1) * step_s)
    return np.array(times)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("base_dir", help="Directory containing one subdirectory per recording")
    parser.add_argument("--recordings", nargs="+", required=True, help="Subdirectory names under base_dir")
    parser.add_argument("--labels", nargs=2, required=True, help="Two model labels (must match filename prefixes)")
    parser.add_argument("--prbs", action="store_true", help="Plot real PRBS ground-truth BER instead of reception%%")
    parser.add_argument("--pattern-file", default=None)
    parser.add_argument("--step", type=float, default=10.0, help="SNAPSHOT_STEP used when the survey ran (default 10s)")
    parser.add_argument("--start", type=float, default=10.0, help="SNAPSHOT_START used when the survey ran (default 10s)")
    parser.add_argument("--out", default=None, help="Output PNG path (default: snapshot_variation.png next to this script)")
    args = parser.parse_args()

    pattern = None
    if args.prbs:
        if args.pattern_file:
            text = open(args.pattern_file).read().strip().replace("\n", "").replace(" ", "")
            pattern = np.array([int(c) for c in text], dtype=np.uint8)
        else:
            pattern = DEFAULT_PRBS_34.copy()

    label_a, label_b = args.labels
    metric_key = "ber_prbs" if args.prbs else "reception_rate"
    metric_name = "PRBS-BER" if args.prbs else "reception rate"

    n = len(args.recordings)
    ncols = 2
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 4 * nrows), squeeze=False)

    print(f"\n{'='*90}\nPER-SNAPSHOT VARIATION  ({metric_name})\n{'='*90}")
    print(f"{'recording':<22} {'model':<20} {'mean':>8} {'median':>8} {'min':>8} {'max':>8} {'n':>4}")

    for i, rec in enumerate(args.recordings):
        ax = axes[i // ncols][i % ncols]
        rec_dir = os.path.join(args.base_dir, rec)

        for label, color in [(label_a, "tab:blue"), (label_b, "tab:orange")]:
            files = find_snapshot_series(rec_dir, label)
            results = [summarize_snapshot(f, prbs=args.prbs, pattern=pattern) for f in files]
            times = _snapshot_times(files, args.step, args.start)
            values = np.array([r[metric_key] for r in results])

            order = np.argsort(times)
            times, values = times[order], values[order]

            ax.plot(times, values, "o-", color=color, label=label, markersize=5)

            mean_v, median_v = np.mean(values), np.median(values)
            print(f"{rec:<22} {label:<20} {mean_v:>8.4f} {median_v:>8.4f} "
                  f"{np.min(values):>8.4f} {np.max(values):>8.4f} {len(values):>4}")

        ax.set_title(rec)
        ax.set_xlabel("snapshot time (s)")
        ax.set_ylabel(metric_name)
        if args.prbs:
            ax.set_ylim(-0.05, 1.05)
        else:
            ax.set_ylim(-5, 105)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)

    # hide any unused subplot slots
    for j in range(n, nrows * ncols):
        axes[j // ncols][j % ncols].set_visible(False)

    fig.tight_layout()
    out_png = args.out or os.path.join(_HERE, "snapshot_variation.png")
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    print(f"\nSaved plot to {out_png}")


if __name__ == "__main__":
    main()
