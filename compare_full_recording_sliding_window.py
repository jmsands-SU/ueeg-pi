#!/usr/bin/env python3
"""
compare_full_recording_sliding_window.py

Reception-rate-vs-time comparison for two Simulink model runs, each a
single continuous decode of the full wall-clock-bounded recording (from
run_full_recording_model_comparison.m). Computes a sliding-window
reception rate directly from the one continuous decode, rather than
disjoint snapshot samples - this is what the earlier snapshot-survey
scripts were approximating with sparse sampling; a continuous decode plus
a sliding window gives the same kind of rate-vs-time picture without
gaps between samples.

Uses each decoded sample's own v1_word_pos/v2_word_pos (exact raw sample
position) to place it in real time, NOT idx/output_rate_hz - that's only
a nominal-cadence approximation and drifts from true time (confirmed
during PRBS debugging this session).

Usage:
    python compare_full_recording_sliding_window.py <out_dir> --labels A B [--config both] [--window 2] [--step 0.5]
        Expects "<label>_<config>.mat" files in out_dir (run_full_recording_
        model_comparison.m's naming convention).
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
from compare_prbs_ber import decode  # noqa: E402


def reception_time_series(mat_path):
    """Returns (t_seconds, diversity_received, v1_received, v2_received)
    sorted by time, one entry per decoded sample (including fully-missing
    ones - see below). diversity_received = at least one copy (v1 or v2)
    present - the "with time-diversity" reception rate. v1_received/
    v2_received = that single copy alone present - the "without diversity,
    only had this one copy" baseline, to show what the duplicate-copy
    reconciliation is actually buying us."""
    reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf, has_low_conf, chunk_edge = decode(mat_path)
    bits = reader.get_decoded_bits(2).ravel()
    v1_pos = bits["v1_word_pos"]
    v2_pos = bits["v2_word_pos"]
    # prefer v1's own frame position; fall back to v2's wherever v1 has none
    pos = np.where(v1_pos >= 0, v1_pos, v2_pos)
    N = len(pos)

    # A sample where BOTH copies are missing has no real frame to anchor a
    # time to (pos stays -1) - dropping those from the series instead of
    # timestamping them was a real bug: it silently removed exactly the
    # "not received" samples from every sliding window's denominator,
    # making every window read 100% regardless of actual reception. Give
    # those an approximate fallback time instead (idx/output_rate_hz -
    # this DOES drift from true time over a long decode, confirmed this
    # session, but only for these already-missing slots, and only by a
    # sliding-window-scale amount - not the precise per-sample cross-
    # referencing that drift mattered for elsewhere).
    t = np.where(pos >= 0, pos.astype(np.float64) / reader.bit_clock_hz,
                 np.arange(N) / reader.output_rate_hz)
    diversity_received = ~(v1_missing & v2_missing)
    v1_received = ~v1_missing
    v2_received = ~v2_missing
    order = np.argsort(t)
    return t[order], diversity_received[order], v1_received[order], v2_received[order]


def sliding_window_rate(t, received, window_s, step_s, t_min, t_max):
    centers = np.arange(t_min, t_max, step_s)
    rates = np.full(len(centers), np.nan)
    counts = np.zeros(len(centers), dtype=int)
    for i, c in enumerate(centers):
        mask = (t >= c - window_s / 2) & (t < c + window_s / 2)
        n = int(np.sum(mask))
        counts[i] = n
        if n > 0:
            rates[i] = np.mean(received[mask])
    return centers, rates, counts


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out_dir", help="Directory containing <label>_<config>.mat files")
    parser.add_argument("--labels", nargs=2, required=True)
    parser.add_argument("--config", default="both", choices=["ant1", "ant2", "both"])
    parser.add_argument("--window", type=float, default=2.0, help="sliding window width, seconds (default 2.0)")
    parser.add_argument("--step", type=float, default=0.5, help="sliding window step, seconds (default 0.5)")
    args = parser.parse_args()
    label_a, label_b = args.labels

    path_a = os.path.join(args.out_dir, f"{label_a}_{args.config}.mat")
    path_b = os.path.join(args.out_dir, f"{label_b}_{args.config}.mat")
    for p in (path_a, path_b):
        if not os.path.exists(p):
            raise FileNotFoundError(p)

    t_a, div_a, v1_a, v2_a = reception_time_series(path_a)
    t_b, div_b, v1_b, v2_b = reception_time_series(path_b)
    print(f"{label_a}: {len(t_a)} decoded samples, t={t_a.min():.2f}-{t_a.max():.2f}s")
    print(f"{label_b}: {len(t_b)} decoded samples, t={t_b.min():.2f}-{t_b.max():.2f}s")

    t_min = min(t_a.min(), t_b.min())
    t_max = max(t_a.max(), t_b.max())
    c_a, rdiv_a, n_a = sliding_window_rate(t_a, div_a, args.window, args.step, t_min, t_max)
    c_b, rdiv_b, n_b = sliding_window_rate(t_b, div_b, args.window, args.step, t_min, t_max)
    _, rv1_a, _ = sliding_window_rate(t_a, v1_a, args.window, args.step, t_min, t_max)
    _, rv2_a, _ = sliding_window_rate(t_a, v2_a, args.window, args.step, t_min, t_max)
    _, rv1_b, _ = sliding_window_rate(t_b, v1_b, args.window, args.step, t_min, t_max)
    _, rv2_b, _ = sliding_window_rate(t_b, v2_b, args.window, args.step, t_min, t_max)
    # "without diversity" baseline: the worse of the two individual copies
    # per window - a diversity scheme can't do better than picking between
    # what's actually available, so this is the honest single-copy floor
    # rather than cherry-picking whichever copy happened to be better.
    rnodiv_a = np.fmin(rv1_a, rv2_a)
    rnodiv_b = np.fmin(rv1_b, rv2_b)

    print(f"\n{'t (s)':>8}  {label_a+' (div)':>14} {label_a+' (no-div)':>16}  "
          f"{label_b+' (div)':>14} {label_b+' (no-div)':>16}")
    for i in range(len(c_a)):
        d_a = f"{100*rdiv_a[i]:5.1f}%" if not np.isnan(rdiv_a[i]) else "--"
        nd_a = f"{100*rnodiv_a[i]:5.1f}%" if not np.isnan(rnodiv_a[i]) else "--"
        d_b = f"{100*rdiv_b[i]:5.1f}%" if not np.isnan(rdiv_b[i]) else "--"
        nd_b = f"{100*rnodiv_b[i]:5.1f}%" if not np.isnan(rnodiv_b[i]) else "--"
        print(f"{c_a[i]:>8.2f}  {d_a:>14} {nd_a:>16}  {d_b:>14} {nd_b:>16}")

    print(f"\noverall mean: {label_a} diversity={100*np.nanmean(rdiv_a):.1f}%  "
          f"no-diversity={100*np.nanmean(rnodiv_a):.1f}%  "
          f"(gain={100*(np.nanmean(rdiv_a)-np.nanmean(rnodiv_a)):.1f} pts)")
    print(f"overall mean: {label_b} diversity={100*np.nanmean(rdiv_b):.1f}%  "
          f"no-diversity={100*np.nanmean(rnodiv_b):.1f}%  "
          f"(gain={100*(np.nanmean(rdiv_b)-np.nanmean(rnodiv_b)):.1f} pts)")

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(c_a, 100 * rdiv_a, "-", label=f"{label_a} (with diversity)", color="#3366cc", linewidth=1.5)
    ax.plot(c_a, 100 * rnodiv_a, "--", label=f"{label_a} (worse single copy)", color="#99b3e6", linewidth=1)
    ax.plot(c_b, 100 * rdiv_b, "-", label=f"{label_b} (with diversity)", color="#cc3333", linewidth=1.5)
    ax.plot(c_b, 100 * rnodiv_b, "--", label=f"{label_b} (worse single copy)", color="#e69999", linewidth=1)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(f"Reception rate (%) — {args.config}")
    ax.set_ylim(0, 105)
    ax.legend(fontsize=9)
    ax.set_title(f"Reception rate vs. time, with/without diversity  "
                 f"(sliding window={args.window}s, step={args.step}s, config={args.config})")
    ax.grid(True, alpha=0.3)
    out_png = os.path.join(args.out_dir, f"sliding_window_{args.config}.png")
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    print(f"\nSaved plot to {out_png}")


if __name__ == "__main__":
    main()
