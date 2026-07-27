#!/usr/bin/env python3
"""
analyze_low_conf_distribution.py

Validates the core assumption behind `_pick_by_low_conf` in compare_prbs_ber.py:
that a copy's low_conf_count (the bitThreshold_hdl/softCombine_hdl saturating
6-bit "how unsure was the soft decoder" counter, switchtosoftdecode model only)
actually predicts how many of that copy's bits are wrong against the known
PRBS ground truth. That arbitration scheme was built and used this session but
never checked against ground truth - this script is that check.

Unlike compare_prbs_ber.py/compare_snapshot_reception.py, which only ever
score the COMBINED/chosen copy's bits, this needs each copy's own error count
independently (regardless of which copy would have been picked), so it calls
best_prbs_match_np() itself per-copy per-sample rather than going through
compute_prbs_ber().

Usage:
    python analyze_low_conf_distribution.py
        (paths are hardcoded to the snapshot_model_comparison sweep - this is
        a one-off validation script, not a general tool)
"""
import glob
import os
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from scipy.stats import pearsonr
    _HAVE_SCIPY = True
except ImportError:
    _HAVE_SCIPY = False

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE  # noqa: E402
from make_figures import _load_mat  # noqa: E402
from read_prbs_binary_ch23 import best_prbs_match_np, DEFAULT_PRBS_34  # noqa: E402

SNAPSHOT_ROOT = r"C:\Temp\snapshot_model_comparison"
OUT_PNG = os.path.join(_HERE, "low_conf_vs_errors.png")

# Same fixed pipeline-delay mapping compare_prbs_ber.py's decode() uses
# internally to read low_conf_count (p = RATE_SCALE*word_pos + OFFSET,
# calibrated against 3 ground-truth anchors in a DENSE-reception
# reference case). Re-derived here (not exposed by decode()) specifically
# to measure how close each mapped index sits to a value transition in
# the raw low_conf_count register - see MARGIN_SAFE_THRESHOLD below for
# why this matters.
RATE_SCALE = 4
INPHASEF_OFFSET = -193

# The low_conf_count register is latched/held (updates once per packet,
# holds until the next). In dense/regular reception the fixed pipeline-
# delay mapping lands deep inside a wide plateau (checked: median margin
# ~51,000 samples in a 100%-reception snapshot) - very safe. In sparse/
# irregular reception (a real, common case - the segments we most care
# about for evaluating arbitration under stress) inter-packet timing gets
# irregular enough that the SAME fixed offset frequently lands within a
# couple samples of a transition edge (checked: median margin 2 samples,
# 53% within 2 samples, in a 15.7%-reception snapshot) - at that point
# which of two adjacent packets' values we actually read is essentially
# arbitrary. A reading within MARGIN_SAFE_THRESHOLD samples of an edge is
# not trustworthy as "this specific packet's own value".
MARGIN_SAFE_THRESHOLD = 5


def _margin_to_edge(raw, p):
    if not (0 <= p < len(raw)):
        return None
    val = raw[p]
    lo = p
    while lo > 0 and raw[lo - 1] == val:
        lo -= 1
    hi = p
    while hi < len(raw) - 1 and raw[hi + 1] == val:
        hi += 1
    return min(p - lo, hi - p)


def collect_pairs():
    """Returns {'v1': (low_conf, errors, margin), 'v2': (...)} kept SEPARATE
    per copy (not pooled - v1/v2 at the same sample aren't independent
    draws, see git history for the earlier pooling-bug fix), each entry
    also carrying its mapping margin (see MARGIN_SAFE_THRESHOLD) so the
    caller can separate trustworthy readings from edge-ambiguous ones.

    Also returns `paired` - one row per (file, sample_idx) where BOTH v1
    and v2 are present, for the within-sample paired-comparison test
    (does "lower low_conf" actually predict "lower error count" AT THE
    SAME MOMENT, which is the only comparison `_pick_by_low_conf` ever
    actually makes - the marginal per-copy correlation pools across many
    different moments/channel-conditions and can't distinguish real
    per-sample discriminative power from a shared time-varying trend
    affecting both copies at once)."""
    pattern_glob = os.path.join(SNAPSHOT_ROOT, "*", "switchtosoftdecode_s*.mat")
    files = sorted(glob.glob(pattern_glob))
    if not files:
        raise FileNotFoundError(f"No files matched {pattern_glob}")
    print(f"Found {len(files)} switchtosoftdecode snapshot files")

    pattern = DEFAULT_PRBS_34.copy()
    data = {"v1": ([], [], []), "v2": ([], [], [])}
    paired = []  # (v1_lc, v1_err, v1_margin, v2_lc, v2_err, v2_margin)
    n_skipped_no_signal = 0

    for f in files:
        reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf, has_low_conf, chunk_edge = decode(f)
        if not has_low_conf:
            n_skipped_no_signal += 1
            continue

        bits_ch2 = reader.get_decoded_bits(2).ravel()
        v1_word_pos = bits_ch2["v1_word_pos"]
        v2_word_pos = bits_ch2["v2_word_pos"]
        raw = np.asarray(_load_mat(f, ["low_conf_count"])["low_conf_count"]).ravel()

        per_sample = {}  # i -> {'v1': (lc, err, margin), 'v2': (...)}
        for copy_key, bits, missing, low_conf, word_pos in [
                ("v1", v1_bits, v1_missing, v1_low_conf, v1_word_pos),
                ("v2", v2_bits, v2_missing, v2_low_conf, v2_word_pos)]:
            low_conf_vals, error_counts, margins = data[copy_key]
            present_idx = np.flatnonzero(~missing)
            for i in present_idx:
                _, _, errors, _ = best_prbs_match_np(bits[i, BIT_SLICE], pattern)
                wp = int(word_pos[i])
                margin = _margin_to_edge(raw, RATE_SCALE * wp + INPHASEF_OFFSET) if wp >= 0 else None
                low_conf_vals.append(int(low_conf[i]))
                error_counts.append(int(errors))
                margins.append(margin if margin is not None else -1)
                per_sample.setdefault(i, {})[copy_key] = (int(low_conf[i]), int(errors), margin if margin is not None else -1)

        for i, entry in per_sample.items():
            if "v1" in entry and "v2" in entry:
                paired.append((*entry["v1"], *entry["v2"]))

    if n_skipped_no_signal:
        print(f"Skipped {n_skipped_no_signal} files with no low_conf_count signal "
              f"(unexpected for switchtosoftdecode - check inputs)")

    out = {k: (np.array(lv, dtype=np.int64), np.array(ec, dtype=np.int64), np.array(mg, dtype=np.int64))
           for k, (lv, ec, mg) in data.items()}
    paired_arr = np.array(paired, dtype=np.int64) if paired else np.zeros((0, 6), dtype=np.int64)
    return out, paired_arr


def paired_comparison_test(paired):
    """Direct test of what _pick_by_low_conf actually needs: at each sample
    where both copies are present, does the LOWER-low_conf copy actually
    have the LOWER (or equal) error count? Reports accuracy overall and
    split by mapping-margin safety - if accuracy is much higher once
    edge-ambiguous readings are excluded, that confirms the mapping
    imprecision (not a fundamentally weak signal) explains a chunk of the
    disagreement seen in individual cases like the fast-walk t=50s sample."""
    if len(paired) == 0:
        print("\nNo paired (both-present) samples found - can't run paired comparison test.")
        return
    v1_lc, v1_err, v1_m, v2_lc, v2_err, v2_m = paired.T

    both_safe = (v1_m >= MARGIN_SAFE_THRESHOLD) & (v2_m >= MARGIN_SAFE_THRESHOLD)
    either_risky = ~both_safe

    def _report(label, mask):
        n = int(np.sum(mask))
        if n == 0:
            print(f"  {label}: n=0")
            return
        lc1, e1, lc2, e2 = v1_lc[mask], v1_err[mask], v2_lc[mask], v2_err[mask]
        decided = lc1 != lc2  # ties give _pick_by_low_conf's "<=" no real signal either way
        n_decided = int(np.sum(decided))
        picks_v1 = lc1[decided] < lc2[decided]
        v1_better_or_eq = e1[decided] <= e2[decided]
        v2_better_or_eq = e2[decided] <= e1[decided]
        correct = np.where(picks_v1, v1_better_or_eq, v2_better_or_eq)
        strictly_correct = np.where(picks_v1, e1[decided] < e2[decided], e2[decided] < e1[decided])
        strictly_wrong = np.where(picks_v1, e1[decided] > e2[decided], e2[decided] > e1[decided])
        print(f"  {label}: n={n} (n with low_conf tie-break decided={n_decided})  "
              f"P(picked copy has <= errors)={np.mean(correct):.1%}  "
              f"P(strictly better)={np.mean(strictly_correct):.1%}  "
              f"P(strictly worse - a real misfire)={np.mean(strictly_wrong):.1%}")

    print(f"\nPaired within-sample comparison (both v1 and v2 present, n={len(paired)} total):")
    _report("all pairs", np.ones(len(paired), dtype=bool))
    _report(f"both margins >= {MARGIN_SAFE_THRESHOLD} (trustworthy readings)", both_safe)
    _report(f"either margin < {MARGIN_SAFE_THRESHOLD} (edge-ambiguous)", either_risky)


def summarize_distribution(label, low_conf_vals, error_counts):
    n_bits = BIT_SLICE.stop - BIT_SLICE.start
    lo, hi = int(low_conf_vals.min()), int(low_conf_vals.max())
    print(f"\n--- {label} ---")
    print(f"low_conf_count observed range: {lo}-{hi}  (saturating counter, nominal range 0-63)")
    print(f"Total samples: {len(low_conf_vals)}")

    # group by exact value - the observed range is usually small enough in
    # practice (per the task background) that per-value bins are more
    # informative than collapsing into wide ranges; only fall back to binning
    # if the range is actually wide enough to make per-value bins too sparse
    # to read.
    unique_vals = np.unique(low_conf_vals)
    if hi - lo <= 40:
        bin_edges = None
        groups = [(v, v) for v in unique_vals]
    else:
        # 16 roughly-equal-width bins across the observed range
        bin_edges = np.linspace(lo, hi + 1, 17)
        groups = [(int(bin_edges[i]), int(bin_edges[i + 1]) - 1) for i in range(len(bin_edges) - 1)]

    print(f"\n{'low_conf':>12}  {'n':>8}  {'mean_err':>9}  {'median_err':>10}  {'P(zero err)':>12}  {'mean BER':>10}")
    print("-" * 70)
    rows = []
    for lo_v, hi_v in groups:
        mask = (low_conf_vals >= lo_v) & (low_conf_vals <= hi_v)
        n = int(np.sum(mask))
        if n == 0:
            continue
        errs = error_counts[mask]
        mean_err = float(np.mean(errs))
        median_err = float(np.median(errs))
        p_zero = float(np.mean(errs == 0))
        label = f"{lo_v}" if lo_v == hi_v else f"{lo_v}-{hi_v}"
        flag = "  (n<10, noisy)" if n < 10 else ""
        print(f"{label:>12}  {n:>8}  {mean_err:>9.3f}  {median_err:>10.1f}  {p_zero:>11.1%}  {mean_err/n_bits:>9.4f}{flag}")
        rows.append((lo_v, hi_v, n, mean_err, median_err, p_zero))
    return rows


def _correlation(low_conf_vals, error_counts, label):
    if np.std(low_conf_vals) == 0 or np.std(error_counts) == 0:
        print(f"{label} Pearson correlation: undefined (no variance in one of the two variables)")
        return float("nan")
    elif _HAVE_SCIPY:
        r, p = pearsonr(low_conf_vals.astype(np.float64), error_counts.astype(np.float64))
        print(f"{label} Pearson correlation (low_conf_count vs error_count): r={r:.4f}  p={p:.3e}  n={len(low_conf_vals)}")
        return r
    else:
        r = float(np.corrcoef(low_conf_vals, error_counts)[0, 1])
        print(f"{label} Pearson correlation (low_conf_count vs error_count): r={r:.4f}  n={len(low_conf_vals)}  (scipy unavailable, p-value not computed)")
        return r


def _binned_means(low_conf_vals, error_counts, rows):
    xs = np.array([0.5 * (lo_v + hi_v) for lo_v, hi_v, n, *_ in rows])
    means = np.array([m for *_, m, med, p in rows])
    ns = np.array([n for _, _, n, *_ in rows])
    sems = np.array([
        (np.std(error_counts[(low_conf_vals >= lo_v) & (low_conf_vals <= hi_v)]) / np.sqrt(n)) if n > 1 else 0.0
        for lo_v, hi_v, n, *_ in rows
    ])
    return xs, means, ns, sems


def main():
    data, paired = collect_pairs()
    for copy_key in ("v1", "v2"):
        if len(data[copy_key][0]) == 0:
            print(f"No (low_conf, error) pairs collected for {copy_key} - nothing to analyze.")
            return

    # Report what fraction of ALL readings (not just the paired subset) are
    # edge-ambiguous in the first place - this is the population-level
    # version of the per-file check that found 0% risky in clean snapshots
    # vs >50% risky in a degraded one.
    for copy_key in ("v1", "v2"):
        _, _, mg = data[copy_key]
        valid = mg >= 0
        risky = valid & (mg < MARGIN_SAFE_THRESHOLD)
        print(f"{copy_key}: {np.sum(risky)}/{np.sum(valid)} ({100*np.mean(risky[valid]) if np.any(valid) else 0:.1f}%) "
              f"of readings are within {MARGIN_SAFE_THRESHOLD} samples of a low_conf_count transition edge (untrustworthy)")

    rows = {}
    corr = {}
    corr_safe = {}
    for copy_key in ("v1", "v2"):
        low_conf_vals, error_counts, margin = data[copy_key]
        rows[copy_key] = summarize_distribution(copy_key, low_conf_vals, error_counts)
        corr[copy_key] = _correlation(low_conf_vals, error_counts, f"{copy_key} (all readings)")
        safe = margin >= MARGIN_SAFE_THRESHOLD
        corr_safe[copy_key] = _correlation(low_conf_vals[safe], error_counts[safe], f"{copy_key} (margin-safe only, n={np.sum(safe)})")

    for copy_key in ("v1", "v2"):
        lv, ec, mg = data[copy_key]
        print(f"{copy_key}: mean low_conf={np.mean(lv):.3f}  mean errors={np.mean(ec):.3f}  n={len(lv)}")

    paired_comparison_test(paired)

    # --- plot: v1 and v2 as separate, distinguishable series (all readings -
    # the margin-safety finding is reported in text, not re-plotted, since
    # splitting the plot 4 ways (copy x margin) would be unreadable) ---
    fig, ax = plt.subplots(figsize=(11, 6.5))
    rng = np.random.default_rng(0)
    colors = {"v1": ("#3366cc", "#1a3d8f"), "v2": ("#cc6633", "#8f3d1a")}

    for copy_key in ("v1", "v2"):
        low_conf_vals, error_counts, margin = data[copy_key]
        scatter_color, mean_color = colors[copy_key]
        jitter = rng.uniform(-0.15, 0.15, size=len(low_conf_vals))
        ax.scatter(low_conf_vals + jitter, error_counts, s=6, alpha=0.06, color=scatter_color, linewidths=0)

        xs, means, ns, sems = _binned_means(low_conf_vals, error_counts, rows[copy_key])
        sizes = np.clip(20 + 4 * np.sqrt(ns), 20, 200)
        ax.errorbar(xs, means, yerr=sems, fmt="none", ecolor=mean_color, elinewidth=1, capsize=3, zorder=4)
        ax.scatter(xs, means, s=sizes, color=mean_color, edgecolors="white", linewidths=0.5, zorder=5,
                   label=f"{copy_key} binned mean ± SEM (r={corr[copy_key]:.3f}, margin-safe r={corr_safe[copy_key]:.3f}, n={len(low_conf_vals)})")

    ax.set_xlabel("low_conf_count")
    ax.set_ylabel(f"bit-error count (of {BIT_SLICE.stop - BIT_SLICE.start} bits)")
    ax.set_title("low_conf_count vs actual PRBS bit errors, per copy per sample\n"
                 "v1 and v2 kept separate (not pooled)")
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.savefig(OUT_PNG, dpi=150, bbox_inches="tight")
    print(f"\nSaved plot to {OUT_PNG}")


if __name__ == "__main__":
    main()
