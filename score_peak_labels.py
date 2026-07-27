#!/usr/bin/env python3
"""
score_peak_labels.py

Scores peak_detect2's raw_pk_flag and the old Group1 gate's group1_gate
against hand-labeled ground-truth packet peak timestamps - the labels
produced by the "Peak Label Bench" interactive tool, exported from a
window loaded via peakLabelLogger.m -> export_peak_label_log.m.

Two input files per window:
  - signal CSV: time, avg1, avg0, packet_average, raw_pk_flag, group1_gate
    (export_peak_label_log.m's output - the real per-cycle signal)
  - labels CSV: time (the hand-marked peak timestamps, one per line)

For each labeled peak, looks for the nearest raw_pk_flag / group1_gate
rising edge within a small sample tolerance (the labels are hand-clicked,
so allow a few samples of slop - matches the "off by 1-2 samples" human
click error already expected and tolerated downstream). Reports, per
label: whether raw_pk_flag fired nearby, whether group1_gate was also
high at/near that same edge, and the combined "would actually accept"
condition (raw_pk_flag AND group1_gate together - the old wiring's AND1).

Also reports totals: how many raw_pk_flag/group1_gate rising edges exist
in the whole window that DON'T correspond to any labeled peak (the
false-positive side of "peak_detect2 is too forgiving").

Usage:
    python score_peak_labels.py <signal_csv> <labels_csv> [--tolerance-samples N]
"""
import argparse
import csv
import sys


def load_signal_csv(path, gate_column="group1_gate"):
    # gate_column lets callers pick which column actually represents "the
    # gate to score" - as of this session's peakLabelLogger.m extension,
    # group1_gate/old_gate_accept/new_gate_accept can all be different
    # signals (confirmed group1_gate was still wired to the OLD circuit,
    # NOT groupGate_hdl.m's own accept_flag, in one real run - don't
    # assume group1_gate is "the new gate" without checking).
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        time, avg1, avg0, pavg, rpf, g1 = [], [], [], [], [], []
        for row in r:
            time.append(float(row["time"]))
            avg1.append(float(row["avg1"]))
            avg0.append(float(row.get("avg0", "nan") or "nan"))
            pavg.append(float(row.get("packet_average", "nan") or "nan"))
            rpf.append(float(row["raw_pk_flag"]))
            g1.append(float(row[gate_column]))
    return {"time": time, "avg1": avg1, "avg0": avg0, "packet_average": pavg,
            "raw_pk_flag": rpf, "group1_gate": g1}


def load_labels_csv(path):
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        return [float(row["time"]) for row in r]


def rising_edges(series):
    """Sample indices where series transitions from <=0.5 to >0.5."""
    edges = []
    prev = 0.0
    for i, v in enumerate(series):
        if v > 0.5 and prev <= 0.5:
            edges.append(i)
        prev = v
    return edges


def nearest_index(time, t):
    # binary search for nearest sample to time t
    lo, hi = 0, len(time) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if time[mid] < t:
            lo = mid + 1
        else:
            hi = mid
    if lo > 0 and abs(time[lo - 1] - t) < abs(time[lo] - t):
        return lo - 1
    return lo


def score(sig, labels, tol_samples):
    dt = sig["time"][1] - sig["time"][0]
    rpf_edges = rising_edges(sig["raw_pk_flag"])
    g1_edges = rising_edges(sig["group1_gate"])

    matched_rpf_edges = set()
    matched_g1_edges = set()
    per_label = []

    for t_label in labels:
        anchor = nearest_index(sig["time"], t_label)

        best_rpf, best_rpf_dist = None, None
        for e in rpf_edges:
            d = e - anchor
            if abs(d) <= tol_samples and (best_rpf_dist is None or abs(d) < abs(best_rpf_dist)):
                best_rpf, best_rpf_dist = e, d
        if best_rpf is not None:
            matched_rpf_edges.add(best_rpf)

        # group1_gate is a LEVEL condition (true for however long
        # avg1-1.5*packet_average-avg0 stays above 0), not a single-cycle
        # pulse like raw_pk_flag - matching it by "nearest rising edge"
        # would wrongly report a miss whenever the gate opened earlier and
        # is still high through the anchor. Check the level directly
        # within the tolerance window instead, same as combined_hit below.
        best_g1_dist = None
        lo_g1 = max(0, anchor - tol_samples)
        hi_g1 = min(len(sig["group1_gate"]) - 1, anchor + tol_samples)
        for i in range(lo_g1, hi_g1 + 1):
            if sig["group1_gate"][i] > 0.5:
                d = i - anchor
                if best_g1_dist is None or abs(d) < abs(best_g1_dist):
                    best_g1_dist = d
        best_g1 = best_g1_dist is not None
        for e in g1_edges:
            if lo_g1 <= e <= hi_g1:
                matched_g1_edges.add(e)

        # combined accept: is there a sample within tolerance where BOTH
        # raw_pk_flag and group1_gate are simultaneously high (not just
        # each independently edging somewhere nearby)?
        combined_hit = False
        combined_dist = None
        lo = max(0, anchor - tol_samples)
        hi = min(len(sig["raw_pk_flag"]) - 1, anchor + tol_samples)
        for i in range(lo, hi + 1):
            if sig["raw_pk_flag"][i] > 0.5 and sig["group1_gate"][i] > 0.5:
                d = i - anchor
                if combined_dist is None or abs(d) < abs(combined_dist):
                    combined_hit, combined_dist = True, d

        # amplitude context at the labeled peak - take the sample with the
        # highest avg1 within tolerance (the actual candidate height), and
        # read avg0/packet_average at that SAME sample so the margin below
        # matches exactly what group1_gate itself would have computed
        # there (avg1 - 1.5*packet_average - avg0 > 0).
        peak_i = max(range(lo, hi + 1), key=lambda i: sig["avg1"][i])
        avg1_v = sig["avg1"][peak_i]
        avg0_v = sig["avg0"][peak_i]
        pavg_v = sig["packet_average"][peak_i]
        margin = avg1_v - 1.5 * pavg_v - avg0_v

        # avg1 SPECIFICALLY at the cycle raw_pk_flag actually fired on
        # (as opposed to avg1's own true local max above) - tests whether
        # peak_detect2 is selecting a good candidate cycle, or firing
        # early/late relative to avg1's real summit and reading a lower
        # value than what was actually achievable nearby. avg1 and
        # raw_pk_flag are supposed to be tap-aligned by construction (both
        # read the same 200-sample-delayed tap), so in principle these
        # should already coincide - a real gap here would mean
        # peak_detect2's own 18-tap AND condition is satisfied on the
        # wrong cycle, not just that the gate threshold is too strict.
        avg1_at_rpf = None
        rpf_shortfall = None
        if best_rpf is not None:
            avg1_at_rpf = sig["avg1"][best_rpf]
            rpf_shortfall = avg1_v - avg1_at_rpf

        per_label.append({
            "t_label": t_label,
            "rpf_hit": best_rpf is not None,
            "rpf_offset_samples": best_rpf_dist,
            "g1_hit": best_g1,
            "g1_offset_samples": best_g1_dist,
            "combined_hit": combined_hit,
            "combined_offset_samples": combined_dist,
            "avg1": avg1_v,
            "avg0": avg0_v,
            "packet_average": pavg_v,
            "group1_margin": margin,
            "avg1_at_rpf": avg1_at_rpf,
            "rpf_shortfall": rpf_shortfall,
        })

    unmatched_rpf = [e for e in rpf_edges if e not in matched_rpf_edges]
    unmatched_g1 = [e for e in g1_edges if e not in matched_g1_edges]

    false_positives = []
    for e in unmatched_rpf:
        a1 = sig["avg1"][e]
        a0 = sig["avg0"][e]
        pa = sig["packet_average"][e]
        false_positives.append({
            "t": sig["time"][e],
            "avg1": a1,
            "avg0": a0,
            "packet_average": pa,
            "group1_margin": a1 - 1.5 * pa - a0,
        })

    return {
        "dt": dt,
        "n_labels": len(labels),
        "n_rpf_edges_total": len(rpf_edges),
        "n_g1_edges_total": len(g1_edges),
        "per_label": per_label,
        "unmatched_rpf_times": [sig["time"][e] for e in unmatched_rpf],
        "unmatched_g1_times": [sig["time"][e] for e in unmatched_g1],
        "false_positives": false_positives,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("signal_csv")
    ap.add_argument("labels_csv")
    ap.add_argument("--tolerance-samples", type=int, default=3,
                     help="how many samples of slop to allow around each hand-clicked label (default 3)")
    ap.add_argument("--gate-column", default="group1_gate",
                     help="which CSV column to score as 'the gate' (default group1_gate - "
                          "use new_gate_accept/old_gate_accept explicitly if the CSV has them, "
                          "don't assume group1_gate reflects groupGate_hdl.m without checking)")
    args = ap.parse_args()

    sig = load_signal_csv(args.signal_csv, args.gate_column)
    labels = load_labels_csv(args.labels_csv)
    result = score(sig, labels, args.tolerance_samples)

    dt_us = result["dt"] * 1e6
    n = result["n_labels"]
    rpf_hits = sum(1 for r in result["per_label"] if r["rpf_hit"])
    g1_hits = sum(1 for r in result["per_label"] if r["g1_hit"])
    combined_hits = sum(1 for r in result["per_label"] if r["combined_hit"])

    print(f"sample period: {dt_us:.3f} us, tolerance: +/-{args.tolerance_samples} samples "
          f"(+/-{dt_us * args.tolerance_samples:.3f} us)")
    print(f"labeled peaks: {n}")
    print()
    print(f"raw_pk_flag total pulses in window: {result['n_rpf_edges_total']} "
          f"({result['n_rpf_edges_total'] / n:.2f}x the labeled peak count)")
    print(f"group1_gate total pulses in window: {result['n_g1_edges_total']} "
          f"({result['n_g1_edges_total'] / n:.2f}x the labeled peak count)")
    print()
    print(f"raw_pk_flag hit rate  (peak_detect2 fires near a real peak):  {rpf_hits}/{n}  ({100*rpf_hits/n:.1f}%)")
    print(f"group1_gate hit rate  (gate is open near a real peak):       {g1_hits}/{n}  ({100*g1_hits/n:.1f}%)")
    print(f"combined hit rate     (both true at the same sample):        {combined_hits}/{n}  ({100*combined_hits/n:.1f}%)")
    print()

    misses = [r for r in result["per_label"] if not r["rpf_hit"]]
    if misses:
        print(f"raw_pk_flag MISSES ({len(misses)}) - peak_detect2 never fired near these labeled peaks:")
        for r in misses:
            print(f"  t={r['t_label']*1000:.3f} ms")
        print()

    gated_out = [r for r in result["per_label"] if r["rpf_hit"] and not r["combined_hit"]]
    if gated_out:
        print(f"group1_gate GATED OUT a real raw_pk_flag hit ({len(gated_out)}) - "
              f"peak_detect2 found it, group1_gate blocked it:")
        for r in gated_out:
            print(f"  t={r['t_label']*1000:.3f} ms  (rpf offset {r['rpf_offset_samples']} samples)")
        print()

    print(f"raw_pk_flag pulses NOT near any labeled peak (false positives): {len(result['unmatched_rpf_times'])}")
    if result["unmatched_rpf_times"]:
        preview = result["unmatched_rpf_times"][:15]
        print("  " + ", ".join(f"{t*1000:.3f}ms" for t in preview) +
              (" ..." if len(result["unmatched_rpf_times"]) > 15 else ""))


if __name__ == "__main__":
    main()
