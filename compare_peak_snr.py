#!/usr/bin/env python3
"""
compare_peak_snr.py

Tests the "it's just lower SNR" hypothesis for why group1_gate vetoes
real, correctly-detected peaks in some windows but not others. Uses
score_peak_labels.py's per-label avg1/avg0/packet_average/group1_margin
values (group1_margin = avg1 - 1.5*packet_average - avg0, the exact
group1_gate formula - positive means the gate would be open) and compares
distributions across outcome groups (gated-out vs combined-hit vs miss)
and across windows.

Usage:
    python compare_peak_snr.py <window1_signal_csv> <window1_labels_csv> \
                                <window2_signal_csv> <window2_labels_csv> \
                                [--tolerance-samples N]
"""
import argparse
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0].rsplit("\\", 1)[0])
from score_peak_labels import load_signal_csv, load_labels_csv, score  # noqa: E402


def summarize(label, rows):
    if not rows:
        print(f"  {label:22s} n=0")
        return
    def stats(key, subset=None):
        vals = [r[key] for r in (subset or rows)]
        return min(vals), sum(vals) / len(vals), max(vals)
    a1 = stats("avg1")
    a0 = stats("avg0")
    pa = stats("packet_average")
    mg = stats("group1_margin")
    print(f"  {label:22s} n={len(rows):3d}  "
          f"avg1[min/mean/max]={a1[0]:.4f}/{a1[1]:.4f}/{a1[2]:.4f}  "
          f"avg0={a0[1]:.4f}  packet_avg={pa[1]:.4f}  "
          f"margin[min/mean/max]={mg[0]:+.4f}/{mg[1]:+.4f}/{mg[2]:+.4f}")

    # rpf_shortfall: how much lower avg1 was AT the raw_pk_flag firing
    # cycle vs. avg1's own true local max nearby - tests whether
    # peak_detect2 is selecting a suboptimal candidate cycle (separate
    # question from whether the gate threshold itself is too strict).
    # false_positives rows don't have this field at all (no labeled peak
    # to compare against), so skip them here.
    with_rpf = [r for r in rows if r.get("rpf_shortfall") is not None]
    if with_rpf:
        sf = stats("rpf_shortfall", with_rpf)
        n_meaningful = sum(1 for r in with_rpf if r["rpf_shortfall"] > 0.001)
        print(f"  {'':22s} candidate shortfall (avg1 at rpf cycle vs. true local max): "
              f"[min/mean/max]={sf[0]:+.4f}/{sf[1]:+.4f}/{sf[2]:+.4f}  "
              f"({n_meaningful}/{len(with_rpf)} off by >0.001)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("w1_signal")
    ap.add_argument("w1_labels")
    ap.add_argument("w2_signal")
    ap.add_argument("w2_labels")
    ap.add_argument("--tolerance-samples", type=int, default=3)
    args = ap.parse_args()

    windows = [
        ("window 1", args.w1_signal, args.w1_labels),
        ("window 2", args.w2_signal, args.w2_labels),
    ]

    for name, sig_path, lab_path in windows:
        sig = load_signal_csv(sig_path)
        labels = load_labels_csv(lab_path)
        result = score(sig, labels, args.tolerance_samples)
        rows = result["per_label"]

        gated_out = [r for r in rows if r["rpf_hit"] and not r["combined_hit"]]
        combined_hit = [r for r in rows if r["combined_hit"]]
        missed = [r for r in rows if not r["rpf_hit"]]

        print(f"\n=== {name} (n={len(rows)}) ===")
        summarize("all labeled peaks", rows)
        summarize("group1_gate GATED OUT", gated_out)
        summarize("combined hit (accepted)", combined_hit)
        summarize("raw_pk_flag missed", missed)
        summarize("FALSE POSITIVES (not a real peak)", result["false_positives"])


if __name__ == "__main__":
    main()
