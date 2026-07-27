#!/usr/bin/env python3
"""
compare_peak_shapes.py

Tests a shape-based (not amplitude-based) way to distinguish real
preamble-correlation peaks from noise wiggles in avg1. A real peak is a
rectangular pulse (the preamble's "string of 1s") convolved with a
rectangular matched-filter window - that produces a predictable ramp-up /
(possible plateau) / ramp-down shape, not an arbitrary bump. A noise
wiggle that happens to be locally tallest has no reason to share that
shape. Also checks the FLANKING silence (samples on either side that sit
near the noise floor before/after the candidate) for asymmetry, since the
transmitted preamble's zero-runs on either side of the pulse may not be
equal length.

For each candidate (real peak or false positive), extracts a local avg1
window and computes:
  - rise_samples / fall_samples: how many samples on each side it takes
    to go from floor-ish (packet_average level) up to (near) the
    candidate's own peak value, and back down - a cheap proxy for
    "does this look like a ramp convolution, not a spike"
  - asymmetry: fall_samples - rise_samples (signed - positive means the
    fall side is slower/longer than the rise side)
  - linearity: how well a straight line fits the rise and fall flanks
    (lower residual = more ramp-like, matching a rectangular convolution;
    high residual = jagged/noise-like)

Usage:
    python compare_peak_shapes.py <signal_csv> <labels_csv> [--window-samples N]
"""
import argparse
import csv
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0].rsplit("\\", 1)[0])
from score_peak_labels import load_signal_csv, load_labels_csv, score, nearest_index  # noqa: E402


def extract_shape(sig, center_i, half_window):
    n = len(sig["avg1"])
    lo = max(0, center_i - half_window)
    hi = min(n - 1, center_i + half_window)
    local = sig["avg1"][lo:hi + 1]
    local_center = center_i - lo
    peak_val = local[local_center]
    # local floor, not a whole-file average - the min within this window's
    # own edges (which are far enough out to be past the ramp if
    # half_window is sized to the real matched-filter width)
    floor_level = min(local)
    thresh = floor_level + 0.5 * (peak_val - floor_level)  # half-max-ish crossing

    # walk left from center until we drop below thresh (rise side length)
    rise = 0
    for i in range(local_center, -1, -1):
        if local[i] < thresh:
            break
        rise += 1
    # walk right from center until we drop below thresh (fall side length)
    fall = 0
    for i in range(local_center, len(local)):
        if local[i] < thresh:
            break
        fall += 1

    # linearity: fit a line to the monotonically-rising run immediately
    # before the peak and the monotonically-falling run immediately after,
    # report mean squared deviation from that line (normalized by peak
    # height so it's comparable across different-amplitude candidates)
    def flank_linearity(seq):
        n2 = len(seq)
        if n2 < 3:
            return None
        xs = list(range(n2))
        mx = sum(xs) / n2
        my = sum(seq) / n2
        num = sum((x - mx) * (y - my) for x, y in zip(xs, seq))
        den = sum((x - mx) ** 2 for x in xs)
        if den == 0:
            return None
        slope = num / den
        intercept = my - slope * mx
        resid = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, seq)) / n2
        height = max(seq) - min(seq)
        return (resid ** 0.5) / height if height > 1e-12 else None

    rise_seq = local[max(0, local_center - rise):local_center + 1]
    fall_seq = local[local_center:min(len(local), local_center + fall + 1)]
    rise_lin = flank_linearity(rise_seq)
    fall_lin = flank_linearity(fall_seq)

    # multiplier-free alternative: count sign changes in the first
    # difference. A real ramp is monotonic (differences keep the same
    # sign until it turns over at the true peak) - noise has no reason to
    # stay monotonic, so it should flip constantly. Needs only a subtract
    # and a compare per sample, no multiply - directly HDL-realistic
    # (no multipliers available on this part).
    def sign_flips(seq):
        n2 = len(seq)
        if n2 < 3:
            return None
        flips = 0
        prev_sign = 0
        for i in range(1, n2):
            d = seq[i] - seq[i - 1]
            sign = 1 if d > 0 else (-1 if d < 0 else 0)
            if sign != 0 and prev_sign != 0 and sign != prev_sign:
                flips += 1
            if sign != 0:
                prev_sign = sign
        return flips

    rise_flips = sign_flips(rise_seq)
    fall_flips = sign_flips(fall_seq)

    return {
        "rise_samples": rise,
        "fall_samples": fall,
        "asymmetry": fall - rise,
        "rise_linearity": rise_lin,
        "fall_linearity": fall_lin,
        "rise_flips": rise_flips,
        "fall_flips": fall_flips,
        "total_flips": (rise_flips or 0) + (fall_flips or 0),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("signal_csv")
    ap.add_argument("labels_csv")
    ap.add_argument("--window-samples", type=int, default=250,
                     help="half-window in samples - default 250 covers a full rise+fall "
                          "cycle for the real 200-sample matched filter (preamble_length_inphasef)")
    ap.add_argument("--tolerance-samples", type=int, default=3)
    ap.add_argument("--out-csv", default=None, help="optional: write per-candidate shape features to CSV")
    args = ap.parse_args()

    sig = load_signal_csv(args.signal_csv)
    labels = load_labels_csv(args.labels_csv)
    result = score(sig, labels, args.tolerance_samples)

    real_rows = []
    for r in result["per_label"]:
        if not r["rpf_hit"]:
            continue
        anchor = nearest_index(sig["time"], r["t_label"])
        lo = max(0, anchor - args.tolerance_samples)
        hi = min(len(sig["avg1"]) - 1, anchor + args.tolerance_samples)
        peak_i = max(range(lo, hi + 1), key=lambda i: sig["avg1"][i])
        shape = extract_shape(sig, peak_i, args.window_samples)
        shape["kind"] = "real_accepted" if r["combined_hit"] else "real_gated_out"
        shape["t"] = r["t_label"]
        real_rows.append(shape)

    fp_rows = []
    rpf_edges_seen = set()
    for f in result["false_positives"]:
        anchor = nearest_index(sig["time"], f["t"])
        shape = extract_shape(sig, anchor, args.window_samples)
        shape["kind"] = "false_positive"
        shape["t"] = f["t"]
        fp_rows.append(shape)

    def summarize(label, rows, key):
        vals = [r[key] for r in rows if r[key] is not None]
        if not vals:
            print(f"  {label:16s} {key:16s} n=0")
            return
        print(f"  {label:16s} {key:16s} n={len(vals):3d}  "
              f"min/mean/max = {min(vals):.3f} / {sum(vals)/len(vals):.3f} / {max(vals):.3f}")

    print(f"=== {args.signal_csv} ===")
    for key in ["rise_samples", "fall_samples", "asymmetry", "rise_linearity", "fall_linearity",
                "rise_flips", "fall_flips", "total_flips"]:
        summarize("real peaks", real_rows, key)
        summarize("false positives", fp_rows, key)
        print()

    if args.out_csv:
        all_rows = real_rows + fp_rows
        fields = ["kind", "t", "rise_samples", "fall_samples", "asymmetry", "rise_linearity", "fall_linearity",
                   "rise_flips", "fall_flips", "total_flips"]
        with open(args.out_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in all_rows:
                w.writerow({k: row.get(k) for k in fields})
        print(f"wrote {len(all_rows)} rows to {args.out_csv}")


if __name__ == "__main__":
    main()
