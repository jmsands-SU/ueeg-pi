#!/usr/bin/env python3
"""
compare_snapshot_reception.py

Reception-rate comparison between two Simulink model runs across a set of
snapshot .mat files (walking-recording data, e.g. from
run_snapshot_survey_model_comparison.m). Two modes:

  - Default: no known bit pattern in the recording, so this reports
    packet-tracking/reception stats instead of BER: per-copy missing rate,
    cross-copy agreement rate (compute_cross_copy_ber - still meaningful
    without a reference pattern, just "did the two independently-decoded
    copies agree"), and the decoder's own
    packet_sequence_anomaly_count/header_drops.
  - --prbs: the recording IS a known PRBS sequence (e.g.
    agchybrid_antennashortways_slowwalk[_ant2].bin), so report real
    ground-truth BER (compute_prbs_ber, same as compare_prbs_ber.py's
    full-recording analysis) instead of the copy-agreement proxy. Only
    pass this when the underlying recording is actually PRBS-encoded -
    against real EEG data, decoded bits compared to a PRBS pattern that
    was never transmitted produce a meaningless ~50% "BER", not a real one.

Deliberately does NOT re-derive reception from push1 duty cycle or
packet_num regularity in isolation (packet_num alone is fragile - not
Viterbi-protected, read at the least-timing-settled point in a packet).
Reuses compare_prbs_ber.py's decode() - same TimeStampBasedReader decode
pipeline (phase-lock, distance-based gap inference, the chunk-boundary
duplicate-sample splice fix) - for the same robustness already validated
this session.

Usage:
    python compare_snapshot_reception.py <snapshot_dir> --labels A B
        Expects files named "<label>_s##.mat" in snapshot_dir, one series
        per label (matching run_snapshot_survey_model_comparison.m's
        naming convention).
    python compare_snapshot_reception.py <snapshot_dir> --labels A B --prbs
        Same, but reports real PRBS ground-truth BER instead of the
        cross-copy agreement proxy - only for snapshot series decoded from
        an actual PRBS recording.
"""
import argparse
import glob
import math
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE, VARNAMES, _align, _pick_by_low_conf, _tally  # noqa: E402
from make_figures import _load_mat  # noqa: E402
from read_prbs_binary_ch23 import compute_cross_copy_ber, compute_prbs_ber, DEFAULT_PRBS_34  # noqa: E402

# Fixed protocol constant, not derived per-file - see summarize_snapshot's
# expected_N comment for why reader.output_rate_hz is the wrong source.
PACKET_RATE_HZ_FIXED = 200.0


def _raw_word_count(mat_path):
    """Number of words in the raw push1/databit1/changeofstrength1/packetnum1
    signal - the TRUE input length for this snapshot's simulation window,
    independent of how much of it the decoder managed to sync to. Deliberately
    NOT reusing reader._words_processed_total for this (see summarize_snapshot's
    comment) - that counter stops advancing at the same point the decoder
    gives up finding a recognizable frame, so it shares N's own blind spot
    rather than fixing it. Small/cheap re-read (a few KB, not the raw IQ)."""
    d = _load_mat(mat_path, VARNAMES)
    arrays = _align([np.asarray(d[k], dtype=np.uint16).ravel() for k in VARNAMES])
    return len(arrays[0])


def _db(ber_new, ber_ref):
    if ber_ref == 0 or ber_new == 0:
        return "--"
    return f"{10 * math.log10(ber_new / ber_ref):+.1f} dB"


def summarize_snapshot(mat_path, prbs=False, pattern=None):
    (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf, has_low_conf,
     chunk_edge, v1_word_pos, v2_word_pos, start_time) = decode(mat_path)
    N = len(v1_bits)
    n_v1_miss = int(np.sum(v1_missing))
    n_v2_miss = int(np.sum(v2_missing))
    n_both_miss = int(np.sum(v1_missing & v2_missing))
    n_either_present = int(np.sum(~(v1_missing & v2_missing)))

    mm, total, ber_cross, n_both = compute_cross_copy_ber(v1_bits, v2_bits, v1_missing, v2_missing, BIT_SLICE)

    # N is only how many sample-slots the decoder managed to CONSTRUCT - if
    # it loses sync entirely for part of the window (no valid frame found at
    # all, not even placeholder-worthy), those slots never get created and N
    # silently shrinks below what the window duration should produce. A
    # naive `present / N` then reports the reception rate of whatever tiny
    # sliver the decoder did sync to, not of the actual window - e.g. N=28
    # with 78.6% "reception" on a 0.5s snapshot reads as decent when the
    # real story is the decoder lost lock for most of the window entirely
    # (flagged by the user 2026-07-25).
    #
    # expected_N must come from the RAW input word count (_raw_word_count),
    # NOT reader._words_processed_total - tried that first and it was wrong:
    # that counter stops advancing at the exact same point the decoder gives
    # up finding a recognizable frame (confirmed empirically - a snapshot
    # with N=28 had _words_processed_total covering only ~121ms of a 500ms
    # window), so it shares N's own blind spot instead of fixing it. The raw
    # word count is genuinely fixed by the snapshot duration regardless of
    # decode success (confirmed: identical 50001 words across snapshots that
    # decoded very differently), which is what "expected" needs to mean here.
    # PACKET_RATE_HZ_FIXED, not reader.output_rate_hz: the packet rate is a
    # FIXED protocol constant (confirmed with the user), not something to
    # re-derive per file. reader.output_rate_hz computes
    # bit_clock_hz*4/(8*avg_frame_length) where avg_frame_length is an
    # EMPIRICAL average from that specific file's own observed gaps
    # (total_words/total_gaps) - it lands near 200Hz with the class's own
    # defaults (bit_clock_hz=100kHz, nominal frame_length=250) but wobbles
    # slightly file-to-file, which is exactly the "expecting different
    # amounts for the same duration" bug being fixed here. A file
    # representing duration_seconds should always expect exactly
    # duration_seconds*200 packet slots, full stop.
    raw_words = _raw_word_count(mat_path)
    duration_s = raw_words / reader.bit_clock_hz if reader.bit_clock_hz else 0.0
    expected_N = duration_s * PACKET_RATE_HZ_FIXED
    coverage = N / expected_N if expected_N else 0.0

    result = dict(
        N=N,
        expected_N=expected_N,
        coverage=coverage,
        v1_missing_rate=n_v1_miss / N if N else 0.0,
        v2_missing_rate=n_v2_miss / N if N else 0.0,
        both_missing_rate=n_both_miss / N if N else 0.0,
        # TRUE reception rate against the FULL window, not just the
        # fraction of it the decoder managed to sync to - this is the
        # primary figure now. reception_rate_of_synced kept alongside as a
        # diagnostic (how good was reception WHEN synced, independent of
        # how much of the window that covers).
        reception_rate=n_either_present / expected_N if expected_N else 0.0,
        reception_rate_of_synced=n_either_present / N if N else 0.0,
        anomalies=reader.packet_sequence_anomaly_count,
        header_drops=reader.packet_sequence_header_drops,
        cross_copy_ber=ber_cross,
        n_cross_copy_both=n_both,
        n_chunk_edge=int(np.sum(chunk_edge)),
        has_low_conf=has_low_conf,
    )

    if prbs:
        n_bits = BIT_SLICE.stop - BIT_SLICE.start

        # v1->v2: v1 preferred, fallback to v2 when v1 absent - a real,
        # realizable combining policy (not a best-of-two-after-the-fact
        # number), matching compare_prbs_ber.py's headline diversity metric.
        #
        # Two figures reported, NOT one - compute_prbs_ber's own `ber`
        # blends genuine bit-level corruption with total sample loss
        # (a missing sample counts as all n_bits wrong), which is
        # misleading if presented as a plain "BER" (flagged by the user
        # 2026-07-25 in compare_prbs_ber.py - same fix applied here for
        # consistency, this script has the identical issue). `ber_prbs`/
        # `ber_prbs_lowconf` are now the TRUE bit-level rate (missing
        # samples excluded from both numerator and denominator) - the
        # primary, correct figure. `_blended` variants keep the old
        # missing-counted-as-wrong number for reference only.
        rows, _, _, _, _ = compute_prbs_ber(
            v1_bits, v2_bits, v1_missing, v2_missing, pattern, BIT_SLICE, copy_priority="v1")
        _, _, ber_prbs_bit, _ = _tally(rows, n_bits, exclude_missing=True)
        _, _, ber_prbs_blended, _ = _tally(rows, n_bits)
        result["ber_prbs"] = ber_prbs_bit
        result["ber_prbs_blended"] = ber_prbs_blended

        # low_conf_choice: quality-aware arbitration between two present
        # copies (see _pick_by_low_conf) - degenerates to pure v1-priority
        # when has_low_conf is False (e.g. plainmodel has no
        # bitThreshold_hdl/softCombine_hdl instrumentation).
        chosen_bits, chosen_missing = _pick_by_low_conf(
            v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf)
        rows_lc, _, _, _, _ = compute_prbs_ber(
            chosen_bits, chosen_bits, chosen_missing, chosen_missing, pattern, BIT_SLICE, copy_priority="v1_only")
        _, _, ber_prbs_lc_bit, _ = _tally(rows_lc, n_bits, exclude_missing=True)
        _, _, ber_prbs_lc_blended, _ = _tally(rows_lc, n_bits)
        result["ber_prbs_lowconf"] = ber_prbs_lc_bit
        result["ber_prbs_lowconf_blended"] = ber_prbs_lc_blended

        # N=0 (total outage - decoder found no valid frame to sync on
        # anywhere in this snapshot, e.g. only a handful of malformed raw
        # frames) makes _tally divide 0 errors / 0 bits and fall back to
        # 0.0 - reads as a "perfect" window rather than the total loss it
        # actually is, silently pulling the averaged BER down. Only the
        # blended figures get the worst-case 1.0 fallback here (they're
        # the ones meant to represent "how much of the truth data did we
        # get right, missing counted against us") - the true bit-level
        # figures have no bits to measure at all in this case, so leave
        # them at 0.0/undefined rather than asserting a bit-error rate
        # that was never actually observed.
        if N == 0:
            result["ber_prbs_blended"] = 1.0
            result["ber_prbs_lowconf_blended"] = 1.0

    return result


def find_snapshot_series(snapshot_dir, label):
    pattern = os.path.join(snapshot_dir, f"{label}_s*.mat")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern}")
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("snapshot_dir", help="Directory containing <label>_s##.mat snapshot files")
    parser.add_argument("--labels", nargs=2, required=True, help="Two model labels (must match filename prefixes)")
    parser.add_argument("--prbs", action="store_true",
                         help="Recording is a known PRBS sequence - report real ground-truth BER "
                              "(PRBS-BER, lc-BER) instead of the cross-copy agreement proxy (x-BER)")
    parser.add_argument("--pattern-file", default=None,
                         help="Text file with PRBS pattern (0/1 chars) - default DEFAULT_PRBS_34. Only used with --prbs")
    args = parser.parse_args()

    pattern = None
    if args.prbs:
        if args.pattern_file:
            text = open(args.pattern_file).read().strip().replace("\n", "").replace(" ", "")
            pattern = np.array([int(c) for c in text], dtype=np.uint8)
        else:
            pattern = DEFAULT_PRBS_34.copy()

    label_a, label_b = args.labels
    files_a = find_snapshot_series(args.snapshot_dir, label_a)
    files_b = find_snapshot_series(args.snapshot_dir, label_b)
    if len(files_a) != len(files_b):
        print(f"⚠️  {label_a} has {len(files_a)} snapshots, {label_b} has {len(files_b)} - "
              f"comparing by index up to the shorter series", file=sys.stderr)
    n = min(len(files_a), len(files_b))

    results_a = [summarize_snapshot(f, prbs=args.prbs, pattern=pattern) for f in files_a[:n]]
    results_b = [summarize_snapshot(f, prbs=args.prbs, pattern=pattern) for f in files_b[:n]]

    ber_cols = [("ber_prbs", "bitBER", ".4f"), ("ber_prbs_lowconf", "lc-bitBER", ".4f"),
                ("ber_prbs_blended", "blendBER", ".4f"), ("ber_prbs_lowconf_blended", "lc-blendBER", ".4f")] \
        if args.prbs else [("cross_copy_ber", "x-BER", ".4f")]

    print(f"\n{'='*100}")
    print(f"SNAPSHOT RECEPTION COMPARISON  ({label_a}  vs  {label_b})" + ("  [PRBS ground truth]" if args.prbs else ""))
    print(f"{'='*100}")
    ber_header = "  ".join(f"{name:>8} {name:>8}" for _, name, _ in ber_cols)
    header = (f"{'snap':>4}  {'N':>5}  {'expN':>5}  "
              f"{'cov%':>6} {'cov%':>6}  "
              f"{'recv%':>7} {'recv%':>7}  "
              f"{'synced%':>7} {'synced%':>7}  "
              f"{'v1miss%':>8} {'v1miss%':>8}  "
              f"{'v2miss%':>8} {'v2miss%':>8}  "
              f"{'bothmiss%':>10} {'bothmiss%':>10}  "
              f"{ber_header}  "
              f"{'anom':>5} {'anom':>5}  "
              f"{'hdrop':>5} {'hdrop':>5}  "
              f"{'edge':>5} {'edge':>5}")
    print(header)
    ber_label_header = "  ".join(f"{label_a:>8} {label_b:>8}" for _ in ber_cols)
    print(f"{'':>4}  {'':>5}  {'':>5}  {label_a:>6} {label_b:>6}  {label_a:>7} {label_b:>7}  "
          f"{label_a:>7} {label_b:>7}  {label_a:>8} {label_b:>8}  "
          f"{label_a:>8} {label_b:>8}  {label_a:>10} {label_b:>10}  "
          f"{ber_label_header}  {label_a:>5} {label_b:>5}  "
          f"{label_a:>5} {label_b:>5}  {label_a:>5} {label_b:>5}")

    for i in range(n):
        ra, rb = results_a[i], results_b[i]
        ber_row = "  ".join(f"{ra[key]:>8.4f} {rb[key]:>8.4f}" for key, _, _ in ber_cols)
        print(f"{i+1:>4}  {ra['N']:>5}  {ra['expected_N']:>5.0f}  "
              f"{100*ra['coverage']:>6.1f} {100*rb['coverage']:>6.1f}  "
              f"{100*ra['reception_rate']:>7.1f} {100*rb['reception_rate']:>7.1f}  "
              f"{100*ra['reception_rate_of_synced']:>7.1f} {100*rb['reception_rate_of_synced']:>7.1f}  "
              f"{100*ra['v1_missing_rate']:>8.2f} {100*rb['v1_missing_rate']:>8.2f}  "
              f"{100*ra['v2_missing_rate']:>8.2f} {100*rb['v2_missing_rate']:>8.2f}  "
              f"{100*ra['both_missing_rate']:>10.2f} {100*rb['both_missing_rate']:>10.2f}  "
              f"{ber_row}  "
              f"{ra['anomalies']:>5} {rb['anomalies']:>5}  "
              f"{ra['header_drops']:>5} {rb['header_drops']:>5}  "
              f"{ra['n_chunk_edge']:>5} {rb['n_chunk_edge']:>5}")

    # --- aggregate across all snapshots ---
    def _agg(results, key):
        return sum(r[key] for r in results) / len(results) if results else 0.0

    ber_agg_row = "  ".join(f"{_agg(results_a,key):>8.4f} {_agg(results_b,key):>8.4f}" for key, _, _ in ber_cols)
    print(f"\n{'-'*100}")
    print(f"{'AVERAGE':>4}  {'':>5}  {'':>5}  "
          f"{100*_agg(results_a,'coverage'):>6.1f} {100*_agg(results_b,'coverage'):>6.1f}  "
          f"{100*_agg(results_a,'reception_rate'):>7.1f} {100*_agg(results_b,'reception_rate'):>7.1f}  "
          f"{100*_agg(results_a,'reception_rate_of_synced'):>7.1f} {100*_agg(results_b,'reception_rate_of_synced'):>7.1f}  "
          f"{100*_agg(results_a,'v1_missing_rate'):>8.2f} {100*_agg(results_b,'v1_missing_rate'):>8.2f}  "
          f"{100*_agg(results_a,'v2_missing_rate'):>8.2f} {100*_agg(results_b,'v2_missing_rate'):>8.2f}  "
          f"{100*_agg(results_a,'both_missing_rate'):>10.2f} {100*_agg(results_b,'both_missing_rate'):>10.2f}  "
          f"{ber_agg_row}  "
          f"{sum(r['anomalies'] for r in results_a):>5} {sum(r['anomalies'] for r in results_b):>5}  "
          f"{sum(r['header_drops'] for r in results_a):>5} {sum(r['header_drops'] for r in results_b):>5}  "
          f"{sum(r['n_chunk_edge'] for r in results_a):>5} {sum(r['n_chunk_edge'] for r in results_b):>5}")

    if args.prbs:
        no_lc = [lbl for lbl, res in [(label_a, results_a), (label_b, results_b)]
                 if res and not res[0]['has_low_conf']]
        if no_lc:
            print(f"\nlc-bitBER note: no low_conf_count signal for {', '.join(no_lc)} - "
                  f"degenerates to pure v1-priority, not real quality-aware arbitration")
        print(f"bitBER/lc-bitBER = TRUE bit-level error rate (missing samples excluded entirely, "
              f"not counted as wrong) - the real number to trust")
        print(f"blendBER/lc-blendBER = for reference only - compute_prbs_ber's native metric, "
              f"which counts a missing sample as ALL bits wrong. This is NOT a bit error rate; "
              f"a window with many missing samples but zero real corruption can still show a high "
              f"blendBER. Don't use it as if it were BER.")
    else:
        print(f"\nx-BER = cross-copy BER (v1 vs v2 direct comparison, no ground truth needed)")
    print(f"expN = expected sample count for this window's actual duration, at the FIXED "
          f"{PACKET_RATE_HZ_FIXED:.0f}Hz packet rate (a protocol constant, not re-derived per file - "
          f"reader.output_rate_hz's own empirical average wobbles slightly file-to-file, which is "
          f"exactly the inconsistency this avoids), regardless of whether the decoder managed to "
          f"sync for all of it")
    print(f"cov% = coverage = N / expN - how much of the window the decoder even managed to sync to "
          f"at all. Low coverage means the decoder lost lock for part of the window; those samples "
          f"never even get a 'missing' placeholder, they just aren't in the array (flagged by the "
          f"user 2026-07-25)")
    print(f"recv% = TRUE reception rate against the FULL window (present samples / expN) - the "
          f"primary figure, accounts for low-coverage windows correctly")
    print(f"synced% = reception rate among ONLY the samples the decoder synced to (present / N) - "
          f"kept for reference; can look artificially high on a low-coverage window since it never "
          f"sees the part of the window that was lost entirely (e.g. N=28 on a window that should "
          f"have ~100 samples can still show high synced% while true recv% is very low)")
    print(f"'edge' = chunk-boundary-artifact samples (see compare_prbs_ber.py) - expect ~0 for short "
          f"snapshots entirely within one run_sim_stream.m chunk")


if __name__ == "__main__":
    main()
