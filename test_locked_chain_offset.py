#!/usr/bin/env python3
"""
test_locked_chain_offset.py

Tests whether the PRBS-offset "slips" _find_chain_slips flags are real
discontinuities, or just noise-driven tie-breaking artifacts from
best_prbs_match_np independently re-searching all 34 rotations for EVERY
sample with no memory of the previous sample's offset.

The dominant step (12) is not a coincidental empirical finding - it's
exact: the full transmitted PRBS chain advances 80 bits/packet (all 4
channels, ch1-ch4), but this decoder only checks ch2+ch3 (40 of those 80
bits) - so the correct per-packet rotation step for a 34-length pattern
is 80 mod 34 = 12, deterministically, confirmed by the user (2026-07-26).

This locks the offset forward from the first sample using that KNOWN,
deterministic +12/packet step (predicted_offset = (prev_offset + 12) %
pattern_len) for any two DIRECTLY ADJACENT present samples, instead of
independently re-searching all 34 rotations each time - re-bootstrapping
via a fresh independent search only after a genuine gap (a missing
sample breaks the chain of adjacency, same scope _find_chain_slips
itself uses). If BER under the LOCKED offset is about the same as BER
under the original independently-researched offset, the "slips" were
search-algorithm noise, not real content discontinuities - locking
should recover the same (or fewer) errors, never dramatically more,
since the locked offset is only ever used where the independent search
already mostly agrees with it anyway.

Usage:
    python test_locked_chain_offset.py <mat_file> [--priority v2_only]
"""
import argparse
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_prbs_ber import decode, BIT_SLICE, DEFAULT_PRBS_34, PATTERN_LEN  # noqa: E402
from read_prbs_binary_ch23 import best_prbs_match_np  # noqa: E402

DOMINANT_STEP = 12  # 80 (full 4-channel chain advance/packet) mod 34 - see module docstring


def _bits_used(v1_bits, v2_bits, v1_missing, v2_missing, i, priority, bit_slice):
    m1, m2 = bool(v1_missing[i]), bool(v2_missing[i])
    if priority == "v1_only":
        return None if m1 else v1_bits[i, bit_slice]
    if priority == "v2_only":
        return None if m2 else v2_bits[i, bit_slice]
    if priority == "v1":
        if not m1:
            return v1_bits[i, bit_slice]
        return None if m2 else v2_bits[i, bit_slice]
    if priority == "v2":
        if not m2:
            return v2_bits[i, bit_slice]
        return None if m1 else v1_bits[i, bit_slice]
    raise ValueError(priority)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mat_file")
    ap.add_argument("--priority", default="v2_only", choices=["v1_only", "v2_only", "v1", "v2"])
    args = ap.parse_args()

    (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf,
     has_low_conf, chunk_edge, v1_word_pos, v2_word_pos, start_time) = decode(args.mat_file)

    pattern = DEFAULT_PRBS_34
    n_bits = BIT_SLICE.stop - BIT_SLICE.start
    N = len(v1_bits)

    idx_arr = np.arange(n_bits)

    total_errors_indep = 0
    total_errors_locked = 0
    total_bits = 0
    n_present = 0
    locked_offset = None
    prev_present = False
    changed_rows = []  # rows where locked errors differ from independent errors

    for i in range(N):
        bu = _bits_used(v1_bits, v2_bits, v1_missing, v2_missing, i, args.priority, BIT_SLICE)
        if bu is None:
            locked_offset = None  # break the chain on a genuine gap
            prev_present = False
            continue

        # independent search (what the current pipeline does every sample)
        best_offset, best_matches, errors_indep, _ = best_prbs_match_np(bu, pattern)

        if locked_offset is None:
            # bootstrap (first sample, or first sample after a gap)
            locked_offset = best_offset
            errors_locked = errors_indep
        else:
            predicted = (locked_offset + DOMINANT_STEP) % PATTERN_LEN
            expected = pattern[(idx_arr + predicted) % PATTERN_LEN]
            errors_locked = int(np.sum(bu != expected))
            locked_offset = predicted

        total_errors_indep += errors_indep
        total_errors_locked += errors_locked
        total_bits += n_bits
        n_present += 1
        if errors_locked != errors_indep:
            changed_rows.append((i, errors_indep, errors_locked))

        prev_present = True

    ber_indep = total_errors_indep / total_bits if total_bits else 0.0
    ber_locked = total_errors_locked / total_bits if total_bits else 0.0

    print(f"{args.mat_file}  priority={args.priority}")
    print(f"present samples: {n_present}  bits/sample: {n_bits}  total bits: {total_bits}")
    print(f"independent-search BER: {total_errors_indep}/{total_bits}  = {ber_indep:.6f} ({100*ber_indep:.4f}%)")
    print(f"locked-offset BER:      {total_errors_locked}/{total_bits}  = {ber_locked:.6f} ({100*ber_locked:.4f}%)")
    print()
    print(f"{len(changed_rows)} samples where locked-offset error count differs from independent search:")
    print(f"{'idx':>5}  {'indep_errors':>12}  {'locked_errors':>13}")
    for i, e_indep, e_locked in changed_rows:
        print(f"{i:5d}  {e_indep:12d}  {e_locked:13d}")


if __name__ == "__main__":
    main()
