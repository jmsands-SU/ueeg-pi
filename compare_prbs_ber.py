#!/usr/bin/env python3
"""
compare_prbs_ber.py

Ground-truth PRBS bit-error-rate + packet-drop comparison between two
Simulink model runs (e.g. non_stacked_output_fixoverflow_switchtosoftdecode
vs non_stacked_output_fixoverflow), using the TimeStampBasedReader decoder
already validated this session (phase-lock, header-drop fix, low_conf_count
arbitration) rather than the live-hardware-only TimeStampBasedPRBSReader.

Unlike compare_reception_rate.py's duplicate-copy-agreement proxy, this
checks decoded bits against the known PRBS reference sequence directly -
real BER, not "did the two copies agree" (which two copies can do while
both being wrong).

Reuses compute_cross_copy_ber()/compute_prbs_ber()/best_prbs_match_np()/
DEFAULT_PRBS_34 from read_prbs_binary_ch23.py (vendored from
https://github.com/jmsands-SU/ueeg-pi) unmodified - those functions already
operate on in-memory bit arrays, no need for that project's 21-byte binary
intermediate format.

Usage:
    python compare_prbs_ber.py <mat_file_A> <mat_file_B> [--labels A B]
"""
import argparse
import math
import os
import sys
from collections import Counter

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from make_figures import _load_mat  # noqa: E402
from sdr_reader_gcs_write import TimeStampBasedReader  # noqa: E402
from read_prbs_binary_ch23 import (  # noqa: E402
    compute_cross_copy_ber,
    compute_prbs_ber,
    DEFAULT_PRBS_34,
)

VARNAMES = ["push1", "databit1", "changeofstrength1", "packetnum1"]
# Channel 2 and channel 3 are inline continuations of the same PRBS chain
# (ch2 = bits 1-20, ch3 = bits 21-40), not independent/differently-offset
# sequences - confirmed with the user. Concatenated per sample in decode()
# below, so this is the full 40-bit width, not read_prbs_binary_ch23.py's
# original CH23_SLICE (which assumed an 80-bit 4-channel-packed row this
# decoder doesn't produce).
BIT_SLICE = slice(0, 40)
PATTERN_LEN = len(DEFAULT_PRBS_34)  # 34
EDGE_MARGIN = 5  # samples from the start/end of the array counted as "window edge"

# Fixed protocol constant, not derived per-file - same fix as
# compare_snapshot_reception.py's PACKET_RATE_HZ_FIXED (see that file's
# comment for why reader.output_rate_hz's own per-file empirical average
# is the wrong source for an expected-sample-count denominator).
PACKET_RATE_HZ_FIXED = 200.0


def _align(arrays):
    max_len = max(len(a) for a in arrays)
    out = []
    for a in arrays:
        if len(a) == max_len:
            out.append(a)
        else:
            ratio = round(max_len / len(a))
            out.append(np.repeat(a, ratio)[:max_len])
    return out


def decode(mat_path):
    """Returns (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf,
    v2_low_conf, has_low_conf, chunk_edge, v1_word_pos, v2_word_pos,
    start_time) for one model's simulation output .mat file."""
    d = _load_mat(mat_path, VARNAMES + ["START_TIME"])
    start_time = float(np.asarray(d["START_TIME"]).ravel()[0])
    push, databit, error, packetnum = (
        np.asarray(d[k], dtype=np.uint16).ravel() for k in VARNAMES
    )
    push, databit, error, packetnum = _align([push, databit, error, packetnum])

    # Pack real low_conf_count bits into words (2026-07-26) - previously
    # omitted (only databit/packetnum/error/push were packed), which meant
    # the reader's OWN internal low_conf_count_arr/DecodedPacket.low_conf_count
    # was always 0 through this decode() path, silently disabling any
    # low_conf-aware logic INSIDE decode_from_word_stream (e.g.
    # _decode_packet_groups' low_conf-based group-builder filter) even
    # though has_low_conf/v1_low_conf/v2_low_conf below (a separate,
    # post-hoc cross-reference) looked correctly populated. Same domain
    # conversion as _low_conf_at below (RATE_SCALE=4, INPHASEF_OFFSET=-193),
    # MINUS the frame_len shift - that one-frame-latency correction is
    # applied internally by the reader itself (_lc_idx = start_idx +
    # frame_len, see sdr_reader_gcs_write.py), not here; this just aligns
    # the raw low_conf_count signal 1:1 with words' own index domain.
    _n_words = len(push)
    _raw_lc_early = _load_mat(mat_path, ["low_conf_count"]).get("low_conf_count")
    if _raw_lc_early is not None:
        _raw_lc_early = np.asarray(_raw_lc_early).ravel()
        _idx = 4 * np.arange(_n_words) - 193
        _in_range = (_idx >= 0) & (_idx < len(_raw_lc_early))
        low_conf_per_word = np.zeros(_n_words, dtype=np.uint16)
        low_conf_per_word[_in_range] = _raw_lc_early[_idx[_in_range]].astype(np.uint16) & 0x3F
    else:
        low_conf_per_word = np.zeros(_n_words, dtype=np.uint16)

    words = ((databit & 0x1) | ((packetnum & 0x7) << 4) | ((error & 0x1) << 7) | ((push & 0x1) << 8)
             | (low_conf_per_word << 9)).astype(np.uint16)

    reader = TimeStampBasedReader(
        enable_gcs=False, quiet=True, gcs_channels=(1, 2, 3, 4),
        # 251 = a normal 250-word frame plus one duplicate idle word from
        # run_sim_stream.m's per-chunk sim() restart (confirmed to land in
        # the idle tail after the last push pulse, never in the payload) -
        # simulation-chunking artifact only, not a real hardware frame length.
        accepted_frame_lengths=(250, 248, 251), frame_length_counts={250: 18, 248: 1},
    )
    reader.decode_from_word_stream(words)

    # ch2 = bits 1-20, ch3 = bits 21-40 of the same PRBS chain - concatenate
    # per sample into the full 40-bit comparison. Both channels come from
    # the same synchronized group-builder loop (see _decode_packet_groups),
    # so they're already sample-for-sample aligned - no separate alignment
    # needed here.
    bits_ch2 = reader.get_decoded_bits(2).ravel()
    bits_ch3 = reader.get_decoded_bits(3).ravel()
    v1_bits = np.concatenate([bits_ch2["v1_bits"], bits_ch3["v1_bits"]], axis=1)
    v2_bits = np.concatenate([bits_ch2["v2_bits"], bits_ch3["v2_bits"]], axis=1)
    # either half missing means the full 40-bit block can't be validated
    v1_missing = bits_ch2["v1_missing"] | bits_ch3["v1_missing"]
    v2_missing = bits_ch2["v2_missing"] | bits_ch3["v2_missing"]

    frame_len_lookup = {int(abs_word): int(fl) for abs_word, pkt_n, fl, is_valid in reader._raw_frame_log}

    # low_conf_count: NOT sourced from bits_ch2["v1_low_conf"] -
    # compare_prbs_ber's `words` reconstruction above never packs the
    # low_conf_count bits (only databit/packetnum/error/push), so
    # _extract_channel_packets' internal low_conf_count_arr - and hence
    # DecodedPacket.low_conf_count / bits_ch2["v1_low_conf"] - is always 0
    # here (confirmed empirically). Read the raw low_conf_count Outport
    # directly instead and cross-reference it via each sample's own
    # v1_word_pos/v2_word_pos using the 4x-domain-scale + fixed pipeline-
    # delay mapping.
    #
    # ONE-FRAME LATENCY (critical): the low_conf_count register for a packet
    # is not valid at that packet's own readout position - it's still
    # holding the PREVIOUS packet's value there. The count only finishes
    # accumulating one full frame period after the packet's own push-start
    # (the soft decoder streams the whole payload before the count settles),
    # so packet A's value doesn't appear until ~250 words later, right where
    # packet B is read. Reading at word_pos therefore returns the WRONG
    # (previous) packet's confidence - it must be read at word_pos +
    # frame_len instead. Validated against PRBS ground truth: this raises
    # low_conf-vs-actual-error correlation from r=0.36 to r=0.48, and fixes
    # concrete arbitration misfires (e.g. agchybrid_fastwalk t=50s sample 83,
    # where v1 was actually perfect but read as the previous packet's high
    # count and got wrongly rejected). +248 (a short frame's length) does
    # nothing; only the +250 nominal frame period works - the latency is the
    # packet period, so shift by each packet's own frame_len (nominal 250).
    RATE_SCALE = 4
    INPHASEF_OFFSET = -193
    FRAME_PERIOD_DEFAULT = 250
    _raw = _load_mat(mat_path, ["low_conf_count"]).get("low_conf_count")

    def _low_conf_at(word_pos):
        out = np.zeros(len(word_pos), dtype=np.uint8)
        if _raw is None:
            # older/non-instrumented model (e.g. plainmodel has no
            # bitThreshold_hdl/softCombine_hdl) - no low_conf_count signal
            # exists at all, not just zero everywhere. Leave as all-zero;
            # summarize() flags this case explicitly rather than silently
            # presenting it as "no disagreements found".
            return out
        raw_low_conf = np.asarray(_raw).ravel()
        valid = word_pos >= 0
        wp = word_pos[valid].astype(np.int64)
        # shift each packet forward by its own frame length (the one-frame
        # pipeline latency above); default to the nominal 250-word period
        # for any position not in the frame log
        frame_len = np.array([frame_len_lookup.get(int(w), FRAME_PERIOD_DEFAULT) for w in wp], dtype=np.int64)
        p = RATE_SCALE * (wp + frame_len) + INPHASEF_OFFSET
        in_range = (p >= 0) & (p < len(raw_low_conf))
        idx = np.flatnonzero(valid)
        out[idx[in_range]] = raw_low_conf[p[in_range]].astype(np.uint8)
        return out

    v1_low_conf = _low_conf_at(bits_ch2["v1_word_pos"])
    v2_low_conf = _low_conf_at(bits_ch2["v2_word_pos"])
    has_low_conf = _raw is not None

    # chunk-edge mask: True wherever either copy's underlying frame was a
    # 251-word run_sim_stream.m chunk-restart artifact (see
    # _find_boundary_duplicate_offset doc in sdr_reader_gcs_write.py).
    # Confirmed this session: even after splicing the duplicate word back
    # out, whichever channel's extraction window happens to contain the
    # duplicate's position can still carry a residual value-level
    # distortion from the restart transient itself - a simulation-harness
    # artifact, not a receiver-logic bug, so excludable rather than
    # counted for or against either model.

    def _is_chunk_edge(word_pos):
        out = np.zeros(len(word_pos), dtype=bool)
        for i in range(len(word_pos)):
            wp = int(word_pos[i])
            if wp >= 0 and frame_len_lookup.get(wp) == 251:
                out[i] = True
        return out

    chunk_edge = _is_chunk_edge(bits_ch2["v1_word_pos"]) | _is_chunk_edge(bits_ch2["v2_word_pos"])
    return (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf,
            has_low_conf, chunk_edge, bits_ch2["v1_word_pos"], bits_ch2["v2_word_pos"], start_time)


def _pick_by_low_conf(v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf):
    """Quality-aware selection: when both copies are present, use whichever
    has the LOWER low_conf_count (more confident); fall back to whichever
    copy is present when only one is. Unlike compute_prbs_ber's copy_priority
    modes (pure fallback-on-missing, never compares present copies), this
    actually arbitrates between two present-but-possibly-disagreeing copies -
    same principle as the low_conf_count-based v1/v2 arbitration built into
    sdr_reader_gcs_write.py's production decode path this session.

    Returns (chosen_bits, chosen_missing) - feed straight into
    compute_prbs_ber with copy_priority='v1_only' (v1 slot IS the choice
    already made, no further fallback needed there).
    """
    N = len(v1_bits)
    chosen_bits = np.zeros_like(v1_bits)
    chosen_missing = np.zeros(N, dtype=bool)
    for i in range(N):
        m1, m2 = bool(v1_missing[i]), bool(v2_missing[i])
        if m1 and m2:
            chosen_missing[i] = True
        elif m1:
            chosen_bits[i] = v2_bits[i]
        elif m2:
            chosen_bits[i] = v1_bits[i]
        elif v1_low_conf[i] <= v2_low_conf[i]:
            chosen_bits[i] = v1_bits[i]
        else:
            chosen_bits[i] = v2_bits[i]
    return chosen_bits, chosen_missing


def _find_chain_slips(rows, pattern_len, persistence=5):
    """Detect a PRBS-chain discontinuity that's invisible to every other
    metric in this file - discovered 2026-07-25, see
    project_prbs_chain_slip_at_chunk_boundary.md (memory).

    best_prbs_match_np (read_prbs_binary_ch23.py) independently re-searches
    all `pattern_len` circular rotations for EVERY sample, with no check
    that the winning rotation is consistent with the previous sample's. A
    genuine one-bit shift in the decoded PRBS content - CONFIRMED 2026-07-25
    via a controlled test (not just correlation): loading the same raw
    window through load_snapshot_to_workspace.m as ONE continuous
    simulation, with no run_sim_stream.m chunking at all, makes the
    anomaly disappear. CHAIN_SLIP and CHUNK_EDGE are confirmed to be the
    SAME underlying chunk-restart mechanism, not two independent root
    causes - kept as separate detectors only because they're empirically
    distinguishable (chunk_edge's narrow 251-word frame-length check
    misses cases this one catches). Still scores a perfect 40/40 match,
    just at a different offset - invisible to `missing`, `errors`, and
    chunk_edge itself.

    STEP VALUE (12) - fully explained, not just empirically observed
    (resolved 2026-07-26, was an open question before): the FULL
    transmitted PRBS chain advances 80 bits/packet across all 4 channels
    (ch1-ch4), but this decoder only checks ch2+ch3 (40 of those 80 bits,
    per BIT_SLICE) - so the correct per-packet rotation step against the
    34-length pattern is 80 mod 34 = 12, deterministically. The earlier
    "naive derivation predicts 6, nobody reconciled the factor-of-2"
    caveat was simply using the wrong bit count (40, what we check,
    instead of 80, what's actually consumed from the chain each packet).

    PERSISTENCE-BASED SLIP DETECTION (rewritten 2026-07-26 - the original
    version below is what this replaced): the original detector flagged
    ANY single adjacent-pair step mismatch as a slip. Confirmed via a
    locked-offset comparison test (test_locked_chain_offset.py) against
    two real, independently-verified cases in the same file that this
    conflated: idx~398, where the step mismatch was followed by 200+
    consecutive present samples ALL consistent with the NEW offset (a
    real, sustained realignment - stronger evidence than the original
    single-transition flag even showed) vs idx=721, where the very next
    present samples reverted to matching the OLD offset (a one-sample
    tie-breaking artifact, not a real discontinuity - independently
    confirmed clean in both the raw radio and the Simulink output, 2026-
    07-26). The original detector could not tell these apart; this
    version can, by checking whether a candidate mismatch's NEW offset
    keeps explaining the next `persistence` present samples (real slip)
    or not (noise - keep tracking from the OLD predicted offset instead,
    don't flag it).

    CAVEAT (still open): `persistence=5` is a reasonable-looking default
    given the two confirmed cases (sustained run of 200+ vs a single
    sample), not independently derived or swept - a real slip that
    somehow resolves in under 5 samples, or noise that coincidentally
    strings together 5 consistent-looking samples, would still be
    misclassified. Worth revisiting with more confirmed cases before
    trusting the exact threshold.

    Returns a boolean array the same length as `rows`, True at slip
    positions (indexed by each row's own `idx`, not by scan position -
    safe to use directly as a per-sample mask), and the dominant step
    used (for the caller to cross-check between independently-run copies
    - see summarize()'s use of this for v1 vs v2).
    """
    n = len(rows)
    slip = np.zeros(n, dtype=bool)

    # Pass 1: dominant step, same as before (kept as a cross-check /
    # fallback identifier even though the step value itself is now known
    # to be a fixed protocol constant - see docstring)
    steps = []
    prev_offset = None
    for r in rows:
        if r["missing"]:
            prev_offset = None
            continue
        if prev_offset is not None:
            steps.append((r["offset"] - prev_offset) % pattern_len)
        prev_offset = r["offset"]
    if not steps:
        return slip, None
    counts = Counter(steps)
    dominant, dominant_n = counts.most_common(1)[0]

    # Pass 2: present-only sequence (idx, offset), remembering which
    # entries immediately follow a gap (missing sample resets the chain -
    # same scope as the original detector)
    present = []
    prev_offset = None
    for r in rows:
        if r["missing"]:
            prev_offset = None
            continue
        present.append(dict(idx=r["idx"], offset=r["offset"], gap_before=prev_offset is None))
        prev_offset = r["offset"]

    # Pass 3: walk the present-only sequence with a locked/predicted
    # offset, only flagging (and only adopting) a step mismatch as a real
    # slip if the NEW offset keeps explaining the next `persistence`
    # present samples too - a one-sample blip that reverts back to the
    # OLD prediction is treated as search noise, not flagged, and doesn't
    # perturb the tracked offset going forward.
    locked_offset = None
    k = 0
    while k < len(present):
        p = present[k]
        if p["gap_before"] or locked_offset is None:
            locked_offset = p["offset"]
            k += 1
            continue
        predicted = (locked_offset + dominant) % pattern_len
        if p["offset"] == predicted:
            locked_offset = predicted
            k += 1
            continue

        # candidate slip - check whether the NEW offset explains the next
        # `persistence` present samples (stop early at a gap, same as
        # the original chain-breaking rule)
        new_base = p["offset"]
        checked = 0
        matches_new = 0
        for j in range(k + 1, min(k + 1 + persistence, len(present))):
            if present[j]["gap_before"]:
                break
            checked += 1
            expected = (new_base + dominant * (j - k)) % pattern_len
            if present[j]["offset"] == expected:
                matches_new += 1

        if checked > 0 and matches_new == checked:
            slip[p["idx"]] = True
            locked_offset = new_base
        else:
            # noise - keep tracking from the OLD predicted offset,
            # this sample's own (likely tie-broken) offset is ignored
            locked_offset = predicted
        k += 1

    return slip, dominant


def _find_frame_gaps(reader):
    """The actual signature of a USB drop, per the user's own description
    (2026-07-25): a frame that IS correctly identified (valid, accepted
    frame length) has a word-distance from the previous correctly-
    identified frame that does NOT match what a single-frame packet_num
    step implies. This directly reuses the reader's OWN internal gap
    logic (`_estimate_frames_in_gap_linear` - the same method
    _extract_channel_packets uses to decide how many placeholder frames
    to backfill) instead of an ad-hoc "is there a big gap or invalid
    frame somewhere nearby" window search (an earlier, cruder version of
    this check, replaced 2026-07-25 for being too indirect).

    Walks every pair of consecutive VALID (accepted-length) frames in
    reader._raw_frame_log and flags any transition where the word-
    distance implies MORE than one frame elapsed. Whether an invalid-
    length frame happens to sit inside that span decides the label
    (REJECTED_FRAME vs CLEAN_GAP) but NOT whether it's a real gap in the
    first place - that's decided by the distance-vs-offset check alone,
    matching how the reader itself actually detects gaps.

    Returns a list of dicts: start_word, end_word, frames_in_gap,
    has_rejected_frame_inside.
    """
    valid_frames = sorted(
        (int(abs_word), int(fl)) for abs_word, pkt_n, fl, is_valid in reader._raw_frame_log if is_valid
    )
    invalid_words = sorted(
        int(abs_word) for abs_word, pkt_n, fl, is_valid in reader._raw_frame_log if not is_valid
    )

    gaps = []
    for (w0, _), (w1, _) in zip(valid_frames, valid_frames[1:]):
        distance = w1 - w0
        frames_in_gap = reader._estimate_frames_in_gap_linear(distance)
        if frames_in_gap > 1:
            has_rejected = any(w0 < iw < w1 for iw in invalid_words)
            gaps.append(dict(start_word=w0, end_word=w1, frames_in_gap=frames_in_gap,
                              has_rejected_frame_inside=has_rejected))
    return gaps


def _gap_signature_at(gaps, anchor_word, margin=500, exact=False):
    """Which gap (if any) a sample's anchor word position falls inside.

    BUG FIXED 2026-07-26: start_word/end_word are drawn directly from
    reader._raw_frame_log's VALID frames (see _find_frame_gaps) - they
    are, by construction, already-confirmed-good frames marking the
    bookends of the gap, not part of the anomaly. The original version
    used an INCLUSIVE bound (start_word - margin <= anchor <= end_word +
    margin), so a sample sitting EXACTLY AT end_word (a real, valid,
    clean frame - literally the recovery point right after the gap)
    inherited the gap's REJECTED_FRAME/CLEAN_GAP label anyway. Caught via
    a direct raw push1/packetnum1 spot-check (2026-07-26) that found a
    completely clean, valid frame at a position the classifier had
    flagged REJECTED_FRAME - traced to this exact off-by-boundary bug,
    not a false alarm about the data.

    Fix: the gap region is the OPEN interval (start_word, end_word) -
    the margin only accounts for a MISSING sample's own true position
    being unknown (its anchor comes from a neighboring present sample's
    word_pos as a proxy, per _classify_anomalies' own docstring), so it
    should never let a sample AT a confirmed-valid boundary inherit the
    gap's label. exact=True (used for non-missing samples, which HAVE
    their own real word_pos - no neighbor-proxy uncertainty to account
    for) skips the margin entirely and uses the strict open interval.
    """
    for g in gaps:
        if exact:
            if g["start_word"] < anchor_word < g["end_word"]:
                return "REJECTED_FRAME" if g["has_rejected_frame_inside"] else "CLEAN_GAP"
        else:
            lo = g["start_word"] - margin
            hi = g["end_word"] + margin
            if lo < anchor_word < hi and anchor_word != g["start_word"] and anchor_word != g["end_word"]:
                return "REJECTED_FRAME" if g["has_rejected_frame_inside"] else "CLEAN_GAP"
    return None


def _classify_anomalies(rows, v1_missing, v2_missing, v1_word_pos, v2_word_pos,
                         start_time, chain_slip, chunk_edge, reader, N):
    """Systematic per-sample classifier for --classify: labels every
    non-clean sample (missing, or errors>0) as exactly one of:

      WINDOW_EDGE    - within EDGE_MARGIN samples of the start/end of the
                        whole array (a window-boundary truncation effect,
                        not a real anomaly - e.g. one copy's time-paired
                        transmission falls just outside the analysis window)
      CLEAN_GAP      - falls inside a real frame-distance-vs-offset gap
                        (see _find_frame_gaps) with no rejected frame
                        inside it. STILL UNCONFIRMED as of 2026-07-25 -
                        theoretically the "clean" presentation of a GNU
                        Radio USB drop, but neither real case checked via
                        scope so far turned out to be this one (both were
                        REJECTED_FRAME - see project_gnuradio_usb_drop_
                        resync_gaps.md). Don't assert this is confirmed.
      REJECTED_FRAME - falls inside the same kind of gap, but with an
                        invalid-length frame physically present inside it.
                        CONFIRMED 2026-07-25 (two real scope screenshots -
                        15m t~2.52s and 10m t~6.97s) to be a genuine GNU
                        Radio USB drop, not separate RF corruption as
                        originally guessed - this is what a drop looks
                        like when the dropped word count doesn't line up
                        with frame boundaries.
      CHAIN_SLIP     - flagged by _find_chain_slips (chunk-boundary artifact
                        - see project_prbs_chain_slip_at_chunk_boundary.md)
      CHUNK_EDGE     - flagged by the existing 251-word chunk_edge detector
      UNEXPLAINED    - none of the above. Do NOT assume these are
                        artifacts - they may be genuine signal degradation
                        with no detectable raw-level trace, and should
                        stay counted, not excluded, absent a specific
                        reason to exclude them.

    A sample can carry more than one label. This function only classifies
    - it does not decide what to exclude.

    Returns a list of dicts: idx, t1, t2, missing, errors, labels (list of
    str), in row order.
    """
    gaps = _find_frame_gaps(reader)

    out = []
    for r in rows:
        i = r["idx"]
        if not (r["missing"] or r["errors"] > 0):
            continue
        wp1, wp2 = int(v1_word_pos[i]), int(v2_word_pos[i])
        t1 = start_time + wp1 / 100000.0 if wp1 >= 0 else float("nan")
        t2 = start_time + wp2 / 100000.0 if wp2 >= 0 else float("nan")

        labels = []
        if i < EDGE_MARGIN or i >= N - EDGE_MARGIN:
            labels.append("WINDOW_EDGE")
        # anchor the raw-frame-log search on whichever copy actually has a
        # position (a genuinely missing sample has no word_pos of its own -
        # use the other copy's position, or the nearest present neighbor,
        # as the nearest real anchor, matching the "bounding window between
        # nearest real anchors" convention used elsewhere in this project).
        # is_own_position tracks whether `anchor` is THIS sample's own real
        # word_pos (exact - no neighbor-proxy uncertainty, so no margin
        # should be applied) vs a neighbor's position used as a stand-in
        # (genuinely uncertain - the margin fallback still applies there).
        anchor = wp1 if wp1 >= 0 else (wp2 if wp2 >= 0 else None)
        is_own_position = anchor is not None
        if anchor is None:
            for j in (i - 1, i + 1):
                if 0 <= j < N:
                    aw1, aw2 = int(v1_word_pos[j]), int(v2_word_pos[j])
                    if aw1 >= 0 or aw2 >= 0:
                        anchor = aw1 if aw1 >= 0 else aw2
                        break
        if anchor is not None:
            sig = _gap_signature_at(gaps, anchor, exact=is_own_position)
            if sig:
                labels.append(sig)
        if chain_slip[i]:
            labels.append("CHAIN_SLIP")
        if chunk_edge[i]:
            labels.append("CHUNK_EDGE")
        if not labels:
            labels.append("UNEXPLAINED")

        out.append(dict(idx=i, t1=t1, t2=t2, missing=r["missing"], errors=r["errors"], labels=labels))
    return out


def _print_classification(label, classified, N):
    print(f"\n  --- anomaly classification ({label}) ---")
    if not classified:
        print("  (no missing/errored samples)")
        return
    print(f"  {'idx':>5} {'t1':>10} {'t2':>10} {'missing':>7} {'errors':>6}  labels")
    for c in classified:
        print(f"  {c['idx']:>5} {c['t1']:>10.5f} {c['t2']:>10.5f} {str(c['missing']):>7} "
              f"{c['errors']:>6}  {'+'.join(c['labels'])}")
    from collections import Counter as _Counter
    label_counts = _Counter(lbl for c in classified for lbl in c['labels'])
    print(f"  {len(classified)}/{N} samples flagged. By label: {dict(label_counts)}")
    if label_counts.get("UNEXPLAINED"):
        print(f"  ⚠️  {label_counts['UNEXPLAINED']} UNEXPLAINED sample(s) - do not assume these are "
              f"artifacts; they may be genuine signal degradation and should stay counted "
              f"unless specifically investigated (see _classify_anomalies docstring)")


def _tally(rows, n_bits, exclude_mask=None, exclude_missing=False):
    """Generic error tally over `rows`.

    exclude_mask (optional): drop rows flagged in this mask entirely (the
    harness-artifact exclusion used for chunk_edge/chain_slip) - neither
    penalized nor counted as correct, just removed from both numerator
    and denominator.

    exclude_missing: if True, ALSO drop missing rows entirely instead of
    counting them as n_bits-wrong (compute_prbs_ber's OWN behavior -
    every missing sample adds n_bits to total_errors, i.e. treats a
    totally-absent sample as if all its bits were individually wrong).
    That blended number is useful as an overall "how much of the truth
    data did we get right" figure, but it is NOT a real bit error rate -
    it folds total frame/sample loss into what gets reported as "BER".
    Flagged by the user 2026-07-25 as misleading when presented as a pure
    bit-level BER. Use exclude_missing=True for the genuine bit-level-only
    rate (computed only over samples that were actually decoded).
    """
    kept = rows
    if exclude_mask is not None:
        kept = [r for r in kept if not exclude_mask[r["idx"]]]
    n_miss = sum(1 for r in kept if r["missing"])
    if exclude_missing:
        kept = [r for r in kept if not r["missing"]]
    t_err = sum(r["errors"] for r in kept)
    t_bits = n_bits * len(kept)
    ber = t_err / t_bits if t_bits else 0.0
    return t_err, t_bits, ber, n_miss


def _db(ber_new, ber_ref):
    """10*log10(BER_new / BER_ref). Negative = improvement. Same formula
    read_prbs_binary_ch23.py's main() uses for its dB column."""
    if ber_ref == 0 or ber_new == 0:
        return "--"
    return f"{10 * math.log10(ber_new / ber_ref):+.1f} dB"


def summarize(label, mat_path, pattern, classify=False):
    print(f"\n{'='*70}\n{label}  ({mat_path})\n{'='*70}")
    (reader, v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf, has_low_conf,
     chunk_edge, v1_word_pos, v2_word_pos, start_time) = decode(mat_path)
    N = len(v1_bits)

    if N == 0:
        # Nothing decoded (e.g. a very short/weak-link test slice) - every
        # stat below is a N-in-the-denominator percentage or BER ratio, so
        # bail out with zeroed placeholders instead of dividing by zero.
        print("Samples decoded: 0 (nothing to compare - skipping BER stats)")
        empty = (0, 0, 0.0, 0)
        results = {lbl: dict(blended_all=empty, blended_excl=empty, bit_all=empty, bit_excl=empty)
                   for lbl in ["v1_only", "v2_only", "v1->v2", "v2->v1", "low_conf_choice"]}
        return dict(reader=reader, N=0, n_both_miss=0,
                    header_drops=reader.packet_sequence_header_drops,
                    anomalies=reader.packet_sequence_anomaly_count,
                    ber_cross=0.0, results=results, has_low_conf=has_low_conf,
                    n_chunk_edge=0, n_chain_slip=0,
                    v1_rr=0.0, v2_rr=0.0, expected_n=0.0, n_v1_present=0, n_v2_present=0)

    # --- packet-level drop/anomaly stats (richer than read_prbs_binary_ch23.py's
    # bit/sample-only accounting - pulled directly from the decoder itself) ---
    n_v1_miss = int(np.sum(v1_missing))
    n_v2_miss = int(np.sum(v2_missing))
    n_both_miss = int(np.sum(v1_missing & v2_missing))
    n_chunk_edge = int(np.sum(chunk_edge))

    # v1/v2 reception rate, individually - standing requirement (same
    # "report v1/v2 individually, never just combined" principle already
    # applied to BER, extended to reception rate too - see
    # feedback_report_v1_v2_individually.md). Fixed-200Hz denominator,
    # NOT N (N only counts what the decoder managed to construct at all -
    # same coverage blind spot compare_snapshot_reception.py's expected_N
    # fix addressed).
    #
    # Denominator window = first-detected-packet to last-detected-packet
    # timestamp (2026-07-26 correction), NOT the full raw file duration.
    # Using the full file duration penalized RR for startup transients
    # (time before the receiver's first lock, e.g. symbol_sync/AGC settling)
    # that occur before there's anything to detect at all - that's a
    # startup-cost artifact, not a missed packet, so it shouldn't count
    # against RR. word_pos is -1 for missing samples (see
    # sdr_reader_gcs_write.py's v1_word_pos/v2_word_pos fill logic), so
    # take the min/max over whichever copy was actually present at each end.
    all_valid_wp = np.concatenate([v1_word_pos[~v1_missing], v2_word_pos[~v2_missing]])
    if len(all_valid_wp) >= 2:
        span_words = int(all_valid_wp.max()) - int(all_valid_wp.min())
        duration_s = span_words / reader.bit_clock_hz if reader.bit_clock_hz else 0.0
    else:
        duration_s = 0.0
    expected_n = duration_s * PACKET_RATE_HZ_FIXED
    n_v1_present = N - n_v1_miss
    n_v2_present = N - n_v2_miss
    v1_rr = n_v1_present / expected_n if expected_n else 0.0
    v2_rr = n_v2_present / expected_n if expected_n else 0.0

    print(f"Samples decoded: {N}  (expected {expected_n:.0f} @ {PACKET_RATE_HZ_FIXED:.0f}Hz "
          f"fixed rate over {duration_s:.3f}s from 1st to last detected packet)")
    print(f"  v1 RR: {n_v1_present}/{expected_n:.0f} ({100*v1_rr:.1f}%)   "
          f"v2 RR: {n_v2_present}/{expected_n:.0f} ({100*v2_rr:.1f}%)")
    print(f"  v1 missing: {n_v1_miss} ({100*n_v1_miss/N:.2f}% of decoded)   "
          f"v2 missing: {n_v2_miss} ({100*n_v2_miss/N:.2f}% of decoded)   "
          f"both missing: {n_both_miss} ({100*n_both_miss/N:.2f}% of decoded)")
    print(f"  chunk-edge samples (simulation-harness artifact, excluded from "
          f"'excl. artifacts' column below): {n_chunk_edge} ({100*n_chunk_edge/N:.2f}%)")
    print(f"  packet_sequence_anomaly_count: {reader.packet_sequence_anomaly_count}")
    print(f"  packet_sequence_header_drops:  {reader.packet_sequence_header_drops}")
    print(f"  prefix_overlap_frames_skipped: {reader.prefix_overlap_frames_skipped}")
    print(f"  placeholder_inserts (cross/intra/group_builder): "
          f"{reader.placeholder_inserts_cross_chunk}/{reader.placeholder_inserts_intra_chunk}/"
          f"{reader.placeholder_inserts_group_builder}")

    # --- cross-copy BER (v1 vs v2 direct comparison, no PRBS pattern needed) ---
    mm, total, ber_cross, n_both = compute_cross_copy_ber(v1_bits, v2_bits, v1_missing, v2_missing, BIT_SLICE)
    print(f"\nCross-copy BER (v1 vs v2, {n_both} samples w/ both present): "
          f"{mm}/{total}  BER={ber_cross:.6f} ({100*ber_cross:.4f}%)")

    # --- PRBS ground-truth BER, both priority directions ---
    n_bits = BIT_SLICE.stop - BIT_SLICE.start
    rows_by_priority = {}
    for label_p, priority in [("v1_only", "v1_only"), ("v2_only", "v2_only"),
                               ("v1->v2", "v1"), ("v2->v1", "v2")]:
        rows, _, _, _, _ = compute_prbs_ber(
            v1_bits, v2_bits, v1_missing, v2_missing, pattern, BIT_SLICE, copy_priority=priority)
        rows_by_priority[label_p] = rows

    # --- chain-slip detection (see _find_chain_slips docstring) - checked
    # independently on each raw copy's own v1_only/v2_only rows (not the
    # fallback-mixed v1->v2/v2->v1 rows, whose `offset` can legitimately
    # jump the moment they switch which copy they're reading) ---
    slip_v1, dominant_v1 = _find_chain_slips(rows_by_priority["v1_only"], PATTERN_LEN)
    slip_v2, dominant_v2 = _find_chain_slips(rows_by_priority["v2_only"], PATTERN_LEN)
    # v1 and v2 are two independent per-sample offset series, but both
    # should be reading the same underlying PRBS chain rate - if their
    # own dominant steps disagree, that's a sign _find_chain_slips picked
    # an unreliable "dominant" on at least one side (see its own caveat
    # docstring) and this file's chain-slip numbers shouldn't be trusted
    # without a closer look.
    if dominant_v1 is not None and dominant_v2 is not None and dominant_v1 != dominant_v2:
        print(f"  ⚠️  chain-slip dominant step disagrees between copies (v1={dominant_v1}, "
              f"v2={dominant_v2}) - chain-slip detection may be unreliable on this file")
    # OR'd together and then applied to ALL FOUR priority columns below,
    # including v2_only/v2->v1 at indices where only v1's own offset
    # series showed a slip (and vice versa) - this assumes the restart
    # transient's effect window is wide enough to also taint the "clean"
    # copy at the same sample index (v1/v2 are ~10ms apart in real time).
    # That assumption is NOT independently verified - it's a deliberately
    # conservative choice (excluding a possibly-still-good sample) rather
    # than a confirmed necessity.
    chain_slip = slip_v1 | slip_v2
    n_chain_slip = int(np.sum(chain_slip))
    print(f"  chain-slip samples (PRBS-offset discontinuity invisible to missing/errors/"
          f"chunk_edge - see project_prbs_chain_slip_at_chunk_boundary.md - also excluded "
          f"from 'excl. artifacts' column; excluding these does NOT necessarily lower BER, "
          f"see _find_chain_slips docstring): {n_chain_slip} ({100*n_chain_slip/N:.2f}%)")
    excl_mask = chunk_edge | chain_slip

    if classify:
        _print_classification("v1_only", _classify_anomalies(
            rows_by_priority["v1_only"], v1_missing, v2_missing, v1_word_pos, v2_word_pos,
            start_time, chain_slip, chunk_edge, reader, N), N)
        _print_classification("v2_only", _classify_anomalies(
            rows_by_priority["v2_only"], v1_missing, v2_missing, v1_word_pos, v2_word_pos,
            start_time, chain_slip, chunk_edge, reader, N), N)

    # Each entry carries FOUR tallies, built in one shot (not mutated in
    # place): the TRUE bit-level BER (bit_all/bit_excl, excludes missing
    # samples entirely - the real bit error rate among what was actually
    # decoded) and the BLENDED figure compute_prbs_ber computes natively
    # (blended_all/blended_excl, counts a missing sample as all n_bits
    # wrong - useful as an overall "how much of the truth data did we get
    # right" number, but NOT a bit error rate - see _tally's docstring).
    # bit-level is what's used for baseline/dB/diversity-gain comparisons
    # below; blended is shown alongside for the missing-inclusive picture.
    results = {
        label_p: dict(
            blended_all=_tally(rows, n_bits),
            blended_excl=_tally(rows, n_bits, exclude_mask=excl_mask),
            bit_all=_tally(rows, n_bits, exclude_missing=True),
            bit_excl=_tally(rows, n_bits, exclude_mask=excl_mask, exclude_missing=True),
        )
        for label_p, rows in rows_by_priority.items()
    }

    # --- low_conf_count-arbitrated diversity: actually compares present-but-
    # possibly-disagreeing copies, unlike the pure fallback-on-missing modes
    # above (see _pick_by_low_conf docstring) ---
    chosen_bits, chosen_missing = _pick_by_low_conf(
        v1_bits, v2_bits, v1_missing, v2_missing, v1_low_conf, v2_low_conf)
    rows, _, _, _, _ = compute_prbs_ber(
        chosen_bits, chosen_bits, chosen_missing, chosen_missing, pattern, BIT_SLICE, copy_priority="v1_only")
    results["low_conf_choice"] = dict(
        blended_all=_tally(rows, n_bits),
        blended_excl=_tally(rows, n_bits, exclude_mask=excl_mask),
        bit_all=_tally(rows, n_bits, exclude_missing=True),
        bit_excl=_tally(rows, n_bits, exclude_mask=excl_mask, exclude_missing=True),
    )

    # Baseline/comparisons use the TRUE bit-level rate (bit_all), not the
    # blended one - see _tally's docstring for why blending missing-as-
    # wrong into "BER" is misleading.
    baseline = max(results["v1_only"]["bit_all"][2], results["v2_only"]["bit_all"][2])
    print(f"\nPRBS ground-truth BER (baseline = worst single copy's TRUE bit-level BER, all samples):")
    if not has_low_conf:
        print(f"  (no low_conf_count signal in this .mat - low_conf_choice row below "
              f"degenerates to pure v1-priority, not real quality-aware arbitration)")
    print(f"  {'strategy':<15} {'missing':>12} {'bit errors/bits':>18} {'BIT BER':>10} "
          f"{'vs worst single':>16}   {'blended BER':>12} {'(incl. missing':>15} {'as all-wrong)':>14}")
    for lbl in ["v1_only", "v2_only", "v1->v2", "v2->v1", "low_conf_choice"]:
        r = results[lbl]
        bit_err, bit_bits, bit_ber, n_miss = r["bit_all"]
        _, _, blended_ber, _ = r["blended_all"]
        print(f"  {lbl:<15} {n_miss:>7}/{N:<4} {bit_err}/{bit_bits:>10} {bit_ber:>10.6f} "
              f"{_db(bit_ber, baseline):>16}   {blended_ber:>12.6f}")
    print(f"  {'  --- excluding chunk-edge + chain-slip samples ---':<15}")
    for lbl in ["v1_only", "v2_only", "v1->v2", "v2->v1", "low_conf_choice"]:
        r = results[lbl]
        bit_err, bit_bits, bit_ber, n_miss = r["bit_excl"]
        _, _, blended_ber, _ = r["blended_excl"]
        print(f"  {lbl:<15} {n_miss:>7}/{N:<4} {bit_err}/{bit_bits:>10} {bit_ber:>10.6f} "
              f"{_db(bit_ber, baseline):>16}   {blended_ber:>12.6f}")

    best_single = min(results["v1_only"]["bit_all"][2], results["v2_only"]["bit_all"][2])
    best_div = min(results["v1->v2"]["bit_all"][2], results["v2->v1"]["bit_all"][2])
    if best_single > 0 and best_div > 0:
        print(f"  Diversity gain over best single copy (true bit-level BER): "
              f"{10*math.log10(best_div/best_single):+.1f} dB")

    return dict(reader=reader, N=N, n_both_miss=n_both_miss,
                header_drops=reader.packet_sequence_header_drops,
                anomalies=reader.packet_sequence_anomaly_count,
                ber_cross=ber_cross, results=results, has_low_conf=has_low_conf,
                n_chunk_edge=n_chunk_edge, n_chain_slip=n_chain_slip,
                v1_rr=v1_rr, v2_rr=v2_rr, expected_n=expected_n,
                n_v1_present=n_v1_present, n_v2_present=n_v2_present)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mat_a", help="First model's run_sim_stream.m output .mat")
    parser.add_argument("mat_b", help="Second model's run_sim_stream.m output .mat")
    parser.add_argument("--labels", nargs=2, default=["Model A", "Model B"])
    parser.add_argument("--pattern-file", default=None,
                         help="Text file with PRBS pattern (0/1 chars) - default DEFAULT_PRBS_34")
    parser.add_argument("--classify", action="store_true",
                         help="Print a systematic per-sample classification of every missing/errored "
                              "sample (WINDOW_EDGE/USB_DROP/CHAIN_SLIP/CHUNK_EDGE/UNEXPLAINED) instead "
                              "of eyeballing timestamps by hand - see _classify_anomalies docstring")
    args = parser.parse_args()

    if args.pattern_file:
        text = open(args.pattern_file).read().strip().replace("\n", "").replace(" ", "")
        pattern = np.array([int(c) for c in text], dtype=np.uint8)
    else:
        pattern = DEFAULT_PRBS_34.copy()

    res_a = summarize(args.labels[0], args.mat_a, pattern, classify=args.classify)
    res_b = summarize(args.labels[1], args.mat_b, pattern, classify=args.classify)

    print(f"\n{'='*70}\nSIDE-BY-SIDE  ({args.labels[0]}  vs  {args.labels[1]})\n{'='*70}")
    print(f"{'':<28} {args.labels[0]:>18} {args.labels[1]:>18}")
    print(f"{'samples decoded':<28} {res_a['N']:>18} {res_b['N']:>18}")
    print(f"{'expected (200Hz fixed)':<28} {res_a['expected_n']:>18.0f} {res_b['expected_n']:>18.0f}")
    v1_rr_a_str = f"{res_a['n_v1_present']}/{res_a['expected_n']:.0f} ({100*res_a['v1_rr']:.1f}%)"
    v1_rr_b_str = f"{res_b['n_v1_present']}/{res_b['expected_n']:.0f} ({100*res_b['v1_rr']:.1f}%)"
    v2_rr_a_str = f"{res_a['n_v2_present']}/{res_a['expected_n']:.0f} ({100*res_a['v2_rr']:.1f}%)"
    v2_rr_b_str = f"{res_b['n_v2_present']}/{res_b['expected_n']:.0f} ({100*res_b['v2_rr']:.1f}%)"
    print(f"{'v1 RR':<28} {v1_rr_a_str:>18} {v1_rr_b_str:>18}")
    print(f"{'v2 RR':<28} {v2_rr_a_str:>18} {v2_rr_b_str:>18}")
    print(f"{'both-copies-missing':<28} {res_a['n_both_miss']:>18} {res_b['n_both_miss']:>18}")
    print(f"{'header drops':<28} {res_a['header_drops']:>18} {res_b['header_drops']:>18}")
    print(f"{'seq anomalies':<28} {res_a['anomalies']:>18} {res_b['anomalies']:>18}")
    print(f"{'cross-copy BER':<28} {res_a['ber_cross']:>18.6f} {res_b['ber_cross']:>18.6f}")
    print(f"{'chunk-edge samples':<28} {res_a['n_chunk_edge']:>18} {res_b['n_chunk_edge']:>18}")
    print(f"{'chain-slip samples':<28} {res_a['n_chain_slip']:>18} {res_b['n_chain_slip']:>18}")
    # TRUE bit-level BER (excludes missing samples entirely) is the
    # primary figure - compute_prbs_ber's blended metric (missing counted
    # as all-wrong) is shown separately, never as "BER" on its own, per
    # the user's 2026-07-25 correction.
    print("\n-- TRUE bit-level BER (missing samples excluded, not counted as wrong) --")
    for lbl in ["v1_only", "v2_only", "v1->v2", "v2->v1", "low_conf_choice"]:
        ber_a = res_a['results'][lbl]['bit_all'][2]
        ber_b = res_b['results'][lbl]['bit_all'][2]
        note = ""
        if lbl == "low_conf_choice" and not (res_a['has_low_conf'] and res_b['has_low_conf']):
            missing = [n for n, r in [(args.labels[0], res_a), (args.labels[1], res_b)] if not r['has_low_conf']]
            note = f"  (no low_conf_count: {', '.join(missing)})"
        print(f"{'bit BER (' + lbl + ')':<28} {ber_a:>18.6f} {ber_b:>18.6f}   {_db(ber_b, ber_a):>10} (B vs A){note}")
    print(f"\n{'  --- excluding chunk-edge + chain-slip samples too ---':<28}")
    for lbl in ["v1_only", "v2_only", "v1->v2", "v2->v1", "low_conf_choice"]:
        ber_a_ex = res_a['results'][lbl]['bit_excl'][2]
        ber_b_ex = res_b['results'][lbl]['bit_excl'][2]
        print(f"{'bit BER (' + lbl + ')':<28} {ber_a_ex:>18.6f} {ber_b_ex:>18.6f}   {_db(ber_b_ex, ber_a_ex):>10} (B vs A)")

    print("\n-- blended figure for reference (missing samples counted as fully wrong - "
          "NOT a bit error rate, see _tally's docstring) --")
    for lbl in ["v1_only", "v2_only", "v1->v2", "v2->v1", "low_conf_choice"]:
        ber_a = res_a['results'][lbl]['blended_all'][2]
        ber_b = res_b['results'][lbl]['blended_all'][2]
        print(f"{'blended (' + lbl + ')':<28} {ber_a:>18.6f} {ber_b:>18.6f}   {_db(ber_b, ber_a):>10} (B vs A)")


if __name__ == "__main__":
    main()
