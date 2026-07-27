#!/usr/bin/env python3
"""
plot_time_diversity_demo.py

Teaching figure: zooms tightly on a single interference episode in the
"interference_test" recording and traces it through ant1's signal-processing
CHAIN — raw IQ capture -> symbol-rate tap -> bit-slicer output -> packet
keep/miss outcome — so a reader can see exactly how one physical RF
disturbance turns into an actual packet loss, and how the TSBR link's
TIME-DIVERSITY scheme (every payload sent twice, offset in time as "copy 1" /
"copy 2") recovers it.

Data:
  raw_data/interference_test_ant1.bin (+ ant2)  - raw complex float32 IQ
    captures (see howload_raw_binfiles.txt), Fs = 8 MHz. Unaffected by the
    MAT regeneration below (same files throughout).
  simulink_outputs/stream_out_interference_ant1only.mat (+ ant2only)        -
    Simulink taps from the SAME recording, HDF5/v7.3 (must use h5py, not
    scipy.io.loadmat). Two groups of variables:
      - push1/databit1/changeofstrength1/packetnum1 (~100 kHz) - decoded via
        TimeStampBasedReader the same way as make_figures.py's
        decode_full_file(), driving the packet-status panel.
      - symbol_sink1/thresholded_signal (~400 kHz, i.e. 4x the ~100 kHz
        push1 rate) - two later Simulink taps plotted directly (no TSBR
        decode involved). A third tap, synced_data_centeredat0, also exists
        at the same rate but isn't used here.
  Each MAT also stores Fs and START_TIME (raw_file_time = decoded_domain_time
  + START_TIME).

Usage:
    python plot_time_diversity_demo.py
"""
import os, sys
import numpy as np
import h5py
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.patches import Rectangle
import matplotlib.patheffects as pe

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
import charfig_style as cfs
cfs.apply_style()

sys.path.insert(0, _HERE)
from sdr_reader_gcs_write import TimeStampBasedReader

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════
DATA_DIR      = os.path.join(_HERE, 'simulink_outputs')
RAW_DATA_DIR  = os.path.join(_HERE, 'raw_data')
PLOTS_DIR     = os.path.join(_HERE, 'plots')

GCS_CHANNEL = '2'

# push1/changeofstrength1/packetnum1's own rate: empirically ~100002 Hz on the
# original long capture (~20 ppm off this nominal value — negligible). Used as
# ground truth to derive the ACTUAL elapsed duration of whatever MAT is
# loaded (long or short), which in turn gives the true rate of every other tap
# in the same file — see decode_interference_file() / load_chain_taps().
PUSH_RATE_HZ = 100_000.0

# Primary antenna for every panel (isolates TIME diversity from antenna
# diversity — see plot_time_diversity_demo() INTENT).
DEMO_ANT = 'ant1'
MAT_BY_ANT = {'ant1': 'stream_out_interference_ant1only.mat',
              'ant2': 'stream_out_interference_ant2only.mat'}
BIN_BY_ANT = {'ant1': 'interference_test_ant1.bin',
              'ant2': 'interference_test_ant2.bin'}

# Zoom window in DECODED-domain seconds, relative to the MAT's own START_TIME
# (currently a short, low-drift 0.2s re-capture: START_TIME=30.2s absolute
# raw-file time). Widened from the original [0.125, 0.145] (one group cycle)
# to [0.105, 0.145] (TWO full 8-packet group cycles, since 0.125 also lands
# exactly on a fresh group boundary, 24 % 4 == 0) to give the packet-number
# color-coding in Panels 1-3 more than one cycle of context. The interference
# episode (Copy 1 slots 24-25 failing, raw/symbol/threshold disturbance
# ~0.1314-0.1334) sits in the second half of this window — see INTENT for why
# the ~6ms gap between Copy 1's failure and the visible disturbance is real
# pipeline latency, not misalignment.
ZOOM_LO_S, ZOOM_HI_S = 0.105, 0.145

# Manual per-panel time-offset knobs (seconds), added to each panel's own time
# axis AFTER its own rate/start-time derivation above. Each panel's time base
# comes from a different source (raw file sample index, chain-tap sample
# index, packet-decode sample index) and small residual misalignments between
# them are not fully pinned down by the derivations above — adjust these by
# hand to visually align a shared feature (e.g. the interference episode)
# across panels.
RAW_OFFSET_S   = -0.006   # Panel 1 (raw IQ)
CHAIN_OFFSET_S = -0.006   # Panels 2-3 (symbol tap / thresholded bits)
PKT_OFFSET_S   = 0   # Panel 4 (packet status)


# ══════════════════════════════════════════════════════════════════════════════
# Decode
# ══════════════════════════════════════════════════════════════════════════════

def _load_mat_fields(mat_path):
    with h5py.File(mat_path, 'r') as f:
        Fs = float(np.asarray(f['Fs']).ravel()[0])
        start_time = float(np.asarray(f['START_TIME']).ravel()[0])
        push      = np.asarray(f['push1']).ravel().astype(np.uint16)
        databit   = np.asarray(f['databit1']).ravel().astype(np.uint16)
        error     = np.asarray(f['changeofstrength1']).ravel().astype(np.uint16)
        packetnum = np.asarray(f['packetnum1']).ravel().astype(np.uint16)
    return Fs, start_time, push, databit, error, packetnum


def decode_interference_file(mat_path):
    """Mirrors make_figures.decode_full_file()'s word-stream reconstruction,
    but also returns the flattened per-output-sample quality codes (needed
    for the packet-status panel), the correct output rate, and a start-time
    OFFSET (in seconds) to add before building this panel's t axis.

    Two different rate/offset issues, needing two different fixes:

    1. reader.output_rate_hz is a fixed NOMINAL rate from protocol-timing
       constants only (bit_clock_hz / avg_frame_length in
       sdr_reader_gcs_write.py's __init__), not measured from this decode —
       over a multi-second decode the real clock drifts enough from that
       nominal value (~0.5-0.6% measured on an earlier, longer interference
       capture) to misalign the packet-status timeline from the raw IQ by
       tens of ms. Fix: compute the rate EMPIRICALLY as (decoded sample
       count) / (known elapsed duration from push1's own near-exact ~100kHz
       rate) — the ~/ueeg-pi/diversity_analysis.py approach.

    2. But that empirical estimator breaks down on a SHORT capture: the
       reader unconditionally discards its first decoded group as a
       startup artifact (sdr_reader_gcs_write.py's _first_group_skipped) —
       one group out of ~2940 is negligible drift-fitting noise on a 14.76s
       capture, but one group out of ~40 (this short 0.2s re-capture) is a
       real ~10% bias, not drift, and empirical_rate silently absorbs it as
       a WRONG rate (measured: 180.0 Hz instead of ~200 Hz here) rather than
       what it actually is: a fixed one-group time offset at the very start.
       Fix: when empirical and nominal rates disagree by more than a few
       percent (a real drift can't plausibly be that large — the long-file
       case above topped out at ~0.6%), trust nominal instead (drift over a
       short capture is negligible anyway) and add back the one skipped
       group as a constant start-time offset instead of folding it into the
       rate.
    """
    Fs, start_time, push, databit, error, packetnum = _load_mat_fields(mat_path)
    max_len = max(len(push), len(databit), len(error), len(packetnum))

    def _align(arr):
        if len(arr) == max_len:
            return arr
        ratio = round(max_len / len(arr))
        return np.repeat(arr, ratio)[:max_len]
    push, databit, error, packetnum = (_align(a) for a in (push, databit, error, packetnum))

    words = ((databit    & 0x1)
             | ((packetnum & 0x7) << 4)
             | ((error    & 0x1)  << 7)
             | ((push     & 0x1)  << 8)).astype(np.uint16)

    reader = TimeStampBasedReader(
        enable_gcs=False, quiet=True,
        gcs_channels=(1, 2, 3, 4),
        # 251 = a normal 250-word frame plus one duplicate idle word from
        # run_sim_stream.m's per-chunk sim() restart (lands in the idle tail
        # after the last push pulse, never in the payload) - a simulation
        # chunking artifact, not a real hardware frame length.
        accepted_frame_lengths=(250, 248, 251),
        frame_length_counts={250: 18, 248: 1},
    )
    reader.decode_from_word_stream(words)
    _values, quality = reader.get_decoded_arrays(GCS_CHANNEL)
    quality_series = quality.reshape(-1).astype(int)

    known_duration = max_len / PUSH_RATE_HZ
    empirical_rate = len(quality_series) / known_duration
    nominal_rate = reader.output_rate_hz

    if abs(empirical_rate - nominal_rate) / nominal_rate < 0.03:
        rate = empirical_rate
        t0_offset = 0.0
    else:
        rate = nominal_rate
        t0_offset = 1.0 / nominal_rate  # the one discarded startup group

    return Fs, start_time, quality_series, rate, t0_offset


def load_chain_taps(mat_path):
    """Loads the two later Simulink taps (symbol_sink1, thresholded_signal —
    ~400 kHz, 4x push1's ~100 kHz) and computes their rate the SAME way as
    decode_interference_file()'s packet-status rate: ground-truth elapsed
    duration from push1's own length, not any nominal/assumed constant. Only
    push1's LENGTH is needed here (not its data), so read via .shape."""
    with h5py.File(mat_path, 'r') as f:
        push_len = f['push1'].shape[-1]
        symbol_sink1 = np.asarray(f['symbol_sink1']).ravel().astype(float)
        thresholded  = np.asarray(f['thresholded_signal']).ravel().astype(np.uint8)
    known_duration = push_len / PUSH_RATE_HZ
    chain_rate = len(symbol_sink1) / known_duration
    return symbol_sink1, thresholded, chain_rate


def load_raw_iq_window(bin_path, t_lo_abs, t_hi_abs, fs):
    """Seeks directly into the raw complex-float32-interleaved IQ file (see
    howload_raw_binfiles.txt, ~2.7 GB each — do NOT read the whole file) and
    returns only the [t_lo_abs, t_hi_abs) slice (ABSOLUTE raw-file seconds).
    Returns (t_abs, iq_complex)."""
    n_lo = max(0, int(np.floor(t_lo_abs * fs)))
    n_hi = int(np.ceil(t_hi_abs * fs))
    bytes_per_sample = 2 * 4  # I (float32) + Q (float32)
    with open(bin_path, 'rb') as fh:
        fh.seek(n_lo * bytes_per_sample)
        raw = np.fromfile(fh, dtype=np.float32, count=(n_hi - n_lo) * 2)
    iq = raw[0::2] + 1j * raw[1::2]
    t_abs = (n_lo + np.arange(len(iq))) / fs
    return t_abs, iq


# ══════════════════════════════════════════════════════════════════════════════
# Figure
# ══════════════════════════════════════════════════════════════════════════════

# Panel 4 colors each segment by WHICH SLOT (1-4) it belongs to, not by
# usable/lost — Copy 1/Copy 2/Recovered live at genuinely different
# x-positions and widths (Recovered spans a full ~5ms output slot, Copy 1/
# Copy 2 each span a ~2.5ms packet within it), so matching the same slot
# across rows by POSITION doesn't work. Matching by COLOR does, since it
# doesn't depend on where or how wide the segment is. Picked as the four
# most mutually-distinct entries of the CVD-safe Okabe-Ito set, avoiding
# bluish_green/vermillion (freed up below for the usable/lost hatch encoding)
# and yellow (charfig_style.py already flags it as weak on white).
SLOT_COLORS = [cfs.OKABE_ITO['blue'], cfs.OKABE_ITO['orange'],
               cfs.OKABE_ITO['sky_blue'], cfs.OKABE_ITO['reddish_purple']]


def plot_time_diversity_demo(include_chain_taps=True):
    """INTENT
    Takeaway: one interference episode traced through FIVE successive stages
      of ant1's own signal-processing chain, on ONE shared time axis, so a
      reader can see exactly how a physical RF disturbance becomes an actual
      packet loss: (1) the raw capture where it appears as an amplitude
      excursion, (2) the symbol-rate tap where it appears as saturation,
      (3) the bit-slicer output where it appears as a stuck-high run instead
      of the normal toggle pattern, (4) the per-COPY packet outcome ("copy 1"
      / "copy 2", each sent separately, ~10ms apart), and (5) the RECOVERED
      per-slot outcome once TIME DIVERSITY (both copies' results combined)
      is applied. This is the figure that explains, to someone who has never
      seen this project, what actually happens inside the link during
      interference — not just that diversity helps, but where in the
      pipeline the damage occurs and where it gets undone.
    Encodings:
      - Panel 1 (raw IQ): |I+jQ| amplitude vs time, ant1's raw capture only
        (a single-pipeline story — showing ant2 here would reopen the
        antenna-diversity topic this figure isn't about)
      - Panel 2 (symbol tap): symbol_sink1, a continuous analog value —
        normal reception is a low, bursty duty cycle; during the episode it
        saturates to a sustained high plateau
      - Panel 3 (thresholded bits): thresholded_signal, the bit-slicer's
        binary output, shown as a black(1)/white(0) strip — normal reception
        is a busy fine-grained toggle; during the episode it locks to a
        solid black (stuck-high) block, visually distinct from the toggle
        texture on either side
      - Panel 4 (packet status, Copy 1 / Copy 2 only): each row is drawn at
        ITS OWN true packet time, not a shared slot timestamp — the two are
        genuinely different packets, exactly 4 inter-packet gaps (2
        output-sample periods, ~10ms) apart, per sdr_reader_gcs_write.py's
        group model. So each row is only lit during its own ~10ms half of
        each ~20ms group cycle and blank the rest of the time — the
        alternation IS the "sent twice, offset in time" mechanism made
        visible, not an artifact.
      - Panels 1-3 also carry the SAME slot color, extended up from Panels
        4-5: Panel 1's and Panel 2's traces are drawn as one colored segment
        PER PACKET (not one solid-color line) so a reader can see directly
        which stretch of raw/symbol signal came from which packet; Panel 3's
        black(1)/white(0) bits are left untouched (recoloring 0/1 pixels
        would destroy that encoding) and instead get a thin colored RULER
        strip drawn alongside them, same per-packet coloring, purely a
        visual echo of Panels 4-5's colors/timing at this higher point in
        the chain.
      - Panel 5 (Recovered): the logical OR of Copy 1/Copy 2, decided once
        per slot — kept on ITS OWN axis, separate from Panel 4, because it
        operates on a genuinely different time granularity: each segment
        spans a full ~5ms output slot (`dt`), not a ~2.5ms packet window
        (`packet_gap`) like Panel 4's rows. Squeezing Recovered into Panel 4
        as a third row (an earlier version of this figure did this) implied
        a shared timeline with Copy 1/Copy 2 that doesn't actually exist —
        their segments have different widths AND x-positions, so nothing
        about their on-screen position lines up, which reads as broken
        alignment even though the underlying data is correct.
      - Cross-row matching (Panel 4's two rows AND Panel 5) is done by
        COLOR, not position: every segment is filled by SLOT_COLORS[slot-1]
        (which of the 4 slots in the current 8-packet group it is), so the
        same hue in Copy 1, Copy 2, and Recovered means the same slot
        regardless of which panel it's in, or how wide/where that panel
        draws it. Usable/lost is a SEPARATE channel: solid fill = usable,
        dense black cross-hatch + black edge outline = lost — chosen so
        loss still reads instantly as "something's flagged here" regardless
        of which slot color it's drawn over (an earlier vermillion '///'
        hatch read poorly specifically against the orange slot color).
        Every segment also carries its slot's digit (1-4) as redundant
        coding on top of the color, for anyone who has trouble
        distinguishing the 4 hues.
      - The zoom window is deliberately exactly ONE 8-packet group cycle
        (~20ms) — with a repeating multi-group cycle, "slot 2 of group 3"
        would need a group index too; with only one group visible, the 1-4
        slot coloring/numbering is unambiguous without extra machinery
      - Explainer key panel (6th row, bottom): a schematic (not a data axis)
        with small drawn swatches teaching the encodings shared by Panels 4
        and 5 — the 4 slot colors, and solid-vs-hatched for usable-vs-lost —
        plus the blank "other copy's turn" state (Panel 4 only), so a reader
        can decode both panels without needing this docstring or a caption
      - All five data panels share one x-axis (decoded-domain seconds); the
        raw file's absolute time = this axis + START_TIME (in the MAT/caption)
      - Shared grey band (all panels): the exact window where a copy was
        actually lost per Panel 4 — the common reference tying the raw/
        symbol/threshold disruption (Panels 1-3) to its packet-level effect
    Explained IN-FIGURE (must appear in the legend/annotation; absence = defect):
      - Panel 4 row labels: Copy 1 / Copy 2; Panel 5 label: Recovered
      - The explainer key panel must show all FOUR slot colors (labeled 1-4)
        AND the solid-vs-hatched usable/lost encoding AND the blank "other
        copy's turn" state — three separate facts, all needed to decode
        Panels 4-5, none inferable from either panel alone
      - Axis labels naming each panel's signal (Raw IQ / Symbol tap /
        Thresholded bits / packet-status row labels)
    Deferred to CAPTION (OK to omit from the figure itself):
      - Fs / START_TIME and the raw-to-decoded time relationship
      - The shared grey band/guide-lines are a soft visual highlight (not a
        precise measurement) carrying the eye from the packet-loss window
        (Panel 4) up through the raw/symbol/threshold disruption it caused
        (Panels 1-3) — deliberately unlabeled in-figure, same treatment as
        other "carry the eye" reference bands used elsewhere in this
        project's figures
      - RAW_OFFSET_S / CHAIN_OFFSET_S / PKT_OFFSET_S (top of file) are manual
        per-panel time-offset knobs, not derived from any measurement —
        adjust by hand to visually align a shared feature across panels;
        current non-zero values reflect a manual visual alignment choice,
        not a claimed/validated physical latency figure
      - symbol_sink1 / thresholded_signal's exact DSP-stage identity
        (matched-filter/symbol-rate tap; post-bit-slicer comparator) is
        INFERRED from variable name, sample rate (4x push1), and value range
        only — no Simulink model file or documentation exists anywhere in
        this repo confirming that interpretation
      - Panel 3's black=1/white=0 convention (standard logic-trace polarity)
      - mismatch codes (5/6: both copies arrived but disagreed) are shown as
        "Recovered" usable (solid, not hatched) in Panel 5 since both did
        physically arrive
      - the ~6ms gap between Copy 1's failure (slots 24-25) and the raw/
        symbol/threshold disturbance's visible onset is real receive-chain
        latency, not a plotting misalignment — sdr_reader_gcs_write.py
        documents an analogous, larger (15ms) FPGA pipeline delay on the
        TRANSMIT side ("sample 0 was captured 15ms before packet 0 was
        sent", ~line 1803), so a smaller receive-side delay of this kind is
        plausible and consistent with a similar lead/lag pattern already
        observed (and accepted, not corrected) on a different episode
        earlier in this figure's development
      - Panel 4's Copy 1/Copy 2 packet placement anchors each 8-packet group
        to that group's own first slot in t_out, then lays out all 8 packets
        back-to-back at the real packet_gap pitch — this reproduces the true
        CONTINUOUS packet stream (confirmed: no gap within a copy's own
        4-packet block, a real ~10ms gap between the two copies' blocks) but
        is not independently verified against a literal per-packet timestamp
        (none is exposed by the reader; see project memory) — good enough for
        this figure's purpose, not a claim of sub-packet timing precision
      - the raw/symbol/threshold disruption (Panels 1-3, ~20ms wide) sits
        INSIDE the wider packet-loss window (Panel 4) rather than filling it
        exactly — packet loss is decided by higher-layer framing/CRC logic
        not directly visible in Panels 2-3
      - a second, leaner variant (`include_chain_taps=False`) drops Panels 2-3
        entirely (symbol tap / thresholded bits), keeping only raw IQ +
        packet status + Recovered — saved separately as
        time_diversity_demo_simple.png/.svg, not swapped in for the full
        version

    Stacked panels (5 when include_chain_taps=True, 3 when False) plus an
    explainer key, shared time axis, tightly zoomed on one interference
    episode traced through DEMO_ANT's signal-processing chain.
    """
    mat_path = os.path.join(DATA_DIR, MAT_BY_ANT[DEMO_ANT])
    Fs, start_time, quality_series, rate, t0_offset = decode_interference_file(mat_path)
    n = len(quality_series)
    t_out = t0_offset + PKT_OFFSET_S + np.arange(n) / rate

    base_q = quality_series & 0x07
    v1_ok = np.isin(base_q, (1, 3, 5, 6))
    v2_ok = np.isin(base_q, (2, 3, 5, 6))
    recovered_ok = base_q != 0

    dt = 1.0 / rate

    # Copy 1 and Copy 2 of the SAME slot are two DIFFERENT physical packets,
    # not simultaneous — sdr_reader_gcs_write.py's group model (output_rate_hz
    # = bit_clock_hz*4/(8*avg_frame_length)) puts one inter-packet gap at
    # exactly 0.5/rate seconds, with slot i's Copy 1 at packet_num=(i%4) and
    # Copy 2 at packet_num=(i%4)+4 within its 8-packet group — i.e. Copy 2
    # always follows Copy 1 by exactly 4 packet-gaps = 2/rate (~10ms at
    # ~200Hz). Plotting both at the slot's own t_out (as an earlier version
    # of this figure did) falsely implies they're simultaneous. Instead,
    # anchor each group's 8 packets to that group's own first slot (i%4==0)
    # in t_out and lay them out back-to-back at the real packet_gap pitch —
    # this reproduces the true continuous packet stream (no gaps within a
    # copy's own 4-packet block, a real ~10ms gap between the two blocks).
    packet_gap = 0.5 / rate
    group_start = t_out[(np.arange(n) // 4) * 4]
    slot_in_group = np.arange(n) % 4
    slot_number = slot_in_group + 1  # 1-4, the color/label key used everywhere below
    v1_time = group_start + slot_in_group * packet_gap
    v2_time = group_start + (4 + slot_in_group) * packet_gap

    # Shared shading band across all panels marking where a copy was actually
    # lost, so the eye can carry the "this is where it mattered" reference
    # down through the rest of the figure. Built from the true per-copy
    # packet windows above (tighter/more accurate than the old slot-level
    # +/-dt/2 approximation).
    #
    # Inclusion test MUST match _segments()'s below — a stricter
    # "t_out >= ZOOM_LO_S" test used previously silently dropped any packet
    # that starts just before ZOOM_LO_S but still overlaps the window (e.g.
    # slot 24 here: v1_time=0.12495, ZOOM_LO_S=0.125, but its packet runs to
    # 0.12745), so the band's left edge fell one slot short of where Copy 1's
    # hatching actually starts. Requires the overlap to be a real fraction of
    # the segment's own width (not just nonzero) so a segment that starts
    # exactly AT a zoom boundary (e.g. the next group's first packet landing
    # exactly at ZOOM_HI_S, a real occurrence since the window is exactly one
    # group cycle wide) doesn't count as "in the window" and draw/band a
    # hairline sliver.
    def _overlaps_zoom(times, width):
        overlap = np.minimum(times + width, ZOOM_HI_S) - np.maximum(times, ZOOM_LO_S)
        return overlap > 0.05 * width

    v1_in_zoom = _overlaps_zoom(v1_time, packet_gap)
    v2_in_zoom = _overlaps_zoom(v2_time, packet_gap)
    loss_bounds = [(v1_time[i], v1_time[i] + packet_gap)
                   for i in np.where(v1_in_zoom & ~v1_ok)[0]]
    loss_bounds += [(v2_time[i], v2_time[i] + packet_gap)
                    for i in np.where(v2_in_zoom & ~v2_ok)[0]]
    band = (min(b[0] for b in loss_bounds), max(b[1] for b in loss_bounds)) if loss_bounds else None

    # Packet-time "chunks" shared by every panel's slot-coloring — Panels 1-3
    # (below) recolor their traces using these, and Panel 4/5's _segments()
    # (further below) uses the identical _overlaps_zoom() test, so no panel
    # can ever disagree with another about where one packet ends and the next
    # begins. Used DIRECTLY, with NO further per-panel shift: RAW_OFFSET_S /
    # CHAIN_OFFSET_S already do the job of bringing t_raw / t_chain onto this
    # SAME shared packet-domain x-axis (that's the entire point of those
    # knobs, and why all panels can share one xlim/band at all) — applying a
    # SECOND shift here when overlaying chunk boundaries double-corrects and
    # throws the match off by roughly the offset's own size. Verified
    # empirically: with no extra shift, the raw/symbol disturbance's DISPLAYED
    # position lands inside slot 1's window almost exactly — i.e. the packet
    # that actually failed — confirming boundaries only need to be used
    # as-is, already resolved onto this shared axis, not converted again.
    _chunks = []
    for i in np.where(v1_in_zoom)[0]:
        _chunks.append((v1_time[i], v1_time[i] + packet_gap, SLOT_COLORS[slot_number[i] - 1]))
    for i in np.where(v2_in_zoom)[0]:
        _chunks.append((v2_time[i], v2_time[i] + packet_gap, SLOT_COLORS[slot_number[i] - 1]))
    _chunks.sort(key=lambda c: c[0])

    def _plot_chunks(ax, t, y, **plot_kw):
        """Draws one ax.plot() call per packet chunk instead of a single
        solid-color line, so the trace itself shows which packet produced
        each stretch of samples. Plain per-chunk plot(), not a per-sample
        LineCollection — there are only ~16-32 chunks across this zoom
        window regardless of how many raw samples each one contains (raw IQ
        alone is ~8MHz, hundreds of thousands of samples per chunk), so this
        stays cheap while a per-sample-segment LineCollection would bloat
        the SVG for zero visual gain (color is constant for tens of
        thousands of consecutive samples anyway)."""
        for c_lo, c_hi, color in _chunks:
            m = (t >= c_lo) & (t < c_hi)
            if m.any():
                ax.plot(t[m], y[m], color=color, **plot_kw)

    if include_chain_taps:
        symbol_sink1, thresholded_signal, chain_rate = load_chain_taps(mat_path)
        t_chain = CHAIN_OFFSET_S + np.arange(len(symbol_sink1)) / chain_rate
        zmask_chain = (t_chain >= ZOOM_LO_S) & (t_chain <= ZOOM_HI_S)

    bin_path = os.path.join(RAW_DATA_DIR, BIN_BY_ANT[DEMO_ANT])
    t_raw_abs, iq = load_raw_iq_window(
        bin_path, start_time + ZOOM_LO_S - RAW_OFFSET_S,
        start_time + ZOOM_HI_S - RAW_OFFSET_S, fs=Fs)
    t_raw = t_raw_abs - start_time + RAW_OFFSET_S
    raw_amp = np.abs(iq)

    if include_chain_taps:
        fig, axes = plt.subplots(
            6, 1, figsize=(7.5, 8.7),
            gridspec_kw={'height_ratios': [1.0, 0.9, 0.4, 0.7, 0.4, 0.35]},
            constrained_layout=True)
        ax_raw, ax_sym, ax_thr, ax_pkt, ax_rec, ax_key = axes
        data_axes = (ax_raw, ax_sym, ax_thr, ax_pkt, ax_rec)
    else:
        fig, axes = plt.subplots(
            4, 1, figsize=(7.5, 5.4),
            gridspec_kw={'height_ratios': [1.0, 0.7, 0.4, 0.35]},
            constrained_layout=True)
        ax_raw, ax_pkt, ax_rec, ax_key = axes
        ax_sym = ax_thr = None
        data_axes = (ax_raw, ax_pkt, ax_rec)
    for ax in data_axes[1:]:
        ax.sharex(ax_raw)

    # ── Panel 1: raw IQ amplitude, ant1 only ───────────────────────────────
    # Colored per-packet (see _plot_chunks) rather than one solid line, so
    # this trace directly shows which packet produced each stretch of signal
    # — the same SLOT_COLORS scheme as Panels 4-5, extended up to the raw
    # capture.
    _plot_chunks(ax_raw, t_raw, raw_amp, lw=0.4)
    ax_raw.set_ylabel('Raw |IQ|\n[a.u.]')
    title = (f"Interference episode traced through {DEMO_ANT}'s signal chain: "
             f"raw capture → symbol tap → bit slicer → packet outcome"
             if include_chain_taps else
             f"Interference episode traced through {DEMO_ANT}'s signal chain: "
             f"raw capture → packet outcome")
    fig.suptitle(title)

    if include_chain_taps:
        # ── Panel 2: symbol-rate tap (symbol_sink1) ────────────────────────
        _plot_chunks(ax_sym, t_chain, symbol_sink1, lw=0.5)
        ax_sym.set_ylabel('Symbol tap\n[a.u.]')

        # ── Panel 3: bit-slicer output (thresholded_signal) ────────────────
        # Extent uses the ZOOM bounds directly (not the data's own half-slot
        # edges) so the strip fills the panel flush to both edges — with a
        # coarser sample spacing (Panel 4) the half-slot convention can leave
        # a visible white gap between the axis edge and the first/last cell.
        # Bits stay black(1)/white(0) — recoloring the pixel VALUES would
        # destroy that encoding — so the packet-color scheme instead gets a
        # thin RULER strip in its own band just above the bits, sharing this
        # same axis (ylim widened to fit both bands).
        extent_th = (ZOOM_LO_S, ZOOM_HI_S, -0.5, 0.5)
        cmap_th = ListedColormap(['white', 'black'])
        ax_thr.imshow(thresholded_signal[zmask_chain][None, :], aspect='auto', cmap=cmap_th,
                      vmin=0, vmax=1, extent=extent_th, origin='lower',
                      interpolation='nearest', zorder=1)
        ruler_xs = [(c_lo, c_hi - c_lo) for c_lo, c_hi, _ in _chunks]
        ruler_colors = [c for _, _, c in _chunks]
        ax_thr.broken_barh(ruler_xs, (0.5, 0.28), facecolors=ruler_colors, zorder=2)
        ax_thr.set_ylim(-0.5, 0.78)
        ax_thr.set_yticks([])
        ax_thr.set_ylabel('Thresholded\nbits')

    # ── Panel 4: packet status (Copy 1 / Copy 2) ───────────────────────────
    # Copy 1 and Copy 2 get their OWN true packet windows (v1_time/v2_time,
    # width=packet_gap), not the slot's shared t_out — this is what actually
    # shows the "sent twice, offset in time" mechanism: each row is only lit
    # during its own copy's ~half-group window and blank (background) during
    # the other copy's window, alternating every packet_gap*4 (~10ms).
    # Recovered (the logical OR, decided once per slot, full slot width) is
    # NOT drawn on this axis — it lives on its own Panel 5 (ax_rec) below,
    # since it operates on a genuinely different time granularity (once per
    # SLOT, not once per PACKET) and stacking it into the same axis as
    # Copy 1/Copy 2 implied a shared timeline between them that doesn't
    # actually exist.

    def _segments(times, width, ok_mask):
        """Returns (usable_xs, usable_colors, lost_xs, lost_colors, all_xs, all_labels)
        for one row, split by usable/lost so each half can get its own hatch.
        Uses the SAME _overlaps_zoom() overlap test as the band computation
        above, so what's drawn here and what the band covers never disagree
        (see the note by _overlaps_zoom's definition)."""
        m = _overlaps_zoom(times, width)
        t_m, ok_m, lbl_m = times[m], ok_mask[m], slot_number[m]
        u_xs = [(t, width) for t, ok in zip(t_m, ok_m) if ok]
        u_colors = [SLOT_COLORS[s - 1] for s, ok in zip(lbl_m, ok_m) if ok]
        l_xs = [(t, width) for t, ok in zip(t_m, ok_m) if not ok]
        l_colors = [SLOT_COLORS[s - 1] for s, ok in zip(lbl_m, ok_m) if not ok]
        all_xs = [(t, width) for t in t_m]
        return u_xs, u_colors, l_xs, l_colors, all_xs, lbl_m

    v1_u_xs, v1_u_c, v1_l_xs, v1_l_c, v1_xs, v1_labels = _segments(v1_time, packet_gap, v1_ok)
    v2_u_xs, v2_u_c, v2_l_xs, v2_l_c, v2_xs, v2_labels = _segments(v2_time, packet_gap, v2_ok)
    rec_u_xs, rec_u_c, rec_l_xs, rec_l_c, rec_xs, rec_labels = _segments(t_out, dt, recovered_ok)

    # Color = which slot (1-4) — the mechanism for cross-row matching, since
    # position doesn't work (see SLOT_COLORS comment above). Hatch = lost,
    # solid = usable: drawn as two broken_barh calls per row so only the
    # "lost" half gets the hatch: keeps a fast "something went wrong here"
    # read without needing red/green as the base fill. A single vermillion
    # '///' hatch (the original choice) was hard to read specifically over
    # the orange slot color (both mid-saturation, adjacent hues) — denser
    # 'xxxx' cross-hatch plus a solid black edge outline reads clearly
    # against all 4 slot colors, not just the ones vermillion contrasted
    # well against.
    hatch_kw = dict(hatch='xxxx', edgecolor='black', linewidth=0.6)
    label_effect = [pe.withStroke(linewidth=1.8, foreground='white')]

    def _draw_row(ax, y0, u_xs, u_c, l_xs, l_c, xs, labels, y_center):
        if u_xs:
            ax.broken_barh(u_xs, (y0, 1.0), facecolors=u_c)
        if l_xs:
            ax.broken_barh(l_xs, (y0, 1.0), facecolors=l_c, **hatch_kw)
        for (x0, width), lbl in zip(xs, labels):
            ax.text(x0 + width / 2, y_center, str(lbl), color='black',
                     fontsize=7, ha='center', va='center', zorder=4,
                     path_effects=label_effect, clip_on=True)

    _draw_row(ax_pkt, -0.5, v1_u_xs, v1_u_c, v1_l_xs, v1_l_c, v1_xs, v1_labels, 0.0)
    _draw_row(ax_pkt, 0.5, v2_u_xs, v2_u_c, v2_l_xs, v2_l_c, v2_xs, v2_labels, 1.0)
    ax_pkt.set_yticks([0, 1])
    ax_pkt.set_yticklabels(['Copy 1', 'Copy 2'])
    ax_pkt.set_ylim(-0.5, 1.5)
    ax_pkt.axhline(0.5, color='white', lw=1.0)

    # ── Panel 5: Recovered (logical OR, decided once per output slot) ──────
    # Its own axis, not a third row squeezed into Panel 4 — see the note by
    # ax_pkt above for why: Recovered's segments are ~5ms wide (one full
    # output slot, `dt`) vs Copy 1/Copy 2's ~2.5ms (`packet_gap`), a real
    # granularity difference, not just a drawing choice.
    _draw_row(ax_rec, -0.5, rec_u_xs, rec_u_c, rec_l_xs, rec_l_c, rec_xs, rec_labels, 0.0)
    ax_rec.set_yticks([0])
    ax_rec.set_yticklabels(['Recovered'])
    ax_rec.set_ylim(-0.5, 0.5)
    ax_rec.set_xlabel('Time relative to interference-test decode start [s]')

    # Explainer key panel (ax_key) — a schematic, not a data axis, teaching
    # both Panel 4 encodings: which color = which slot, and solid-vs-hatched
    # = usable-vs-lost. Replaces a plain color legend because Panel 4 uses
    # TWO independent visual channels (hue + hatch) that a simple handle list
    # can't demonstrate as clearly as small drawn swatches can.
    ax_key.axis('off')

    def _swatch(x, y, w, h, label, **rect_kw):
        ax_key.add_patch(Rectangle((x, y), w, h, transform=ax_key.transAxes,
                                    clip_on=False, **rect_kw))
        ax_key.text(x + w + 0.01, y + h / 2, label, transform=ax_key.transAxes,
                    ha='left', va='center', fontsize=8)

    sw, sh = 0.03, 0.28
    # Slot-color swatches, labeled with the same digits drawn on the segments.
    # Same colors also recolor Panels 1's/2's traces and Panel 3's ruler
    # strip (when present) — one shared scheme top to bottom, so a single
    # key line covers all of it.
    slot_key_label = ('slot in group (also colors traces/ruler above):'
                       if include_chain_taps else
                       'slot in group (also colors the raw trace above):')
    ax_key.text(0.0, 0.95, slot_key_label, transform=ax_key.transAxes,
                ha='left', va='center', fontsize=8, fontweight='bold')
    x0 = 0.02
    for i, color in enumerate(SLOT_COLORS):
        _swatch(x0, 0.62, sw, sh, str(i + 1), facecolor=color)
        x0 += 0.09
    # Usable / lost (hatch) swatches.
    ax_key.text(0.0, 0.45, 'status:', transform=ax_key.transAxes,
                ha='left', va='center', fontsize=8, fontweight='bold')
    _swatch(0.02, 0.05, sw, sh, 'usable', facecolor='0.75')
    _swatch(0.20, 0.05, sw, sh, 'lost', facecolor='0.75', **hatch_kw)
    _swatch(0.38, 0.05, sw, sh, "other copy's turn", facecolor='white',
            edgecolor='0.5', linewidth=0.6)

    # Shared band marking the exact copy-loss window, carried up through
    # Panels 1-3 as a common reference (see INTENT). NOT drawn as a tint on
    # Panels 4-5: a translucent overlay there would render the "other copy's
    # turn" blank state in two different shades (white outside the band,
    # light grey inside it) — a review caught this reading as a fourth,
    # unexplained state. Panels 4-5 get thin guide lines at the same x
    # positions instead, which cross-references the window without
    # recoloring any of its cells.
    for ax in data_axes:
        ax.set_xlim(ZOOM_LO_S, ZOOM_HI_S)
        if band is not None:
            if ax in (ax_pkt, ax_rec):
                for x in band:
                    ax.axvline(x, color='0.4', lw=0.8, ls=(0, (2, 1)), zorder=3)
            else:
                ax.axvspan(band[0], band[1], color='0.6', alpha=0.15, zorder=2, lw=0)
        cfs.style_axes(ax)

    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(PLOTS_DIR, exist_ok=True)

    print('Building time-diversity demo figure (full) ...')
    fig = plot_time_diversity_demo(include_chain_taps=True)
    cfs.save_fig(fig, os.path.join(PLOTS_DIR, 'time_diversity_demo'))
    print(f'Saved time_diversity_demo.png/.svg -> {PLOTS_DIR}')

    print('Building time-diversity demo figure (simple, no chain taps) ...')
    fig_simple = plot_time_diversity_demo(include_chain_taps=False)
    cfs.save_fig(fig_simple, os.path.join(PLOTS_DIR, 'time_diversity_demo_simple'))
    print(f'Saved time_diversity_demo_simple.png/.svg -> {PLOTS_DIR}')

    plt.show()


if __name__ == '__main__':
    main()
