#!/usr/bin/env python3
"""
make_figures.py  (self-contained bundle)

Loads the three long MAT recordings in ./data/ (one per antenna config),
runs TSBR decode, computes sliding-window reception rates, and produces:

  Figure A — walk-path diagram, path colored by local time-diversity rate
  Figure B — reception rate vs. distance from the receiver (zone bars)

Each figure is written to ./plots/ as BOTH .png and .svg.

Figure-intent convention: every plotting function opens its docstring with an
`INTENT` block — the single source of truth for what the figure is for. It records
the takeaway, what each visual encoding means, which encodings must be explained
IN the figure (legend/annotation; their absence is a defect), and which are
deferred to the caption (and so are OK to omit from the figure). Readers and the
figure-review agents read this instead of reverse-engineering intent.

Usage:
    python make_figures.py

Everything needed lives in this folder: the decoder (sdr_reader_gcs_write.py)
and the recordings (data/stream_out_*_wholefile.mat). Adjust WALK_START_S,
WALK_END_S, and WALK_CORNER_TIMES_7 below to match the walk timing.
"""

import os, sys
import numpy as np
import scipy.io
import matplotlib.pyplot as plt
import matplotlib.collections as mc
import matplotlib.colors as mcolors
import matplotlib.patheffects as mpe
from matplotlib.ticker import MaxNLocator

# ── Composite-panel typography ────────────────────────────────────────────────
# These figures are embedded as single ~PANEL_H_IN panels in a multi-panel
# figure. Hold every text element at the shared FONT_PT (set absolutely, since
# matplotlib points don't scale with figsize). Edit charfig_style.py to retune.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import charfig_style as cfs
cfs.apply_style()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

# Long MAT files (one per antenna configuration) covering the full walk.
# Bundled alongside this script in ./data/.  Set a path to None to skip a config.
_HERE     = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(_HERE, 'simulink_outputs')
# MAT_ANT1 = os.path.join(_DATA_DIR, 'stream_out_longcircle_ant1only.mat')
# MAT_ANT2 = os.path.join(_DATA_DIR, 'stream_out_longcircle_ant2only.mat')
# MAT_BOTH = os.path.join(_DATA_DIR, 'stream_out_both_longcircle_wholefile.mat')
MAT_ANT1 = os.path.join(_DATA_DIR, 'stream_out_ant1only_wholefile.mat')
MAT_ANT2 = os.path.join(_DATA_DIR, 'stream_out_ant2only_wholefile.mat')
MAT_BOTH = os.path.join(_DATA_DIR, 'stream_out_both_wholefile.mat')
# Companion walk video: only a fallback wall-clock anchor for the (now unused)
# q-value CSV. Not needed for the figures, so disabled in this bundle.
WALK_VIDEO = None

GCS_CHANNEL = '2'

# Sliding window: number of TSBR output samples (~200 Hz) to average over.
# 150 ≈ 0.75 s ≈ 0.5–1 m of walk.  Increase for smoother, decrease for sharper.
WINDOW_SAMPLES = 300

# Floor for the log-scaled error-rate axis in plot_rate_vs_distance(). A window
# can show exactly zero error (every sample in it decoded), but a WINDOW_SAMPLES
# -sample window can't resolve error finer than half a sample's weight — clip
# to that floor (a FRACTION, not a percent — the axis is a probability, the
# conventional way to present an error/bit-error rate) instead of letting
# zero-error windows break the log scale.
ERROR_FLOOR = 0.5 / WINDOW_SAMPLES

# Recording time (seconds) at the start and end of the walk
WALK_START_S = 0.0
WALK_END_S   = None   # None → use full recording duration

# Recording times at the 7 intermediate path corners (seconds), in order:
#   BR→BL,  BL→TL,  TL→TR,  TR→BR,  BR→TR,  TR→TL,  TL→BL
# Set any unknown corner to None for arc-proportional interpolation.
WALK_CORNER_TIMES_7 = [i-25 for i in [37, 46, 58, 64, 71, 82, 90]]

# Room geometry (feet → metres)
W_M  = 48 * 0.3048
H_M  = 28 * 0.3048
RX_M = (W_M / 2, 3 * 0.3048)

# Lateral separation (m) between the two laps' device positions. This is PHYSICAL,
# not cosmetic: the antenna is head-mounted, so walking one direction vs. the other
# places the head ~1 foot to the side — the two laps sample genuinely different
# positions. ≈ 1 ft = 0.3048 m.
DELTA_PATH = 0.3048

# Path colormap for reception rate — the shared CVD-safe sequential map
# (viridis): perceptually uniform, optimised for colour-vision deficiency, and
# greyscale-safe (unlike RdYlGn).
PATH_CMAP = plt.get_cmap(cfs.SEQUENTIAL_CMAP)

# Overlay colours — warm Okabe–Ito hues, all OFF the viridis colormap (which
# spans blue→green→yellow) so direction arrows / landmarks never read as a
# reception value. Marker SHAPE (arrow / diamond / star) is the primary cue;
# colour is secondary, so these stay within the shared colour-blind-safe scheme.
CW_COLOR    = cfs.OKABE_ITO['vermillion']      # Lap 1, clockwise
CCW_COLOR   = cfs.OKABE_ITO['reddish_purple']  # Lap 2, counter-clockwise
START_COLOR = 'white'                          # Start/End diamond (black-edged; blends w/ viridis otherwise)
RX_COLOR    = cfs.OKABE_ITO['orange']          # Rx star

# Distance zones for Figure 3 (rate vs distance-from-Rx). None → auto quantile
# bands (~equal sample count each). Set explicit metre edges for physically
# meaningful bands, e.g. [0, 3, 6, 9, 16].
DISTANCE_ZONE_EDGES = [0,2.5,5,7.5,10]
N_DISTANCE_ZONES    = 5   # number of auto zones when DISTANCE_ZONE_EDGES is None

LABELS   = ['Ant 1 only', 'Ant 2 only', 'Both antennas']

# Every figure is saved here (PNG) for review before you choose which to keep.
PLOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'plots')

# Decoder quality base-code (quality & 0x07) → human-readable word for the CSV.
#   0 none | 1 only_v1 | 2 only_v2 | 3 both (copies matched)
#   5 mismatch_v1 / 6 mismatch_v2 (copies disagreed; decoder picked v1 / v2).
# The chosen copy is denoted so the wrong-copy half of a mismatch can be
# attributed (a mismatch is 50% correct under no-diversity).
# Bit 3 (0x08) is a separate error flag and is stripped before this lookup.
# Codes 4 and 7 are never produced by the decoder.
Q_WORDS = {0: 'none', 1: 'only_v1', 2: 'only_v2', 3: 'both',
           5: 'mismatch_v1', 6: 'mismatch_v2'}


# ══════════════════════════════════════════════════════════════════════════════
# Path geometry helpers  (kept in sync with survey_diversity_plot.py)
# ══════════════════════════════════════════════════════════════════════════════

def _perimeter_arc_anchors(W, H):
    return np.array([0, W, W+H, 2*W+H, 2*(W+H),
                     2*W+3*H, 3*W+3*H, 3*W+4*H, 4*(W+H)])


def _perimeter_xy(s, W, H):
    H, W, s = float(H), float(W), float(s)
    if s <= W:               return (W - s, 0.0)
    elif s <= W + H:         return (0.0, s - W)
    elif s <= 2*W + H:       return (s - W - H, H)
    elif s <= 2*(W + H):     return (W, H - (s - 2*W - H))
    elif s <= 2*W + 3*H:     return (W, s - 2*(W + H))
    elif s <= 3*W + 3*H:     return (W - (s - 2*W - 3*H), H)
    elif s <= 3*W + 4*H:     return (0.0, H - (s - 3*W - 3*H))
    else:                    return (s - 3*W - 4*H, 0.0)


def _fill_corner_times(t_waypoints_raw, W, H):
    arcs   = _perimeter_arc_anchors(W, H)
    t_wpts = list(t_waypoints_raw)
    known  = [(i, float(t_wpts[i])) for i in range(len(t_wpts))
              if t_wpts[i] is not None]
    for k in range(len(known) - 1):
        lo_i, t_lo = known[k]
        hi_i, t_hi = known[k + 1]
        arc_span = arcs[hi_i] - arcs[lo_i]
        for i in range(lo_i + 1, hi_i):
            frac = (arcs[i] - arcs[lo_i]) / arc_span
            t_wpts[i] = t_lo + frac * (t_hi - t_lo)
    return t_wpts


def _position_and_arc(t, t_waypoints_raw, W, H):
    t_wpts = _fill_corner_times(t_waypoints_raw, W, H)
    arcs   = _perimeter_arc_anchors(W, H)
    t = max(float(t_wpts[0]), min(float(t), float(t_wpts[-1])))
    for i in range(len(t_wpts) - 1):
        dt = float(t_wpts[i + 1]) - float(t_wpts[i])
        if t <= float(t_wpts[i + 1]) + 1e-9:
            frac = (t - float(t_wpts[i])) / dt if dt > 0 else 0.0
            s = float(arcs[i]) + frac * (float(arcs[i + 1]) - float(arcs[i]))
            return _perimeter_xy(s, W, H), s
    s = float(arcs[-1])
    return _perimeter_xy(s, W, H), s


def _path_xy_offset(s, W, H, delta):
    """W×H are the given path dimensions (outer CCW loop).
    Lap 1 CW: delta inside the W×H boundary.
    Lap 2 CCW: exactly at the W×H boundary."""
    H, W, s, delta = float(H), float(W), float(s), float(delta)
    perimeter = 2 * (W + H)
    # Lap 1 CW — inner path (exact corners at delta inside W×H)
    if s <= W:               return (W - delta - s * (W - 2*delta) / W,           delta)
    elif s <= W + H:         return (delta,                                         delta + (s - W) * (H - 2*delta) / H)
    elif s <= 2*W + H:       return (delta + (s - W - H) * (W - 2*delta) / W,     H - delta)
    elif s <= perimeter:     return (W - delta,                                     H - delta - (s - 2*W - H) * (H - 2*delta) / H)
    # Lap 2 CCW — outer path at exactly W×H (no offset)
    elif s <= 2*W + 3*H:     return (W,                   s - perimeter)
    elif s <= 3*W + 3*H:     return (W - (s - 2*W - 3*H), H)
    elif s <= 3*W + 4*H:     return (0.0,                  H - (s - 3*W - 3*H))
    else:                    return (s - 3*W - 4*H,        0.0)


def _save_all_figures(out_dir):
    """Save every open matplotlib figure to out_dir as BOTH .png and .svg,
    named from its suptitle so they are easy to pick through afterward."""
    import re
    os.makedirs(out_dir, exist_ok=True)
    saved = []
    for num in plt.get_fignums():
        fig = plt.figure(num)
        title = (fig.get_suptitle() or '').strip()
        slug = re.sub(r'[^\w\-]+', '_', title).strip('_')[:60]
        name = f'fig{num:02d}_{slug}' if slug else f'fig{num:02d}'
        for ext, kw in (('png', {'dpi': 300}), ('svg', {})):
            path = os.path.join(out_dir, f'{name}.{ext}')
            fig.savefig(path, bbox_inches='tight', **kw)
            saved.append(os.path.basename(path))
    print(f'\nSaved {len(saved)} figure file(s) to {out_dir}/')
    for nm in saved:
        print(f'  {nm}')
    return saved


def _add_path_arrow(ax, p0, p1, color, frac=0.4):
    """Stemless direction marker: a filled triangle (no shaft) pointing along
    p0→p1, so direction is conveyed while covering as little of the colored
    path as possible. White edge keeps it legible over any background colour."""
    p0, p1 = np.asarray(p0, float), np.asarray(p1, float)
    d   = p1 - p0
    mid = p0 + frac * d
    # Regular-triangle marker (3 sides) points +y at angle 0; rotate to heading.
    angle = np.degrees(np.arctan2(d[1], d[0])) - 90.0
    ax.scatter([mid[0]], [mid[1]], marker=(3, 0, angle), s=22,
               facecolor=color, edgecolor='white', linewidths=0.5, zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
# Data loading & decoding
# ══════════════════════════════════════════════════════════════════════════════

def _load_mat(filepath, varnames):
    try:
        d = scipy.io.loadmat(filepath, squeeze_me=True, variable_names=list(varnames))
        return {k: d[k] for k in varnames if k in d}
    except NotImplementedError:
        pass
    except Exception as exc:
        raise RuntimeError(f'scipy.io.loadmat failed: {exc}') from exc
    try:
        import h5py
    except ImportError as exc:
        raise RuntimeError('Mat file is MATLAB v7.3 (HDF5) but h5py is not installed. '
                           'Install with: pip install h5py') from exc
    result = {}
    with h5py.File(filepath, 'r') as f:
        for k in varnames:
            if k not in f:
                continue
            raw = f[k][()]
            result[k] = raw.ravel().astype(np.float64) if raw.ndim > 0 else float(raw)
    return result


def _read_wall_times(filepath):
    """Read the WALL_START / WALL_END string vars from a MAT file.

    Handles both classic (scipy) and v7.3/HDF5 (h5py, where char arrays are
    uint16 code points) layouts. Returns (start, end) as datetime objects, or
    None for any that is missing/unparseable. Format: 'YYYY-MM-DD HH:MM:SS'.
    """
    from datetime import datetime
    raw = {}
    try:
        d = scipy.io.loadmat(filepath, squeeze_me=True,
                             variable_names=['WALL_START', 'WALL_END'])
        for k in ('WALL_START', 'WALL_END'):
            if k in d:
                raw[k] = d[k]
    except NotImplementedError:
        import h5py
        with h5py.File(filepath, 'r') as f:
            for k in ('WALL_START', 'WALL_END'):
                if k in f:
                    raw[k] = ''.join(chr(int(c)) for c in np.ravel(f[k][()]))
    except Exception:
        return None, None

    def _parse(k):
        if k not in raw:
            return None
        v = raw[k]
        if isinstance(v, np.ndarray):
            v = v.item() if v.size == 1 else ''.join(map(str, v.tolist()))
        s = str(v).strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f'  WARN: could not parse {k}={s!r} (expected "YYYY-MM-DD HH:MM:SS")')
            return None

    return _parse('WALL_START'), _parse('WALL_END')


def _video_start_time(video_path):
    """Best-effort wall-clock start time of a video, as a local naive datetime.

    Prefers ffprobe's creation_time (the recording start; converted from UTC to
    local). Falls back to file mtime minus the video duration (mtime ≈ when the
    file was finalized = end of recording), then to bare mtime. Returns None if
    the path is missing or nothing can be determined.
    """
    import subprocess, json
    from datetime import datetime, timedelta
    if not video_path or not os.path.exists(video_path):
        if video_path:
            print(f'  Video fallback path not found: {video_path}')
        return None

    creation = duration = None
    try:
        out = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
             '-show_entries', 'format_tags=creation_time:format=duration',
             video_path],
            capture_output=True, text=True, timeout=30)
        if out.returncode == 0 and out.stdout.strip():
            fmt = json.loads(out.stdout).get('format', {})
            if fmt.get('duration'):
                duration = float(fmt['duration'])
            ct = (fmt.get('tags') or {}).get('creation_time')
            if ct:
                dt = datetime.fromisoformat(ct.replace('Z', '+00:00'))
                creation = dt.astimezone().replace(tzinfo=None) if dt.tzinfo else dt
    except (FileNotFoundError, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
        pass  # ffprobe absent or metadata unreadable — fall through to mtime

    base = os.path.basename(video_path)
    if creation is not None:
        print(f'  Video start (ffprobe creation_time): {creation:%Y-%m-%d %H:%M:%S}  [{base}]')
        return creation
    mtime = datetime.fromtimestamp(os.path.getmtime(video_path))
    if duration is not None:
        start = mtime - timedelta(seconds=duration)
        print(f'  Video start (mtime − {duration:.0f}s duration): {start:%Y-%m-%d %H:%M:%S}  [{base}]')
        return start
    print(f'  Video start (mtime, approximate): {mtime:%Y-%m-%d %H:%M:%S}  [{base}]')
    return mtime


def save_quality_timeseries(mat_path, quality, output_rate, video_path=None):
    """Write a CSV of per-sample quality bytes with wall-clock timestamps.

    Timestamps are anchored at WALL_START (read from the MAT) plus the decoded
    elapsed time (sample_index / output_rate) — WALL_END is used only as a
    sanity check, since the recording may have been stopped slightly early.
    If the MAT has no WALL_START, falls back to the companion video's start time.
    Saved next to the MAT file as <matbasename>_qvalues.csv.
    """
    wall_start, wall_end = _read_wall_times(mat_path)
    anchor_source = 'WALL_START'
    if wall_start is None and video_path is not None:
        wall_start = _video_start_time(video_path)
        anchor_source = 'video'
    n = len(quality)
    rel = np.arange(n) / float(output_rate)

    if wall_start is not None:
        base = np.datetime64(wall_start.strftime('%Y-%m-%dT%H:%M:%S'))
        ts = base + np.round(rel * 1000).astype('timedelta64[ms]')
        # datetime64 renders ISO with a 'T'; use a space to match WALL_START format.
        wall_strs = np.char.replace(ts.astype('datetime64[ms]').astype(str), 'T', ' ')
        computed_end = wall_start.strftime('%Y-%m-%d %H:%M:%S') if n == 0 else \
            str(ts[-1].astype('datetime64[s]')).replace('T', ' ')
        note = f'anchor={anchor_source}({wall_start:%Y-%m-%d %H:%M:%S})  computed_end={computed_end}'
        if anchor_source == 'WALL_START' and wall_end is not None:
            note += f'  WALL_END={wall_end:%Y-%m-%d %H:%M:%S}'
    else:
        wall_strs = np.full(n, '', dtype=object)
        note = 'no WALL_START and no video anchor — wall_time column left blank'

    # Strip the error flag (bit 3) before mapping the base reception code.
    q_words = np.array([Q_WORDS.get(int(q) & 0x07, f'q{int(q) & 0x07}') for q in quality],
                       dtype=object)
    out_path = os.path.splitext(mat_path)[0] + '_qvalues.csv'
    arr = np.column_stack([
        np.asarray(wall_strs, dtype=object),
        np.round(rel, 4).astype(str),
        q_words,
    ])
    np.savetxt(out_path, arr, fmt='%s', delimiter=',',
               header='wall_time,rel_s,q', comments='')
    print(f'  Saved {n} q-values → {out_path}  ({note})')
    return out_path


def decode_full_file(mat_path):
    """
    Load the long MAT, reconstruct the word stream, run TSBR decode.
    Returns (quality, output_rate_hz) where quality is a per-sample int array.
    """
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from sdr_reader_gcs_write import TimeStampBasedReader

    reader = TimeStampBasedReader(
        enable_gcs=False,
        quiet=True,  # suppress per-event decode warnings; we print a summary below
        gcs_channels=(1, 2, 3, 4),
        # 251 = a normal 250-word frame plus one duplicate idle word from
        # run_sim_stream.m's per-chunk sim() restart (lands in the idle tail
        # after the last push pulse, never in the payload) - a simulation
        # chunking artifact, not a real hardware frame length.
        accepted_frame_lengths=(250, 248, 251),
        frame_length_counts={250: 18, 248: 1},
    )

    d = _load_mat(mat_path, ['push1', 'databit1', 'changeofstrength1', 'packetnum1'])
    push      = np.asarray(d['push1'],                                           dtype=np.uint16).ravel()
    N0        = len(push)
    databit   = np.asarray(d.get('databit1',          np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()
    error     = np.asarray(d.get('changeofstrength1', np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()
    packetnum = np.asarray(d.get('packetnum1',        np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()

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

    reader.decode_from_word_stream(words)
    _values, quality_raw = reader.get_decoded_arrays(GCS_CHANNEL)
    quality = np.asarray(quality_raw).ravel().astype(int)

    # Quality-byte distribution. The error flag (bit 3, 0x08) is stripped first;
    # we categorise on the base code (quality & 0x07). mismatch (base 5/6) is its
    # own category — both copies arrived but disagreed and the decoder picked one
    # by neighbour/magnitude; it is a recovered sample, not a miss.
    q_base = np.asarray(quality).astype(int) & 0x07
    n = len(q_base)
    n_error = int(np.count_nonzero(np.asarray(quality).astype(int) & 0x08))
    cols   = ['none', 'only_v1', 'only_v2', 'both', 'mismatch']
    counts = {
        'none':     int(np.count_nonzero(q_base == 0)),
        'only_v1':  int(np.count_nonzero(q_base == 1)),
        'only_v2':  int(np.count_nonzero(q_base == 2)),
        'both':     int(np.count_nonzero(q_base == 3)),
        'mismatch': int(np.count_nonzero(np.isin(q_base, (5, 6)))),
    }
    def _pct(c):
        return 100.0 * c / n if n else 0.0
    print(f'  Decoded {n} samples @ {reader.output_rate_hz:.1f} Hz '
          f'({n / reader.output_rate_hz:.1f}s)')
    print('  ' + f'{"":8}' + ''.join(f'{c:>10}' for c in cols))
    print('  ' + f'{"count":8}' + ''.join(f'{counts[c]:>10}' for c in cols))
    print('  ' + f'{"pct":8}' + ''.join(f'{_pct(counts[c]):>9.1f}%' for c in cols))
    print(f'  (error flag set on {n_error} / {n} samples = {_pct(n_error):.1f}%)')
    accounted = sum(counts.values())
    if accounted != n:
        print(f'  NOTE: {n - accounted} sample(s) had an unexpected base code '
              f'(not in {{0,1,2,3,5,6}})')

    # Mismatch / phase-tracking stats.
    print(f'  Mismatch stats — anomalies:{reader.packet_sequence_anomaly_count}  '
          f'header_drops:{reader.packet_sequence_header_drops}  '
          f'dist_corrections:{reader.packet_num_distance_corrections}  '
          f'relocks:{reader.phase_relocks}  locked:{reader._phase_locked}')

    return quality, float(reader.output_rate_hz)


# ══════════════════════════════════════════════════════════════════════════════
# Sliding-window rates
# ══════════════════════════════════════════════════════════════════════════════

def sliding_rates(quality, W):
    """
    Centered sliding window of width W over quality bytes.
    Uses cumsum rolling mean so edge samples clip to a smaller window
    rather than zero-padding, which would artificially lower rates at the
    start and end of the recording.

    Per-sample contribution to each rate (base code = quality & 0x07):
        both(3)        → nodiv 1,   timdiv 1
        only_v1(1)/only_v2(2) → nodiv ½, timdiv 1   (one of two copies present)
        mismatch(5/6)  → nodiv ½,   timdiv 1   (both copies present but disagree;
                          exactly one single-copy system holds the correct value,
                          so it is 50% correct without diversity, but diversity
                          picks the right one → full credit)
        none(0)        → nodiv 0,   timdiv 0

    Returns:
        rate_nodiv:  1 – (only_v1 + only_v2 + mismatch + 2·neither) / 2
        rate_timdiv: 1 – neither_fraction
    Both shape (N,), dtype float64.
    """
    q_base   = quality & 0x07
    neither  = (q_base == 0).astype(float)
    only_v1  = (q_base == 1).astype(float)
    only_v2  = (q_base == 2).astype(float)
    mismatch = np.isin(q_base, (5, 6)).astype(float)

    def _rolling(arr):
        half = W // 2
        n    = len(arr)
        cs   = np.concatenate([[0.0], np.cumsum(arr)])
        lo   = np.maximum(0, np.arange(n) - half)
        hi   = np.minimum(n, np.arange(n) + half + 1)
        return (cs[hi] - cs[lo]) / (hi - lo).astype(float)

    s_neither  = _rolling(neither)
    s_v1       = _rolling(only_v1)
    s_v2       = _rolling(only_v2)
    s_mismatch = _rolling(mismatch)

    rate_timdiv = 1.0 - s_neither
    rate_nodiv  = 1.0 - (s_v1 + s_v2 + s_mismatch + 2.0 * s_neither) / 2.0
    return rate_nodiv, rate_timdiv


# ══════════════════════════════════════════════════════════════════════════════
# Figure 1 — rate vs time (3 subplots, one per antenna config)
# ══════════════════════════════════════════════════════════════════════════════

def plot_rate_vs_time_all(configs, walk_start, walk_end, window_s):
    """
    configs: list of (label, t_axis, rate_nodiv, rate_timdiv) — skip Nones.
    Produces one subplot per config, shared x-axis.
    """
    active = [(lbl, t, nd, td) for lbl, t, nd, td in configs
              if t is not None]
    n = len(active)
    fig, axes = plt.subplots(n, 1, figsize=(13, 3 * n), sharex=True,
                             constrained_layout=True)
    if n == 1:
        axes = [axes]

    # Shared colour-blind-safe scheme: bluish-green = with diversity, vermillion
    # = no diversity; the walk-window span is a faint neutral sky-blue.
    C_TIMDIV = cfs.OKABE_ITO['bluish_green']
    C_NODIV  = cfs.OKABE_ITO['vermillion']
    for ax, (label, t_axis, rate_nodiv, rate_timdiv) in zip(axes, active):
        ax.fill_between(t_axis, 100 * rate_timdiv, alpha=0.15, color=C_TIMDIV)
        ax.plot(t_axis, 100 * rate_timdiv,
                color=C_TIMDIV, linewidth=1.1, label='With time diversity')

        ax.fill_between(t_axis, 100 * rate_nodiv, alpha=0.15, color=C_NODIV)
        ax.plot(t_axis, 100 * rate_nodiv,
                color=C_NODIV, linewidth=1.1, label='No time diversity (avg single-copy)')

        walk_mask = (t_axis >= walk_start) & (t_axis <= walk_end)
        if walk_mask.any():
            m_td = 100 * float(np.nanmean(rate_timdiv[walk_mask]))
            m_nd = 100 * float(np.nanmean(rate_nodiv[walk_mask]))
            ax.axhline(m_td, color=C_TIMDIV, linestyle='--', linewidth=0.9, alpha=0.7)
            ax.axhline(m_nd, color=C_NODIV, linestyle='--', linewidth=0.9, alpha=0.7)
            ax.text(walk_end, m_td + 1.5, f'{m_td:.0f}%',
                    color=C_TIMDIV, ha='right', va='bottom', fontsize=9)
            ax.text(walk_end, m_nd - 1.5, f'{m_nd:.0f}%',
                    color=C_NODIV, ha='right', va='top', fontsize=9)

        ax.axvspan(walk_start, walk_end, alpha=0.07, color=cfs.OKABE_ITO['sky_blue'])
        ax.set_xlim(t_axis[0], t_axis[-1])
        ax.set_ylim(0, 105)
        ax.set_ylabel('Reception rate (%)')
        ax.set_title(label)
        ax.grid(True, alpha=0.35)

    axes[-1].set_xlabel('Recording time (s)')
    # Single shared legend outside the axes (the curves are identical across
    # subplots), so it never overlaps the data.
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='outside lower center', ncol=2, fontsize=9)
    fig.suptitle(
        f'Sliding-window reception rate  '
        f'(W = {WINDOW_SAMPLES} samples ≈ {window_s:.2f} s)',
        fontsize=11)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Figure 2 — colored paths (one room diagram per antenna config)
# ══════════════════════════════════════════════════════════════════════════════

def _draw_room_path(ax, t_walk, rate_timdiv_walk, t_waypoints_raw,
                    W, H, rx, delta, label):
    """Populate one axes with a colored-path room diagram."""
    cmap = PATH_CMAP
    norm = mcolors.Normalize(vmin=0, vmax=1)

    arcs_path = np.array([_position_and_arc(t, t_waypoints_raw, W, H)[1]
                          for t in t_walk])
    xy_path = np.array([_path_xy_offset(s, W, H, delta) for s in arcs_path])

    segs   = np.stack([xy_path[:-1], xy_path[1:]], axis=1)
    colors = cmap(norm(rate_timdiv_walk[:-1]))
    lc = mc.LineCollection(segs, colors=colors, linewidths=2.2,
                           capstyle='round', joinstyle='round', zorder=5)

    ax.set_aspect('equal')
    ax.set_xlabel('x [m]')
    ax.set_ylabel('y [m]')
    ax.set_title(label)
    # Few ticks: 10 pt labels crowd a small panel.
    ax.xaxis.set_major_locator(MaxNLocator(nbins=3, integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=3, integer=True))

    cw_xy  = np.array([[W-delta, delta], [delta, delta],
                        [delta, H-delta], [W-delta, H-delta], [W-delta, delta]])
    ccw_xy = np.array([[W, 0.0], [W, H], [0.0, H], [0.0, 0.0], [W, 0.0]])
    ax.plot(cw_xy[:, 0],  cw_xy[:, 1],  '-',  color=CW_COLOR,
            lw=1.0, alpha=0.25)
    ax.plot(ccw_xy[:, 0], ccw_xy[:, 1], '--', color=CCW_COLOR,
            lw=1.0, alpha=0.25)
    # Stagger CW/CCW arrows along each edge so the two laps' markers don't stack.
    for i in range(4):
        _add_path_arrow(ax, cw_xy[i],  cw_xy[i + 1],  CW_COLOR,  frac=0.35)
        _add_path_arrow(ax, ccw_xy[i], ccw_xy[i + 1], CCW_COLOR, frac=0.55)

    ax.add_collection(lc)
    ax.plot(*_path_xy_offset(0.0, W, H, delta), 'D', markersize=4,
            color=START_COLOR, markeredgecolor='black', markeredgewidth=0.6,
            zorder=8, label='Start / End')
    # Rx star (labelled via the legend, so no redundant on-plot text).
    ax.plot(rx[0], rx[1], '*', markersize=7, color=RX_COLOR,
            markeredgecolor='white', markeredgewidth=0.5, zorder=8, label='Rx')

    ax.set_xlim(-0.25, W + 0.25)
    ax.set_ylim(-0.25, H + 0.25)
    return lc


# One colour per antenna config, drawn from the shared colour-blind-safe
# Okabe–Ito scheme (charfig_style). High-contrast picks (blue / vermillion /
# bluish-green) so the three series stay separable under CVD and in greyscale.
COND_COLORS = {
    'Ant 1 only':    cfs.OKABE_ITO['blue'],
    'Ant 2 only':    cfs.OKABE_ITO['vermillion'],
    'Both antennas': cfs.OKABE_ITO['bluish_green'],
}

# Short config labels shared across figures (Ant 1 / Ant 2 are two receive points
# sampling the same distribution; 'Both antennas' = antenna diversity → 'Ant-div').
SHORT_LABELS = {'Ant 1 only': 'Ant 1', 'Ant 2 only': 'Ant 2',
                'Both antennas': 'Ant-div'}


def _auto_distance_zones(dists, n_zones=4):
    """Quantile-based distance zones: each zone holds ~equal sample count, so the
    zones are always well-populated and never degenerate (unlike gap-splitting,
    which collapses when the distance spread is fairly continuous). Returns bin
    edges (length ≤ n_zones+1; duplicates collapse if distances are very clumped).
    Pass explicit edges to DISTANCE_ZONE_EDGES to override with physical bands."""
    d = np.asarray(dists, float)
    d = d[np.isfinite(d)]
    if len(d) < 2:
        lo = d[0] if len(d) else 0.0
        return np.array([lo - 1e-6, lo + 1e-6])
    edges = np.unique(np.quantile(d, np.linspace(0.0, 1.0, n_zones + 1)))
    edges[0] -= 1e-6
    edges[-1] += 1e-6
    return edges


def plot_rate_vs_distance(configs, t_waypoints_raw, W, H, rx, delta,
                          n_zones=4, zone_edges=None):
    """INTENT
    Takeaway: time diversity (retransmission) suppresses the OUTAGE TAIL, not just
      the average. Two panels share one log error-rate axis: LEFT = w/o
      time-diversity, RIGHT = with time-diversity. Because the y-limits match,
      a box's height and upper tail shrinking left→right IS the time-diversity
      gain. A median+IQR bar on a linear 0-100% axis hid this — near-Rx zones
      saturate close to 100% reception, so a 99%→99.9% gain is an invisible
      sliver; plotting 1-rate on a log axis turns that into a full decade, the
      largest visible change of any zone. Secondary: all three antenna
      conditions (Ant 1, Ant 2, Ant-div) shown separately within each panel —
      not pooled — so a real difference between the two single-antenna receive
      points would be visible rather than hidden by averaging.
    Encodings:
      - x-axis        = distance-from-Rx zone (quantile bands, or DISTANCE_ZONE_EDGES),
                        identical zones in both panels
      - y-axis (log)  = per-window ERROR rate as a FRACTION/probability,
                        1-reception_rate (the conventional way to present an
                        error/bit-error rate — not a log of a percent). Log scale
                        de-saturates the near-Rx zones and makes error-reduction
                        FACTORS (not just differences) readable
      - two panels    = w/o time-diversity (left) vs with time-diversity (right),
                        SAME y-limits so the shrinkage between panels is the gain
      - box color     = antenna condition (Ant 1 / Ant 2 / Ant-div)
      - box body      = IQR of the per-window error rate
      - whiskers      = 5th/95th percentile (NOT the Tukey 1.5xIQR rule — that
                        rule is a linear-space additive offset and behaves badly
                        under a log transform / skewed distribution; percentiles
                        commute with the log transform, so they stay meaningful
                        on this axis). Points beyond the whiskers = the outage tail
      - connecting line = per-zone median of each color, so the trend across
                        distance reads without eyeballing box positions
    Explained IN-FIGURE (must appear in the legend/annotation; absence = defect):
      - panel titles → "w/o Time-diversity" / "With Time-diversity"
      - box color → antenna condition, named in the legend (Ant 1 / Ant 2 / Ant-div)
      - y-axis label states this is an ERROR rate (fraction) on a LOG axis, so
        "lower is better" and "one gridline = one decade" both read correctly
        without the caption
    Deferred to CAPTION (OK to omit from the figure itself):
      - distance-zone edge definitions and the sliding-window width
      - the log-axis floor: a window with zero error is clipped to ERROR_FLOOR
        (half a sample's weight out of WINDOW_SAMPLES) so it doesn't break the
        log scale — state this explicitly in the caption
      - whisker convention (5th/95th percentile, not Tukey 1.5xIQR) and that
        points beyond the whiskers are the outage tail
    Good = a reader can see, without the caption, that the error distribution
      (body AND tail) shrinks from the left panel to the right panel, and which
      color is which antenna condition; the floor-clipping caveat and whisker
      convention are left to the caption.

    Two square panels sharing one log-scaled error-rate y-axis: LEFT = w/o
    time-diversity (single-copy) error rate, RIGHT = with time-diversity
    (retransmission-recovered) error rate. Within each panel, boxplots of
    per-window error rate (1 - reception rate, as a fraction) grouped by
    distance-from-Rx zone and by antenna condition (Ant 1 / Ant 2 / Ant-div,
    kept separate). Replaces the old median+IQR stacked-bar design, which
    saturated near the 100% reception ceiling and collapsed the outage tail
    into a single IQR cap.

    configs: (label, t_walk, rate_timdiv_walk, rate_nodiv_walk) — skip Nones.
    """
    active = [c for c in configs if c[1] is not None]
    rx = np.asarray(rx, float)

    def _dist_for(t_walk):
        arc = np.array([_position_and_arc(t, t_waypoints_raw, W, H)[1] for t in t_walk])
        xy  = np.array([_path_xy_offset(s, W, H, delta) for s in arc])
        return np.linalg.norm(xy - rx, axis=1)

    # Each antenna condition kept separate (no Ant 1/Ant 2 pooling) so a real
    # difference between the two single-antenna receive points stays visible.
    series = []   # (label, dist, r_div, r_nodiv)
    for label, t_walk, r_div, r_nodiv in active:
        dist = _dist_for(np.asarray(t_walk, float))
        series.append((label, dist, np.asarray(r_div, float), np.asarray(r_nodiv, float)))
    all_d = (np.concatenate([s[1] for s in series]) if series else np.array([0.0, 1.0]))

    if zone_edges is not None:
        edges = np.asarray(zone_edges, float)
    else:
        edges = _auto_distance_zones(all_d, n_zones)
    K = len(edges) - 1
    xpos = np.arange(K)
    zmask = lambda d, z: ((d >= edges[z]) & (d <= edges[z + 1])) if z == K - 1 \
        else ((d >= edges[z]) & (d < edges[z + 1]))
    def _zlabel(z):
        # Terse (no unit; unit goes on the axis label) and multi-line so 10 pt
        # labels do not collide on a small panel.
        m = zmask(all_d, z)
        if np.any(m):
            return f'{all_d[m].min():.1f}–\n{all_d[m].max():.1f}'
        return f'{edges[z]:.1f}–\n{edges[z + 1]:.1f}'
    zlabels = [_zlabel(z) for z in range(K)]

    n_cfg   = len(series)
    group_w = 0.82
    bw      = group_w / max(n_cfg, 1)
    offs    = (np.arange(n_cfg) - (n_cfg - 1) / 2.0) * bw

    fig, axes = plt.subplots(1, 2, figsize=cfs.panel_size(2.0), sharey=True,
                             constrained_layout=True)
    # ridx: which per-window rate array each panel draws from (1 = r_div = with
    # time-diversity, 2 = r_nodiv = w/o time-diversity).
    panel_defs = [('w/o Time-diversity', 2), ('With Time-diversity', 1)]

    for ax, (title, ridx) in zip(axes, panel_defs):
        ax.set_axisbelow(True)
        for ci, (label, dist, r_div, r_nodiv) in enumerate(series):
            rate = r_div if ridx == 1 else r_nodiv
            err  = np.maximum(1.0 - rate, ERROR_FLOOR)
            data = [err[zmask(dist, z)] for z in range(K)]
            positions = xpos + offs[ci]
            color = COND_COLORS.get(label)
            # whis=(5, 95): percentile whiskers, not the Tukey 1.5xIQR rule —
            # percentiles are scale-invariant under the log transform, so the
            # whisker caps stay meaningful on this log axis (see INTENT above).
            ax.boxplot(data, positions=positions, widths=bw * 0.85, whis=(5, 95),
                       patch_artist=True, showfliers=True,
                       flierprops=dict(marker='o', markersize=1.6,
                                       markerfacecolor=color, markeredgecolor='none',
                                       alpha=0.4),
                       medianprops=dict(color='black', linewidth=1.0),
                       boxprops=dict(facecolor=color, alpha=0.6,
                                     edgecolor=color, linewidth=0.6),
                       whiskerprops=dict(color=color, linewidth=0.7),
                       capprops=dict(color=color, linewidth=0.7))
            # Per-zone median trend line so the shape across distance reads
            # without eyeballing box positions (one line per color, per panel).
            medians = [np.nanmedian(d) if len(d) else np.nan for d in data]
            ax.plot(positions, medians, color=color, lw=1.0, marker='o', ms=2.0,
                    alpha=0.9, zorder=5)

        ax.set_yscale('log')
        ax.set_xticks(xpos)
        ax.set_xticklabels(zlabels)
        ax.set_xlabel('Distance from Rx [m]')
        ax.set_xlim(-0.5, K - 0.5)
        ax.set_title(title, fontsize=cfs.FONT_PT)
        ax.grid(True, axis='y', which='major', alpha=0.3)
        ax.set_box_aspect(1)
        cfs.style_axes(ax)

    axes[0].set_ylabel('Error rate (1−RR)  (log)')
    for ax in axes[1:]:
        ax.tick_params(labelleft=False)

    from matplotlib.patches import Patch
    legend_handles = [Patch(facecolor=COND_COLORS.get(lbl), label=SHORT_LABELS.get(lbl, lbl))
                      for lbl, *_ in series]
    fig.legend(handles=legend_handles, loc='outside lower center',
               ncol=len(legend_handles), handlelength=1.2, handletextpad=0.4,
               borderpad=0.3, columnspacing=1.2)
    fig.set_constrained_layout_pads(w_pad=0.02, h_pad=0.02, hspace=0.0, wspace=0.02)
    return fig


def plot_colored_loop_all(configs, t_waypoints_raw, W, H, rx, delta, window_s):
    """
    configs: list of (label, t_walk, rate_timdiv_walk) — skip Nones.
    Produces ONE figure with the active configs stacked VERTICALLY (one room
    diagram per row). Each row is keyed by its config label set bold and vertical
    along the left side; all rows share a single rate colorbar on the right and a
    single marker key (lap / start / Rx) below. Returns [fig].
    """
    active = [(lbl, t, td) for lbl, t, td in configs if t is not None]
    n = len(active)

    cmap = PATH_CMAP
    norm = mcolors.Normalize(vmin=0, vmax=1)

    from matplotlib.lines import Line2D
    legend_handles = [
        Line2D([0], [0], marker='>', linestyle='None', markersize=5,
               markerfacecolor=CW_COLOR, markeredgecolor='white', label='Lap 1 CW'),
        Line2D([0], [0], marker='>', linestyle='None', markersize=5,
               markerfacecolor=CCW_COLOR, markeredgecolor='white', label='Lap 2 CCW'),
        Line2D([0], [0], marker='D', linestyle='None', markersize=4.5,
               markerfacecolor=START_COLOR, markeredgecolor='black', label='Start / End'),
        Line2D([0], [0], marker='*', linestyle='None', markersize=7,
               markerfacecolor=RX_COLOR, markeredgecolor='white', label='Rx'),
    ]

    # The whole stack shares one charfig panel height (PANEL_H_IN): the n rooms
    # split it, so each row is ~PANEL_H_IN/n tall. Width holds one wide room.
    fig, axes = plt.subplots(n, 1, figsize=(cfs.PANEL_H_IN * 1.1,
                                            cfs.PANEL_H_IN),
                             constrained_layout=True)
    if n == 1:
        axes = [axes]

    for ax, (label, t_walk, rate_timdiv_walk) in zip(axes, active):
        print(f'  {label}: mapping {len(t_walk)} samples to path ...')
        _draw_room_path(ax, t_walk, rate_timdiv_walk, t_waypoints_raw,
                        W, H, rx, delta, label)
        ax.set_title('')   # row is keyed by the bold side label instead
        # The bold, vertical config label IS the side label — at this panel size
        # there is no room for a separate 'y [m]' title as well. Held at FONT_PT
        # (it must fit within the ~PANEL_H_IN/n row height) and padded out so it
        # clears the y-tick numbers.
        ax.set_ylabel(SHORT_LABELS.get(label, label),
                      fontweight='bold', fontsize=cfs.FONT_PT)
        ax.yaxis.labelpad = 6

    # Only the bottom row carries the x-axis title and tick labels.
    for ax in axes[:-1]:
        ax.set_xlabel('')
        ax.tick_params(labelbottom=False)

    # Single marker key stacked vertically in the right margin — keeps the full
    # figure height for the room loops instead of a bottom strip. Added before the
    # colorbar so constrained_layout reserves the outer-right column for it; the
    # colorbar then sits between the rooms and the legend, spanning the full height.
    fig.legend(handles=legend_handles, loc='outside center right', ncol=1,
               handletextpad=0.3, labelspacing=0.5, borderpad=0.3, frameon=False)

    # Single rate colorbar shared across all rows, full height of the stack.
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cb = fig.colorbar(sm, ax=list(axes), orientation='vertical', fraction=0.046,
                      pad=0.02, label='Reception rate')
    cb.locator = MaxNLocator(nbins=4)
    cb.update_ticks()

    fig.suptitle('Walk paths — reception rate')
    # Pull the rows together — the equal-aspect rooms otherwise leave wide gaps.
    fig.set_constrained_layout_pads(h_pad=0.02, hspace=0.0, w_pad=0.02, wspace=0.0)
    return [fig]


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    mat_paths = [MAT_ANT1, MAT_ANT2, MAT_BOTH]

    results = []   # (label, t_axis, rate_nodiv, rate_timdiv, duration_s, output_rate)
    first_duration = None
    for label, mat_path in zip(LABELS, mat_paths):
        if mat_path is None:
            results.append((label, None, None, None, None, None))
            continue
        print(f'\nLoading and decoding [{label}]  {mat_path} ...')
        try:
            quality, output_rate = decode_full_file(mat_path)
        except Exception as exc:
            print(f'  FAILED: {exc}')
            results.append((label, None, None, None, None, None))
            continue
        N          = len(quality)
        duration_s = N / output_rate
        if first_duration is None:
            first_duration = duration_s
        window_s = WINDOW_SAMPLES / output_rate
        print(f'  {N} samples  |  {output_rate:.1f} Hz  |  {duration_s:.1f} s  '
              f'|  window {window_s:.3f} s')

        rate_nodiv, rate_timdiv = sliding_rates(quality, WINDOW_SAMPLES)
        t_axis = np.arange(N) / output_rate
        results.append((label, t_axis, rate_nodiv, rate_timdiv, duration_s, output_rate))

    # Use the longest active recording to set walk_end if not specified
    active_durations = [r[4] for r in results if r[4] is not None]
    walk_end = WALK_END_S if WALK_END_S is not None else (
        max(active_durations) if active_durations else 0.0)
    t_waypoints_raw = ([WALK_START_S] + list(WALK_CORNER_TIMES_7) + [walk_end])

    # Print per-config walk-window means
    print('\n── Walk-window means ──')
    for label, t_axis, rate_nodiv, rate_timdiv, *_ in results:
        if t_axis is None:
            print(f'  {label:20s}  —')
            continue
        walk_mask = (t_axis >= WALK_START_S) & (t_axis <= walk_end)
        if walk_mask.any():
            m_td = 100 * float(np.nanmean(rate_timdiv[walk_mask]))
            m_nd = 100 * float(np.nanmean(rate_nodiv[walk_mask]))
            print(f'  {label:20s}  time-div {m_td:.1f}%   no-div {m_nd:.1f}%')

    window_s_display = (WINDOW_SAMPLES / results[next(
        i for i, r in enumerate(results) if r[1] is not None)][5])

    # Figure A — walk path colored by time-diversity reception rate
    figA_configs = []
    for label, t_axis, _, rate_timdiv, *__ in results:
        if t_axis is None:
            figA_configs.append((label, None, None))
            continue
        walk_mask = (t_axis >= WALK_START_S) & (t_axis <= walk_end)
        figA_configs.append((label, t_axis[walk_mask], rate_timdiv[walk_mask]))
    print('Plotting Figure A (walk path) ...')
    plot_colored_loop_all(figA_configs, t_waypoints_raw,
                          W_M, H_M, RX_M, DELTA_PATH, window_s_display)

    # Figure B — reception rate vs distance from Rx (zone-aggregated, div + no-div)
    figB_configs = []
    for label, t_axis, rate_nodiv, rate_timdiv, *__ in results:
        if t_axis is None:
            figB_configs.append((label, None, None, None))
            continue
        walk_mask = (t_axis >= WALK_START_S) & (t_axis <= walk_end)
        figB_configs.append((label, t_axis[walk_mask],
                             rate_timdiv[walk_mask], rate_nodiv[walk_mask]))
    print('Plotting Figure B (rate vs distance) ...')
    plot_rate_vs_distance(figB_configs, t_waypoints_raw,
                          W_M, H_M, RX_M, DELTA_PATH,
                          n_zones=N_DISTANCE_ZONES, zone_edges=DISTANCE_ZONE_EDGES)

    _save_all_figures(PLOTS_DIR)
    plt.show()


if __name__ == '__main__':
    main()
