"""
test_restart_logic.py — unit tests for autorestart / quality-gate logic.

Runs without real hardware or GCS credentials.
Uses existing seams:
  - enable_gcs=False  → skips all GCS client initialisation
  - bladerf absent    → _bladerf is None, class loads fine (setup_device never called)
  - decoded_quality_by_channel is a plain dict[int, deque] → injectable directly
  - _append_gcs_group accepts plain numpy arrays

Run with:  python3 -m pytest test_restart_logic.py -v
"""

import json
import os
import time
import threading
from collections import deque

import numpy as np
import pytest

from sdr_reader_gcs_write import TimeStampBasedReader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_reader(**kwargs) -> TimeStampBasedReader:
    """Instantiate a reader with GCS disabled so no credentials are needed."""
    defaults = dict(enable_gcs=False, enable_plotting=False, enable_gcs_trigger=False)
    defaults.update(kwargs)
    return TimeStampBasedReader(**defaults)


def inject_quality(reader: TimeStampBasedReader, quality_values: list, ch: int = None):
    """Push synthetic quality groups into decoded_quality_by_channel.

    quality_values: list of ints (quality codes) — each appended as a 1-element group.
    ch: channel number (defaults to reader.channel_to_decode).
    """
    if ch is None:
        ch = reader.channel_to_decode
    dq = reader.decoded_quality_by_channel[ch]
    for q in quality_values:
        dq.append(np.array([q, q, q, q], dtype=np.int8))


def make_rows(quality_code: int, n: int = 1):
    """Build n synthetic (values_row, packed_quality) tuples for _post_restart_hold_buffer."""
    rows = []
    for _ in range(n):
        # All channels in a 4-slot group; pack quality into nibbles
        vals = np.zeros(4, dtype=np.int32)
        pq = np.uint16(0)
        for ch_idx in range(4):
            pq = np.uint16(pq | ((quality_code & 0xF) << (4 * ch_idx)))
        rows.append((vals, pq))
    return rows


def fake_group_quality(quality_code: int, channels=(1, 2, 3, 4)):
    """Return a group_quality dict with the given code for all 4 slots in each channel."""
    return {ch: [quality_code] * 4 for ch in channels}


# ---------------------------------------------------------------------------
# 1. Drop rate calculation
# ---------------------------------------------------------------------------

class TestDropRate:
    def test_all_good(self):
        r = make_reader()
        inject_quality(r, [3] * 100)
        assert r._recent_drop_rate() == pytest.approx(0.0)

    def test_all_bad(self):
        r = make_reader()
        inject_quality(r, [0] * 100)
        assert r._recent_drop_rate() == pytest.approx(1.0)

    def test_half_bad(self):
        r = make_reader()
        inject_quality(r, [0] * 50 + [3] * 50)
        rate = r._recent_drop_rate()
        assert 0.45 < rate < 0.55  # 50% ± tolerance for 4-slot groups

    def test_empty_returns_zero(self):
        r = make_reader()
        assert r._recent_drop_rate() == 0.0


# ---------------------------------------------------------------------------
# 2. Watchdog cooldown
# ---------------------------------------------------------------------------

class TestWatchdogCooldown:
    def _run_one_watchdog_cycle(self, reader):
        """Simulate one watchdog evaluation (skip the sleep loop)."""
        if reader._last_restart_time is not None:
            if (time.time() - reader._last_restart_time) < reader.restart_cooldown_s:
                return  # still in cooldown
        drop_rate = reader._recent_drop_rate()
        if drop_rate > reader.sdr_restart_drop_threshold:
            reader._sdr_restart_requested.set()

    def test_cooldown_suppresses_restart(self):
        r = make_reader()
        inject_quality(r, [0] * 200)  # all bad
        r._last_restart_time = time.time()  # cooldown just started
        self._run_one_watchdog_cycle(r)
        assert not r._sdr_restart_requested.is_set()

    def test_cooldown_expired_allows_restart(self):
        r = make_reader()
        inject_quality(r, [0] * 200)  # all bad
        r._last_restart_time = time.time() - r.restart_cooldown_s - 1  # expired
        self._run_one_watchdog_cycle(r)
        assert r._sdr_restart_requested.is_set()

    def test_no_cooldown_allows_restart(self):
        r = make_reader()
        inject_quality(r, [0] * 200)
        r._last_restart_time = None
        self._run_one_watchdog_cycle(r)
        assert r._sdr_restart_requested.is_set()


# ---------------------------------------------------------------------------
# 3. Quality gate — buffering behaviour
# ---------------------------------------------------------------------------

class TestQualityGate:
    def _activate_gate(self, reader):
        # enable_gcs must be True so _append_gcs_group doesn't short-circuit before the gate.
        # gcs_bucket_obj stays None so any flush is safely skipped.
        reader.enable_gcs = True
        reader._post_restart_hold = True
        reader._post_restart_hold_buffer = []
        reader._post_restart_hold_timestamps = []
        reader._post_restart_hold_start = time.time()
        reader.gcs_recording_active = True

    def test_gate_buffers_instead_of_writing(self):
        r = make_reader()
        self._activate_gate(r)
        gq = fake_group_quality(3)
        r._append_gcs_group({}, {ch: [0]*4 for ch in range(1,5)}, gq)
        assert len(r._post_restart_hold_buffer) == 4  # 4 slots buffered
        assert len(r.gcs_write_buffer) == 0           # nothing written

    def test_gate_clears_on_good_window(self):
        r = make_reader()
        r.post_restart_quality_window_s = 1.0  # short window for test
        self._activate_gate(r)
        window_samples = int(r.post_restart_quality_window_s * r.output_rate_hz)
        # Feed enough good-quality groups (4 slots each)
        groups_needed = (window_samples // 4) + 2
        gq = fake_group_quality(3)
        grv = {ch: [100]*4 for ch in range(1,5)}
        for _ in range(groups_needed):
            r._append_gcs_group({}, grv, gq)
        assert not r._post_restart_hold
        assert len(r.gcs_write_buffer) > 0

    def test_gate_stays_on_bad_window(self):
        r = make_reader()
        r.post_restart_quality_window_s = 1.0
        self._activate_gate(r)
        window_samples = int(r.post_restart_quality_window_s * r.output_rate_hz)
        groups_needed = (window_samples // 4) + 2
        gq = fake_group_quality(0)  # all bad
        grv = {ch: [0]*4 for ch in range(1,5)}
        for _ in range(groups_needed):
            r._append_gcs_group({}, grv, gq)
        assert r._post_restart_hold  # still held
        assert len(r.gcs_write_buffer) == 0

    def test_gate_stays_held_on_failed_window(self):
        """A rolling window that fails keeps the gate held and nothing written to GCS."""
        r = make_reader()
        r.post_restart_quality_window_s = 0.5
        self._activate_gate(r)
        window_samples = int(r.post_restart_quality_window_s * r.output_rate_hz)
        gq_bad = fake_group_quality(0)
        grv = {ch: [0]*4 for ch in range(1,5)}
        groups_needed = (window_samples // 4) + 2
        for _ in range(groups_needed):
            r._append_gcs_group({}, grv, gq_bad)
        # Gate still held, nothing written to GCS
        assert r._post_restart_hold
        assert len(r.gcs_write_buffer) == 0

    def test_max_gate_triggers_restart(self):
        r = make_reader()
        r.post_restart_quality_window_s = 1.0
        self._activate_gate(r)
        r._post_restart_hold_start = time.time() - r.post_restart_max_gate_s - 1
        gq = fake_group_quality(0)
        grv = {ch: [0]*4 for ch in range(1,5)}
        r._append_gcs_group({}, grv, gq)
        assert r._sdr_restart_requested.is_set()
        assert not r._post_restart_hold  # gate cleared before requesting restart


# ---------------------------------------------------------------------------
# 4. Recording state file
# ---------------------------------------------------------------------------

STATE_FILE = '/tmp/sdr_recording_state.json'


class TestRecordingStateFile:
    def setup_method(self):
        # Clean up any leftover state file before each test
        try:
            os.remove(STATE_FILE)
        except FileNotFoundError:
            pass

    def _make_recording_reader(self):
        r = make_reader()
        r.gcs_recording_active = True
        r.gcs_session_id = 'test_session'
        r.gcs_blob_name = 'test_blob'
        r._gcs_recording_start_time = time.time()
        r._gcs_trigger_duration = 600.0
        return r

    def test_state_file_written_on_start(self):
        r = self._make_recording_reader()
        r._write_recording_state_file()
        assert os.path.exists(STATE_FILE)
        with open(STATE_FILE) as f:
            state = json.load(f)
        assert state['session_id'] == 'test_session'
        assert state['blob_name'] == 'test_blob'
        assert state['duration_seconds'] == 600.0
        assert state['projected_end_time_unix'] is not None

    def test_state_file_deleted_on_stop(self):
        r = self._make_recording_reader()
        r._write_recording_state_file()
        assert os.path.exists(STATE_FILE)
        r._delete_recording_state_file()
        assert not os.path.exists(STATE_FILE)

    def test_delete_is_idempotent(self):
        r = make_reader()
        r._delete_recording_state_file()  # should not raise

    def test_resume_check_expired_window(self):
        state = {
            'session_id': 'old_session',
            'blob_name': 'old_blob',
            'start_time_utc': '2020-01-01T00:00:00Z',
            'start_time_unix': 1577836800.0,
            'duration_seconds': 60.0,
            'projected_end_time_unix': 1577836860.0,  # far in the past
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        r = make_reader()
        r._check_resume_recording()
        assert not r.gcs_recording_active
        assert not os.path.exists(STATE_FILE)

    def test_resume_check_valid_window(self):
        projected_end = time.time() + 300.0
        state = {
            'session_id': 'live_session',
            'blob_name': 'live_blob',
            'start_time_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'start_time_unix': time.time(),
            'duration_seconds': 600.0,
            'projected_end_time_unix': projected_end,
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        r = make_reader()
        r._check_resume_recording()
        assert r.gcs_recording_active
        assert r.gcs_session_id == 'live_session'
        assert r.gcs_blob_name == 'live_blob'

    def test_resume_check_no_file(self):
        r = make_reader()
        r._check_resume_recording()  # should silently do nothing
        assert not r.gcs_recording_active

    def test_resume_check_open_ended_recording(self):
        """Open-ended recording (no duration) should always resume."""
        state = {
            'session_id': 'open_session',
            'blob_name': 'open_blob',
            'start_time_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'start_time_unix': time.time(),
            'duration_seconds': None,
            'projected_end_time_unix': None,
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        r = make_reader()
        r._check_resume_recording()
        assert r.gcs_recording_active
