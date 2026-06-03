from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Histogram parameters (NetEQ delay_manager.cc defaults)
# ---------------------------------------------------------------------------
MAX_DELAY_MS:        int   = 500    # ceiling of tracked inter-arrival time
BUCKET_MS:           int   = 10     # each histogram bucket = 10 ms
NUM_BUCKETS:         int   = MAX_DELAY_MS // BUCKET_MS   # 50 buckets

FORGET_FACTOR:       float = 0.983  # from real Chromium log (delay_manager.cc)
START_FORGET_WEIGHT: float = 2.0    # NetEQ warm-up: first packets steer less
QUANTILE:            float = 0.95   # p95 — same as NetEQ default

HYSTERESIS_MS:       float = 20.0   # minimum shift to accept a new target (US7379466 §8)
MIN_GAPS_WARMUP:     int   = 8      # gaps needed before histogram is trusted

# DrainMode thresholds (NetEQ decision_logic.cc)
ACCELERATE_THRESHOLD_MS: float = 40.0
DECELERATE_THRESHOLD_MS: float = 85.0   # deceleration_target_level_offset_ms=85

STALL_MS:            float = 600.0  # no chunk this long → stall declared

MIN_TARGET_MS:       float = 20.0
MAX_TARGET_MS:       float = 400.0


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class DrainMode(Enum):
    """
    Recommended drain speed — mirrors NetEQ decision_logic.cc states.
    Read via snapshot(); not required by the current pipeline but available
    for future wiring into _exotel_sender_worker.
    """
    NORMAL      = "NORMAL"
    ACCELERATE  = "ACCELERATE"   # buffer too full → drain faster
    DECELERATE  = "DECELERATE"   # buffer too low  → drain slower


@dataclass
class MonitorSnapshot:
    """Full monitor state for one drain-loop tick."""
    target_ms:          float
    current_buffer_ms:  float
    drain_mode:         DrainMode
    concealment_needed: bool
    avg_gap_ms:         float
    quantile_ms:        float
    is_warmed_up:       bool
    is_stalled:         bool
    histogram:          list[float]


# ---------------------------------------------------------------------------
# ChunkArrivalMonitor
# ---------------------------------------------------------------------------

class ChunkArrivalMonitor:
    """
    Drop-in replacement for the v1 ChunkArrivalMonitor in tts_pipeline.py.

    Existing call sites — unchanged
    --------------------------------
        chunk_monitor.record_chunk()          # in _stream_chunks_to_queue
        chunk_monitor.reset_timer()           # in player loop, per sentence
        chunk_monitor.mini_buffer_bytes       # threshold in player loop
        chunk_monitor.summary()               # logging

    New capabilities (not yet wired in pipeline, available for future use)
    -----------------------------------------------------------------------
        chunk_monitor.target_ms               # ms equivalent of mini_buffer_bytes
        chunk_monitor.is_stalled()            # wire into sender starvation logic
        chunk_monitor.snapshot(buf_ms)        # DrainMode + concealment_needed

    Parameters
    ----------
    bytes_per_sec : int
        Must match the pipeline's BYTES_PER_SEC (= SAMPLE_RATE * BYTES_PER_SAMPLE).
        Default 16_000 = 8 kHz × 16-bit mono (pipeline's actual value).
    """

    def __init__(
        self,
        bytes_per_sec:       int   = 16_000,   # pipeline: 8000 Hz * 2 bytes
        quantile:            float = QUANTILE,
        forget_factor:       float = FORGET_FACTOR,
        start_forget_weight: float = START_FORGET_WEIGHT,
        hysteresis_ms:       float = HYSTERESIS_MS,
        min_target_ms:       float = MIN_TARGET_MS,
        max_target_ms:       float = MAX_TARGET_MS,
    ) -> None:
        self._bytes_per_sec      = bytes_per_sec
        self._quantile           = quantile
        self._forget_factor      = forget_factor
        self._hysteresis_ms      = hysteresis_ms
        self._min_target_ms      = min_target_ms
        self._max_target_ms      = max_target_ms

        # Histogram state
        self._histogram:      list[float] = [0.0] * NUM_BUCKETS
        self._forget_weight:  float       = start_forget_weight

        # Timing state
        self._last_chunk_t:   Optional[float] = None
        self._gap_count:      int             = 0
        self._sum_gaps_ms:    float           = 0.0

        # Target state — start at 80 ms (small, low-latency startup default)
        # Inherited across turns; reset_timer() does NOT clear this.
        self.target_ms:       float = 80.0
        self._is_warmed_up:   bool  = False

    # ------------------------------------------------------------------
    # Existing pipeline call sites — unchanged signatures
    # ------------------------------------------------------------------

    def record_chunk(self) -> None:
        """Call every time a TTS chunk arrives. Synchronous, non-blocking."""
        now_ms = time.perf_counter() * 1000.0
        if self._last_chunk_t is not None:
            gap_ms = now_ms - self._last_chunk_t
            self._gap_count   += 1
            self._sum_gaps_ms += gap_ms
            self._update_histogram(gap_ms)
            self._recompute_target()
        self._last_chunk_t = now_ms

    def reset_timer(self) -> None:
        """
        Call between sentences (player loop).
        Clears timing so inter-sentence silence isn't recorded as a gap.
        Preserves target_ms and histogram — best prior for next sentence.
        """
        self._last_chunk_t  = None
        self._is_warmed_up  = False
        # _gap_count NOT reset — warmup gate stays open after first sentence

    @property
    def mini_buffer_bytes(self) -> int:
        """
        Bytes equivalent of target_ms.
        Preserves the existing pipeline call site:
            threshold = ... chunk_monitor.mini_buffer_bytes
        """
        return int(self.target_ms / 1000.0 * self._bytes_per_sec)

    def summary(self) -> str:
        """
        Log line — called by the pipeline's player loop.
        Matches the information density of v1 but uses histogram-derived values.
        """
        q_ms   = self._read_quantile_ms()
        buf_ms = self.target_ms
        return (
            f"target={buf_ms:.0f}ms | "
            f"buf={self.mini_buffer_bytes}B | "
            f"q95={q_ms:.0f}ms | "
            f"avg={self.avg_gap_ms:.0f}ms | "
            f"n={self._gap_count} | "
            f"warmed={self._is_warmed_up}"
        )

    # ------------------------------------------------------------------
    # New capabilities (available, not yet wired in pipeline)
    # ------------------------------------------------------------------

    def snapshot(self, current_buffer_ms: float) -> MonitorSnapshot:
        """
        Full state snapshot for one drain-loop tick.
        Pass the current audio queue depth in ms.

        Future wiring suggestion for _exotel_sender_worker:
            buf_ms   = frame_ready_queue.qsize() * FRAME_DURATION_MS
            snap     = chunk_monitor.snapshot(buf_ms)
            if snap.drain_mode == DrainMode.ACCELERATE:
                # drain an extra frame this tick
            if snap.concealment_needed:
                # insert comfort noise instead of silence
        """
        stalled     = self.is_stalled()
        concealment = stalled or (current_buffer_ms <= 0.0)

        if current_buffer_ms > self.target_ms + ACCELERATE_THRESHOLD_MS:
            mode = DrainMode.ACCELERATE
        elif current_buffer_ms < self.target_ms - DECELERATE_THRESHOLD_MS:
            mode = DrainMode.DECELERATE
        else:
            mode = DrainMode.NORMAL

        return MonitorSnapshot(
            target_ms          = self.target_ms,
            current_buffer_ms  = current_buffer_ms,
            drain_mode         = mode,
            concealment_needed = concealment,
            avg_gap_ms         = self.avg_gap_ms,
            quantile_ms        = self._read_quantile_ms(),
            is_warmed_up       = self._is_warmed_up,
            is_stalled         = stalled,
            histogram          = list(self._histogram),
        )

    def is_stalled(self, threshold_ms: float = STALL_MS) -> bool:
        """
        True if no chunk has arrived for longer than threshold_ms.

        Future wiring suggestion for _exotel_sender_worker:
            if chunk_monitor.is_stalled():
                print(f"{R}[Sender] chunk source stalled{RST}")
        """
        if self._last_chunk_t is None:
            return False
        return (time.perf_counter() * 1000.0 - self._last_chunk_t) > threshold_ms

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def avg_gap_ms(self) -> float:
        return self._sum_gaps_ms / self._gap_count if self._gap_count else 0.0

    @property
    def is_warmed_up(self) -> bool:
        return self._is_warmed_up

    # ------------------------------------------------------------------
    # Internal — histogram
    # ------------------------------------------------------------------

    def _update_histogram(self, gap_ms: float) -> None:
        """
        Exponential-decay histogram update (delay_manager.cc / webrtchacks):
          • All buckets × forget_factor           (old mass decays)
          • bucket[new_gap] += (1 - forget_factor) (new observation added)
          • sum(buckets) stays = 1

        forget_weight starts at 2.0 and decays toward 1.0 so early chunks
        don't over-steer the target (NetEQ start_forget_weight=2).
        """
        effective_ff = min(self._forget_factor, 1.0 - 1.0 / self._forget_weight)
        self._forget_weight = max(1.0, self._forget_weight * self._forget_factor)

        for i in range(NUM_BUCKETS):
            self._histogram[i] *= effective_ff

        bucket_idx = min(int(gap_ms / BUCKET_MS), NUM_BUCKETS - 1)
        self._histogram[bucket_idx] += (1.0 - effective_ff)

    def _read_quantile_ms(self) -> float:
        """
        Walk histogram left→right; return ms where cumulative prob ≥ quantile.
        Returns MAX_DELAY_MS if histogram not yet populated.
        """
        cumulative = 0.0
        for i, prob in enumerate(self._histogram):
            cumulative += prob
            if cumulative >= self._quantile:
                return (i + 0.5) * BUCKET_MS
        return float(MAX_DELAY_MS)

    def _recompute_target(self) -> None:
        """
        Update target_ms from histogram.
        Gates: warmup (MIN_GAPS_WARMUP) + hysteresis (HYSTERESIS_MS).
        """
        if self._gap_count < MIN_GAPS_WARMUP:
            return

        if not self._is_warmed_up:
            self._is_warmed_up = True

        new_target = self._read_quantile_ms()
        new_target = max(self._min_target_ms, min(self._max_target_ms, new_target))

        if abs(new_target - self.target_ms) > self._hysteresis_ms:
            old = self.target_ms
            self.target_ms = new_target
            print(
                f"[ChunkMonitor] target {old:.0f}ms → {new_target:.0f}ms | "
                f"q95={new_target:.0f}ms | "
                f"avg_gap={self.avg_gap_ms:.0f}ms | "
                f"buf={self.mini_buffer_bytes}B | "
                f"n={self._gap_count}"
            )