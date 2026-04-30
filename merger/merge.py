#!/usr/bin/env python3
"""
WarMap merger.

Reads recorder NDJSON dumps from `scripts/WarMapRecorder/dumps/`, aggregates
by zone (or by pit-world for pit records), and writes curated per-zone JSON
to `<repo>/data/zones/<key>.json`.

The curated output is what the runtime loader (and eventually the
Batmobile-replacement plugin) reads. Cells get majority-voted across
sessions, actors are deduped by (skin, position, floor), and a saturation
heuristic decides when a zone has enough data that the recorder can stop
probing it.

Usage:
    python merge.py --once                  # merge everything once + exit
    python merge.py --watch                 # one initial pass, then watch for new files
    python merge.py --dumps <path> --out <path>   # custom paths
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

# orjson is 2-5x faster than stdlib `json` on the line-by-line parse hot
# path.  Optional import so dev runs without it (`pip install orjson`)
# still work, just slower.
try:
    import orjson as _orjson
    _HAS_ORJSON = True
    _JSON_DECODE_ERROR: tuple = (_orjson.JSONDecodeError, ValueError)
except ImportError:
    _orjson = None  # type: ignore
    _HAS_ORJSON = False
    _JSON_DECODE_ERROR = (json.JSONDecodeError,)


def _loads(s):
    """Fast JSON loader.  orjson when available, json fallback."""
    if _HAS_ORJSON:
        return _orjson.loads(s)
    if isinstance(s, (bytes, bytearray)):
        s = s.decode('utf-8', errors='replace')
    return json.loads(s)


# ---------------------------------------------------------------------------
# Parse cache -- keyed by absolute path string -> (mtime, parsed Record).
# Lives at module level so it persists across calls within the same Python
# process (the FastAPI worker that holds the scheduler lock).  When a file's
# mtime hasn't changed since the last parse, we reuse the cached Record
# instead of re-reading + re-parsing.  Entries for files that no longer
# exist on disk get evicted lazily on the next merge_all call.
#
# Memory cost: roughly the parsed-form size of all current dumps (a few GB
# at 1k+ dumps).  On the on-prem box with 30 GB RAM this is cheap.  If you
# ever migrate back to a small VPS, set _PARSE_CACHE_MAX to bound it.
_PARSE_CACHE: dict[str, tuple[float, 'Optional[Record]']] = {}
_PARSE_CACHE_MAX = 50_000     # safety upper bound; well above realistic usage


# ---------------------------------------------------------------------------
# State cache -- the merged dict[zone_key, KeyAgg] is kept alive across
# merge_all calls so we don't have to rebuild it from scratch every cycle.
# Together with the parse cache this turns a steady-state merge from
# "walk all dumps" to "fold the new dumps into the existing state."
#
# Reset to None to force a full rebuild (e.g. on quarantine where we'd
# need to subtract a record's contribution -- not currently supported).
# merge_all auto-detects file deletions and triggers a rebuild itself.
_STATE_CACHE: 'Optional[dict[str, KeyAgg]]' = None
_LAST_FILES: set[str] = set()

# ---------------------------------------------------------------------------
# Constants + tuning knobs
# ---------------------------------------------------------------------------

SCHEMA_VERSION_SUPPORTED = 1

# Saturation: a zone is "done" once new cells per session drops below this
# for SATURATION_RUNS consecutive sessions.
SATURATION_NEW_CELLS = 5
SATURATION_RUNS = 3

# Cell key: rounded to integer cell coords (cx, cy). Floor included for pit.
CellKey = tuple                       # (cx, cy)
ActorKey = tuple                      # (skin, rx, ry, floor)

# Activity kinds that share the same merge key (use record.zone)
ZONE_KEYED_ACTIVITIES = {
    'town', 'helltide', 'nmd', 'undercity', 'hordes', 'overworld',
}

# ---------------------------------------------------------------------------
# Per-record parsing
# ---------------------------------------------------------------------------

@dataclass
class Record:
    schema_version: int
    session_id: str
    activity_kind: str
    zone: str
    world: str
    world_id: int
    started_at: int
    ended_at: int
    complete: bool                              # footer line was present
    floor_worlds:    dict[int, str]             # floor_idx -> world name
    floor_world_ids: dict[int, int]             # floor_idx -> world_id (hash)
    samples: list[dict]
    events: list[dict]
    actors: list[dict]
    grid_cells_by_floor: dict[int, list[tuple[int, int, int]]]   # floor -> [(cx, cy, w)]
    grid_resolution: float

    @property
    def duration_s(self) -> int:
        return max(0, self.ended_at - self.started_at)


def parse_ndjson(path: Path) -> Optional[Record]:
    """Parse a single NDJSON dump into a Record. Returns None if header missing.

    Note on cells: post-v0.2 the recorder stops actively probing walkability
    (the host now ships its own pathfinder, so we no longer need a derived
    grid for navigation).  Walkable cells are reconstructed from the position
    sample stream -- every sample = "a real player stood walkable here".
    Older dumps that still carry explicit `grid_cell` lines keep merging
    correctly via the same code path.
    """
    header = None
    samples: list[dict] = []
    events: list[dict] = []
    actors: list[dict] = []
    grid: dict[int, list[tuple[int, int, int]]] = collections.defaultdict(list)
    floor_worlds:    dict[int, str] = {}
    floor_world_ids: dict[int, int] = {}
    grid_resolution = 0.5
    complete = False

    # Read whole file as bytes + splitlines is significantly faster than
    # iterating the file object line-by-line in text mode.  Combined with
    # orjson on the hot loads call, this dropped per-file parse time
    # ~3-4x in microbenchmarks.
    try:
        data = path.read_bytes()
    except OSError as e:
        print(f"  ! cannot read {path}: {e}", file=sys.stderr)
        return None
    for line in data.splitlines():
        if not line:
            continue
        try:
            obj = _loads(line)
        except _JSON_DECODE_ERROR:
            continue
        t = obj.get('type')
        if t == 'header':
            header = obj
            floor_worlds[1]    = obj.get('world', '')
            floor_world_ids[1] = obj.get('world_id', 0)
            # New header field carries the recorder's cell-derivation
            # resolution; old headers omit it and we fall back to the
            # 0.5m default below.
            if 'cell_resolution_m' in obj:
                grid_resolution = obj['cell_resolution_m']
        elif t == 'sample':
            samples.append(obj)
        elif t == 'event':
            events.append(obj)
            if obj.get('kind') == 'floor_change':
                meta = obj.get('metadata') or {}
                to_floor    = meta.get('to_floor')
                to_world    = meta.get('to_world')
                to_world_id = meta.get('to_world_id')
                if to_floor and to_world:
                    floor_worlds[to_floor] = to_world
                if to_floor and to_world_id:
                    floor_world_ids[to_floor] = to_world_id
        elif t == 'actor':
            actors.append(obj)
        elif t == 'grid_cell':
            fl = obj.get('floor', 1)
            grid[fl].append((obj['cx'], obj['cy'], obj['w']))
            if obj.get('res'):
                grid_resolution = obj['res']
        elif t == 'footer':
            complete = True

    if not header:
        return None

    # If the dump has no explicit grid_cell entries, derive walkable cells
    # from the sample stream.  Each sample is a "player stood here" =
    # walkable.  Floor handling: in pit records, samples carry their own
    # `floor` field; otherwise default to floor 1.
    total_explicit = sum(len(c) for c in grid.values())
    if total_explicit == 0 and samples:
        seen: dict[int, set[tuple[int, int]]] = {}
        for s in samples:
            x = s.get('x'); y = s.get('y')
            if x is None or y is None:
                continue
            cx = int(round(x / grid_resolution))
            cy = int(round(y / grid_resolution))
            fl = int(s.get('floor', 1))
            bucket = seen.setdefault(fl, set())
            if (cx, cy) in bucket:
                continue
            bucket.add((cx, cy))
            grid[fl].append((cx, cy, 1))   # all sample-derived cells are walkable

    return Record(
        schema_version=header.get('schema_version', 0),
        session_id=header.get('session_id', ''),
        activity_kind=header.get('activity_kind', ''),
        zone=header.get('zone', ''),
        world=header.get('world', ''),
        world_id=header.get('world_id', 0),
        started_at=header.get('started_at', 0),
        ended_at=header.get('ended_at', 0),
        complete=complete,
        floor_worlds=floor_worlds,
        floor_world_ids=floor_world_ids,
        samples=samples,
        events=events,
        actors=actors,
        grid_cells_by_floor=dict(grid),
        grid_resolution=grid_resolution,
    )


# ---------------------------------------------------------------------------
# Per-key aggregation
# ---------------------------------------------------------------------------

@dataclass
class CellAgg:
    """Running aggregate for one cell across many session contributions."""
    walk: int = 0
    block: int = 0

    def vote(self, walkable: bool) -> None:
        if walkable:
            self.walk += 1
        else:
            self.block += 1

    @property
    def total(self) -> int:
        return self.walk + self.block

    # Walkable trumps blocked.  The recorder's grid_probe uses the host's
    # is_point_walkeable() which is a deterministic snapshot of the game's
    # walkable mesh -- if it returned True for a cell in any session, that
    # cell IS walkable in the game.  Walls never observe as walkable.
    # Boss-room doors and Hordes wave-gates DO observe walkable post-fight,
    # so even one such observation should win against any number of
    # observations made while the door was closed.
    #
    # The previous "majority vote" + "10% threshold + walk_count >= 2"
    # logic was too conservative -- a boss-room door observed walkable in
    # 1 session out of 6 stayed blocked because walk_count < 2.  With
    # bias-toward-walkable, that case correctly becomes walkable.
    @property
    def is_walkable(self) -> bool:
        return self.walk >= 1

    @property
    def confidence(self) -> float:
        if self.total == 0:
            return 0.0
        return max(self.walk, self.block) / self.total


@dataclass
class ActorAgg:
    skin: str
    kind: str
    x: float
    y: float
    z: float
    floor: int
    first_seen_session: str
    first_seen_t: float
    sessions_seen: set[str] = field(default_factory=set)
    total_observations: int = 0
    type_id: Optional[int] = None
    sno_id: Optional[int] = None
    radius: Optional[float] = None
    is_boss: Optional[bool] = None
    is_elite: Optional[bool] = None


@dataclass
class FloorMetaAgg:
    """Per-floor diagnostics surfaced via the curated JSON.  Lets the
    viewer show 'floor 3 -> world=DGN_Skov_Aegoye_Boss, world_id=...'
    in the zone-details dropdown without having to crack open dump
    files.

    `worlds` accumulates all distinct world names ever seen on this
    floor (typically one per zone, but the same floor index can repeat
    across sessions if floor-change detection is fuzzy on a particular
    zone -- a multi-name list is itself a useful diagnostic).  Same for
    world_ids."""
    worlds:    set[str] = field(default_factory=set)
    world_ids: set[int] = field(default_factory=set)
    sessions:  set[str] = field(default_factory=set)


@dataclass
class KeyAgg:
    """Aggregated geometry + actors for one merge key (zone or pit-world)."""
    key: str
    key_type: str                                      # 'zone' or 'pit_world'
    grid_resolution: float = 0.5
    # cells_by_floor[floor][(cx, cy)] -> CellAgg
    cells_by_floor: dict[int, dict[CellKey, CellAgg]] = field(
        default_factory=lambda: collections.defaultdict(dict))
    actors: dict[ActorKey, ActorAgg] = field(default_factory=dict)
    sessions: set[str] = field(default_factory=set)
    activity_kinds: set[str] = field(default_factory=set)
    # All-floors world tracking -- set so re-runs that observe the same
    # name don't bloat memory.  Surfaced in the curated JSON as sorted
    # lists.
    worlds:    set[str] = field(default_factory=set)
    world_ids: set[int] = field(default_factory=set)
    # Per-floor breakdown (floor_idx -> FloorMetaAgg).  defaultdict so
    # callers can floors_meta[k].worlds.add(...) without a key check.
    floors_meta: dict[int, FloorMetaAgg] = field(
        default_factory=lambda: collections.defaultdict(FloorMetaAgg))
    # Saturation tracking: history of cells_total after each session merge
    cells_history: list[tuple[int, int]] = field(default_factory=list)
                                                    # [(unix_ts, cells_total), ...]


def merge_record_into(state: dict[str, KeyAgg], rec: Record) -> list[str]:
    """
    Merge record into the appropriate KeyAgg(s). Returns the list of keys
    that were touched (a pit record can touch many).
    """
    touched: list[str] = []

    if rec.activity_kind == 'pit':
        # Each floor's cells/actors belong to that floor's WORLD (template).
        for floor_idx, cells in rec.grid_cells_by_floor.items():
            world = rec.floor_worlds.get(floor_idx)
            if not world:
                continue
            key = world
            agg = _get_or_create(state, key, 'pit_world')
            agg.activity_kinds.add('pit')
            agg.sessions.add(rec.session_id)
            agg.grid_resolution = rec.grid_resolution
            # Pit key IS the world name -- worlds set is degenerate
            # (always one entry == agg.key) but we still record it so
            # the viewer code path is uniform across zone/pit_world.
            agg.worlds.add(world)
            wid = rec.floor_world_ids.get(floor_idx)
            if wid:
                agg.world_ids.add(wid)
            fm = agg.floors_meta[1]
            fm.worlds.add(world)
            if wid:
                fm.world_ids.add(wid)
            fm.sessions.add(rec.session_id)
            cell_map = agg.cells_by_floor[1]   # within the template, only one floor
            for cx, cy, w in cells:
                _vote_cell(cell_map, cx, cy, w)
            for a in rec.actors:
                if a.get('floor') == floor_idx:
                    _merge_actor(agg.actors, a, rec.session_id)
            touched.append(key)

    elif rec.activity_kind in ZONE_KEYED_ACTIVITIES:
        key = rec.zone
        agg = _get_or_create(state, key, 'zone')
        agg.activity_kinds.add(rec.activity_kind)
        agg.sessions.add(rec.session_id)
        agg.grid_resolution = rec.grid_resolution
        # Top-level worlds is the union of (header world) + (every per-
        # floor world we observed via floor_change events).  The header
        # world alone is misleading for multi-floor zones (undercity
        # drops you into floor 1's world; floors 2+ are different
        # worlds reached only mid-session).  Aggregating both lets the
        # viewer's "worlds:" row show e.g. UC_Ziggurat_01,
        # UC_Ziggurat_02, UC_Ziggurat_03 rather than just the entrance.
        if rec.world:
            agg.worlds.add(rec.world)
        if rec.world_id:
            agg.world_ids.add(rec.world_id)
        for w in rec.floor_worlds.values():
            if w: agg.worlds.add(w)
        for wid in rec.floor_world_ids.values():
            if wid: agg.world_ids.add(wid)
        # Per-floor meta: prefer the floor-change-event-tracked maps
        # (they cover every floor visited mid-session) and fall back to
        # the header's zone-level world+world_id for floors that were
        # never explicitly transitioned to.
        for floor_idx in rec.grid_cells_by_floor.keys():
            fm = agg.floors_meta[floor_idx]
            fw  = rec.floor_worlds.get(floor_idx)    or rec.world
            fwi = rec.floor_world_ids.get(floor_idx) or rec.world_id
            if fw:  fm.worlds.add(fw)
            if fwi: fm.world_ids.add(fwi)
            fm.sessions.add(rec.session_id)
        for floor_idx, cells in rec.grid_cells_by_floor.items():
            cell_map = agg.cells_by_floor[floor_idx]
            for cx, cy, w in cells:
                _vote_cell(cell_map, cx, cy, w)
        for a in rec.actors:
            _merge_actor(agg.actors, a, rec.session_id)
        touched.append(key)

    else:
        # Unknown activity kind -- skip.
        return []

    # Log a snapshot of cell counts for saturation tracking.
    for key in touched:
        agg = state[key]
        cells_total = sum(len(m) for m in agg.cells_by_floor.values())
        agg.cells_history.append((rec.ended_at or int(time.time()), cells_total))

    return touched


def _get_or_create(state: dict[str, KeyAgg], key: str, key_type: str) -> KeyAgg:
    if key not in state:
        state[key] = KeyAgg(key=key, key_type=key_type)
    return state[key]


# Skins we never emit into the merged actor catalog -- transient props
# whose position is meaningless for pathing (they spawn wherever a kill /
# event happened, then despawn).  Mirrors the recorder-side list in
# WarMapRecorder/core/actor_capture.lua's SKIN_IGNORE_SUBSTR.  Filtering
# at merge time means re-merging existing dumps removes these from the
# emitted zone JSONs and the actor index, without needing to re-collect
# data after the recorder is updated.
#
# Substring match (`s in skin`) -- pattern entries catch all variants.
_SKIN_IGNORE_SUBSTR = (
    # Pit-boss paragon-glyph upgrade gizmo: spawns at the boss kill location,
    # which is random per-run.  All three known variants:
    'Gizmo_Paragon_Glyph_Upgrade',
    'EGD_MSWK_GlyphUpgrade',
    'Pit_Glyph',
    # Floor pickups -- spawn at random kill positions, consumed-on-pickup.
    #   HealthPot_Dose_Pickup            (type_id 30631106,   sno_id 47186448)
    #   Sorcerer_CracklingEnergy_Pickup  (type_id 1385506706, sno_id 72352155)
    #   BurningAether                    (type_id 3189278226, sno_id 1234174277)
    'HealthPot_Dose_Pickup',
    'Sorcerer_CracklingEnergy_Pickup',
    'BurningAether',
)


def _is_ignored_skin(skin: str) -> bool:
    if any(s in skin for s in _SKIN_IGNORE_SUBSTR):
        return True
    # Dynamic patterns: admin-added at runtime via /admin/ignore.
    # _DYNAMIC_IGNORE is refreshed by the caller (app/main.py's _run_merge)
    # on every merge cycle so additions take effect within ~30s.
    return any(s in skin for s in _DYNAMIC_IGNORE)


# Mutable list -- callers (the FastAPI _run_merge) replace this with the
# DB's current ignore_patterns on every merge cycle.  Empty by default
# so a stand-alone merger run (no DB) just uses the static list.
_DYNAMIC_IGNORE: tuple = ()


def set_dynamic_ignore(patterns):
    """Replace the dynamic ignore-pattern set.  Called by the FastAPI
    server before kicking off a merge so the merger picks up admin-
    added ignores.  Idempotent on identical input."""
    global _DYNAMIC_IGNORE
    new = tuple(p for p in (patterns or []) if p)
    if new != _DYNAMIC_IGNORE:
        _DYNAMIC_IGNORE = new
        # Force the next merge to do a full rebuild so existing aggregates
        # drop the newly-ignored skins.
        reset_merge_state()


def _vote_cell(cell_map: dict[CellKey, CellAgg], cx: int, cy: int, w: int) -> None:
    # Defensive filter: drop cells within ~5m of world (0,0).  Older
    # recorder versions wrote bogus probe-target cells around origin
    # when the host returned near-zero player positions during teleport
    # transitions.  Real D4 maps live thousands of cell-units from origin
    # so this rejects garbage without ever clipping legitimate data.
    if abs(cx) < 10 and abs(cy) < 10:
        return
    k = (cx, cy)
    if k not in cell_map:
        cell_map[k] = CellAgg()
    cell_map[k].vote(w == 1)


def _merge_actor(actors: dict[ActorKey, ActorAgg], a: dict, session_id: str) -> None:
    skin = a.get('skin')
    if not skin:
        return
    if _is_ignored_skin(skin):
        return
    rx = round(a.get('x', 0))
    ry = round(a.get('y', 0))
    floor = a.get('floor', 1)
    key = (skin, rx, ry, floor)
    if key not in actors:
        actors[key] = ActorAgg(
            skin=skin,
            kind=a.get('kind', '?'),
            x=a.get('x', 0),
            y=a.get('y', 0),
            z=a.get('z', 0),
            floor=floor,
            first_seen_session=session_id,
            first_seen_t=a.get('first_t', 0),
            type_id=a.get('type_id'),
            sno_id=a.get('sno_id'),
            radius=a.get('radius'),
            is_boss=a.get('is_boss'),
            is_elite=a.get('is_elite'),
        )
    agg = actors[key]
    agg.sessions_seen.add(session_id)
    agg.total_observations += int(a.get('samples', 1) or 1)


# ---------------------------------------------------------------------------
# Saturation heuristic
# ---------------------------------------------------------------------------

def is_saturated(agg: KeyAgg) -> tuple[bool, dict]:
    """
    A key is saturated when the last SATURATION_RUNS sessions each added
    fewer than SATURATION_NEW_CELLS new cells. Returns (saturated, stats).
    """
    history = agg.cells_history
    if len(history) < SATURATION_RUNS + 1:
        return False, {
            'sessions_merged': len(history),
            'reason': f'need {SATURATION_RUNS + 1} sessions',
        }
    deltas = []
    for i in range(len(history) - SATURATION_RUNS, len(history)):
        prev = history[i - 1][1] if i > 0 else 0
        curr = history[i][1]
        deltas.append(curr - prev)
    saturated = all(d < SATURATION_NEW_CELLS for d in deltas)
    return saturated, {
        'sessions_merged': len(history),
        'recent_new_cells': deltas,
        'threshold': SATURATION_NEW_CELLS,
    }


# ---------------------------------------------------------------------------
# Output emission
# ---------------------------------------------------------------------------

def emit_curated(out_dir: Path, agg: KeyAgg) -> Path:
    """Write a curated `<key>.json` for one merge key. Returns the path."""
    out_dir.mkdir(parents=True, exist_ok=True)

    # Compute bbox + actors-per-floor count for the loader's quick checks
    bbox = None
    cells_out_by_floor: dict[str, list[list[int]]] = {}
    for floor, cells in agg.cells_by_floor.items():
        rows: list[list[int]] = []
        for (cx, cy), agg_cell in cells.items():
            rows.append([
                cx, cy,
                1 if agg_cell.is_walkable else 0,
                round(agg_cell.confidence, 3),
                agg_cell.total,
            ])
            wx, wy = cx * agg.grid_resolution, cy * agg.grid_resolution
            if bbox is None:
                bbox = [wx, wy, wx, wy]
            else:
                if wx < bbox[0]: bbox[0] = wx
                if wy < bbox[1]: bbox[1] = wy
                if wx > bbox[2]: bbox[2] = wx
                if wy > bbox[3]: bbox[3] = wy
        cells_out_by_floor[str(floor)] = rows

    actors_out: list[dict] = []
    for key, a in agg.actors.items():
        d = {
            'skin': a.skin,
            'kind': a.kind,
            'x': a.x, 'y': a.y, 'z': a.z,
            'floor': a.floor,
            'sessions_seen': len(a.sessions_seen),
            'total_observations': a.total_observations,
        }
        if a.type_id is not None:  d['type_id'] = a.type_id
        if a.sno_id is not None:   d['sno_id']  = a.sno_id
        if a.radius is not None:   d['radius']  = a.radius
        if a.is_boss:              d['is_boss']  = True
        if a.is_elite:             d['is_elite'] = True
        actors_out.append(d)

    saturated, sat_info = is_saturated(agg)

    # Per-floor diagnostic block.  Mirrors the cells_out_by_floor key
    # convention (str(floor_idx)) so the viewer can index both maps
    # with the same key.  Sessions count rather than the full session
    # ID set -- the set can be 100+ entries on busy zones; consumers
    # don't need the IDs, just the count.
    floors_meta_out: dict[str, dict] = {}
    for fid, fm in agg.floors_meta.items():
        floors_meta_out[str(fid)] = {
            'worlds':        sorted(fm.worlds),
            'world_ids':     sorted(fm.world_ids),
            'sessions':      len(fm.sessions),
        }

    payload = {
        'schema_version': SCHEMA_VERSION_SUPPORTED,
        'key':       agg.key,
        'key_type':  agg.key_type,
        'merged_at': int(time.time()),
        'sessions_merged':  len(agg.sessions),
        'activity_kinds':   sorted(agg.activity_kinds),
        'saturated':        saturated,
        'saturation_info':  sat_info,
        # Top-level world identifiers seen across all sessions for this
        # merge key.  Lists rather than scalars because the same zone
        # name (e.g. an undercity dungeon) can host multiple worlds
        # across its floors.
        'worlds':           sorted(agg.worlds),
        'world_ids':        sorted(agg.world_ids),
        'grid': {
            'resolution': agg.grid_resolution,
            'bbox': bbox,
            'floors': cells_out_by_floor,
            # Per-floor world + session breakdown (NEW).  Same key set
            # as 'floors' so a viewer can iterate floors and pull both
            # the cell list and the meta with one loop.
            'floors_meta': floors_meta_out,
        },
        'actors': actors_out,
    }

    out_path = out_dir / f'{_safe_filename(agg.key)}.json'
    tmp = out_path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, indent=None, separators=(',', ':'))
    os.replace(tmp, out_path)

    # Also write a pre-compressed companion so /zones/{key} can serve the
    # gzipped bytes directly (Content-Encoding: gzip) without paying the
    # gzip CPU cost on every request.  With multiple uploaders pulling the
    # zone catalog in parallel, runtime gzip via GZipMiddleware was pegging
    # the server's CPU.  Pre-compression turns each /zones/{key} response
    # into a static-file read + sendfile -- effectively free.
    import gzip
    gz_path = out_dir / f'{_safe_filename(agg.key)}.json.gz'
    gz_tmp  = gz_path.with_suffix('.gz.tmp')
    with gz_tmp.open('wb') as f:
        # mtime=0 makes the .gz reproducible (same bytes for same JSON);
        # avoids spurious If-Modified-Since misses when the JSON content
        # didn't actually change.  compresslevel=6 is the gzip default
        # (good size/speed balance).
        with gzip.GzipFile(fileobj=f, mode='wb', mtime=0, compresslevel=6) as gz:
            with out_path.open('rb') as src:
                gz.write(src.read())
    os.replace(gz_tmp, gz_path)
    return out_path


def _safe_filename(key: str) -> str:
    return ''.join(c if c.isalnum() or c in ('_', '-', '.') else '_' for c in key)


# ---------------------------------------------------------------------------
# Saturated.json (consumed by the recorder to skip already-mapped zones)
# ---------------------------------------------------------------------------

def emit_saturated(out_dir: Path, state: dict[str, KeyAgg]) -> Path:
    saturated_zones = []
    saturated_pit_worlds = []
    for key, agg in state.items():
        sat, _ = is_saturated(agg)
        if not sat:
            continue
        if agg.key_type == 'zone':
            saturated_zones.append(key)
        elif agg.key_type == 'pit_world':
            saturated_pit_worlds.append(key)
    out = {
        'updated_at': int(time.time()),
        'zones':      sorted(saturated_zones),
        'pit_worlds': sorted(saturated_pit_worlds),
    }
    path = out_dir / 'saturated.json'
    tmp = path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)
    os.replace(tmp, path)
    return path


# ---------------------------------------------------------------------------
# Coverage report (human-readable summary, emitted next to data/zones/)
# ---------------------------------------------------------------------------

def emit_coverage(out_dir: Path, state: dict[str, KeyAgg]) -> Path:
    rows = []
    for key in sorted(state.keys()):
        agg = state[key]
        sat, info = is_saturated(agg)
        cells_total = sum(len(m) for m in agg.cells_by_floor.values())
        rows.append({
            'key': key,
            'key_type': agg.key_type,
            'sessions': len(agg.sessions),
            'cells': cells_total,
            'actors': len(agg.actors),
            'saturated': sat,
            'recent_new_cells': info.get('recent_new_cells'),
            'activity_kinds': sorted(agg.activity_kinds),
        })
    path = out_dir / 'coverage.json'
    tmp = path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump({'updated_at': int(time.time()), 'keys': rows}, f, indent=2)
    os.replace(tmp, path)
    return path


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def find_repo_paths() -> tuple[Path, Path, Path]:
    """Returns (dumps_dir, data_zones_dir, sidecar_drop_dir)."""
    here = Path(__file__).resolve().parent
    repo = here.parent.parent     # WarMap/tools/merger -> WarMap/
    scripts = repo.parent / 'scripts'
    dumps = scripts / 'WarMapRecorder' / 'dumps'
    data = repo / 'data' / 'zones'
    sidecar = scripts / 'WarMapData'
    return dumps, data, sidecar


def _parse_with_cache(fp: Path) -> Optional[Record]:
    """parse_ndjson + a (path, mtime) cache.  On cache hit returns the
    previously-parsed Record without touching the disk.  On miss / stale
    mtime, parses fresh and stores.  Eviction happens in merge_all when
    a file disappears from the dumps dir."""
    try:
        mt = fp.stat().st_mtime
    except OSError:
        return None
    key = str(fp)
    cached = _PARSE_CACHE.get(key)
    if cached and cached[0] == mt:
        return cached[1]
    rec = parse_ndjson(fp) if fp.suffix == '.ndjson' else _parse_legacy(fp)
    # Cache the result even if rec is None -- avoids re-parsing files that
    # consistently fail to parse (corrupted dumps, missing header, etc.).
    if len(_PARSE_CACHE) > _PARSE_CACHE_MAX:
        # Should never realistically hit this; bound just in case.
        _PARSE_CACHE.clear()
    _PARSE_CACHE[key] = (mt, rec)
    return rec


def merge_all(dumps: Path, only_complete: bool = True) -> dict[str, KeyAgg]:
    """Walk the dumps dir, fold each into the per-zone aggregate state,
    return the state.

    Incremental fast path: when this is called repeatedly within the
    same Python process (i.e. the FastAPI scheduler-winning worker), we
    reuse the cached state from the previous call and only fold in
    files that are new or have a changed mtime.  Files that disappeared
    since last call trigger a full rebuild (we can't cleanly subtract a
    record's contribution from an aggregate).

    Also tracks which zone keys were touched this call -- caller can
    use that to do a selective emit of only the changed zone JSONs.
    See `merger.last_touched_keys` for the most-recent-call value.
    """
    global _STATE_CACHE, _LAST_FILES

    t0 = time.time()
    files = sorted(dumps.glob('*.ndjson'))
    files += sorted(dumps.glob('*.json'))     # legacy single-blob format
    file_set = {str(f) for f in files}

    # Decide between full rebuild vs incremental.  Full rebuild fires
    # when (a) we have no cached state or (b) any file was removed since
    # the previous call (quarantine, manual delete).  In case (b) we
    # also evict the parse cache for the dropped paths.
    removed = _LAST_FILES - file_set
    if removed:
        for k in removed:
            _PARSE_CACHE.pop(k, None)
    do_rebuild = (_STATE_CACHE is None) or bool(removed)

    if do_rebuild:
        state: dict[str, KeyAgg] = {}
        candidates = files          # process every file
        mode = 'rebuild'
    else:
        # Reuse last cycle's aggregated state.  We mutate in place; if
        # this turns out to need a snapshot (concurrent emit), copy here.
        state = _STATE_CACHE  # type: ignore[assignment]
        # Only walk files that are newly added or whose mtime changed
        # since their cached parse.  parse_ndjson itself short-circuits
        # via the parse cache so unchanged files cost ~one stat() call.
        candidates = []
        for fp in files:
            try:
                mt = fp.stat().st_mtime
            except OSError:
                continue
            cached = _PARSE_CACHE.get(str(fp))
            if not cached or cached[0] != mt:
                candidates.append(fp)
        mode = 'incremental'

    print(f'[merge] {mode}: {len(candidates)} candidate / {len(files)} total files in {dumps}')
    accepted = 0
    skipped = 0
    touched_keys: set[str] = set()

    for fp in candidates:
        rec = _parse_with_cache(fp)
        if rec is None:
            skipped += 1
            continue
        if rec.schema_version != SCHEMA_VERSION_SUPPORTED:
            print(f'  - {fp.name}: unsupported schema_version={rec.schema_version}, skipping')
            skipped += 1
            continue
        if only_complete and not rec.complete:
            # In-progress session; skip until it has a footer line
            continue
        touched = merge_record_into(state, rec)
        if touched:
            accepted += 1
            for k in touched:
                touched_keys.add(k)
            print(f'  + {fp.name}: {rec.activity_kind}/{rec.zone} -> {", ".join(touched)}')

    _STATE_CACHE = state
    _LAST_FILES  = file_set
    # Stash the touched-zones set on the module so callers (emit_all) can
    # use it for selective re-emission.  When mode='rebuild' we touched
    # everything by definition -- empty set means "emit nothing" which
    # would be wrong, so signal that here.
    merge_all.last_touched_keys = touched_keys if mode == 'incremental' else set(state.keys())
    merge_all.last_mode         = mode

    elapsed = time.time() - t0
    print(f'[merge] {mode}: accepted {accepted}, skipped {skipped}, touched={len(touched_keys)} zones in {elapsed:.2f}s')
    return state


# Module-level holders for the most recent call's metadata (avoids a
# return-type change to merge_all that'd ripple into older callers).
merge_all.last_touched_keys = set()  # type: ignore[attr-defined]
merge_all.last_mode         = ''     # type: ignore[attr-defined]


def reset_merge_state() -> None:
    """Clear the parse + state caches.  Forces the next merge_all call
    to do a full rebuild from disk.  Useful after admin operations
    (quarantine, zone reset) where the in-memory state can't be
    incrementally fixed."""
    global _STATE_CACHE, _LAST_FILES
    _PARSE_CACHE.clear()
    _STATE_CACHE = None
    _LAST_FILES  = set()


def _parse_legacy(path: Path) -> Optional[Record]:
    """Parse the old single-blob JSON format into a Record (best-effort)."""
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    grid_by_floor: dict[int, list[tuple[int, int, int]]] = collections.defaultdict(list)
    grid_res = 0.5
    if isinstance(data.get('geometry'), dict):
        g = data['geometry'].get('grid')
        if isinstance(g, dict):
            grid_res = g.get('resolution', 0.5)
            for fk, cells in (g.get('floors') or {}).items():
                grid_by_floor[int(fk)] = [(c[0], c[1], c[2]) for c in cells]
    actors = []
    if isinstance(data.get('geometry'), dict):
        actors = data['geometry'].get('actors') or []
    fw  = {1: data.get('world', '')}
    fwi = {1: data.get('world_id', 0)}
    for ev in (data.get('events') or []):
        if ev.get('kind') == 'floor_change':
            meta = ev.get('metadata') or {}
            tf, tw, twid = (meta.get('to_floor'),
                            meta.get('to_world'),
                            meta.get('to_world_id'))
            if tf and tw:
                fw[tf] = tw
            if tf and twid:
                fwi[tf] = twid
    return Record(
        schema_version=data.get('schema_version', 0),
        session_id=data.get('session_id', ''),
        activity_kind=data.get('activity_kind', ''),
        zone=data.get('zone', ''),
        world=data.get('world', ''),
        world_id=data.get('world_id', 0),
        started_at=data.get('started_at', 0),
        ended_at=data.get('ended_at', 0),
        complete=True,                                    # legacy = always complete
        floor_worlds=fw,
        floor_world_ids=fwi,
        samples=data.get('samples') or [],
        events=data.get('events') or [],
        actors=actors,
        grid_cells_by_floor=dict(grid_by_floor),
        grid_resolution=grid_res,
    )


def emit_actor_index(out_dir: Path, state: dict[str, KeyAgg]) -> Path:
    """
    Universal actor index: { skin: [ {key, kind, x, y, z, floor}, ... ], ... }

    A single skin can appear in multiple zones (e.g., the Iron Wolves
    Pit-key Crafter exists in both Cerrigar and Skov_Temis); we keep the
    full list so the travel planner can pick the closest one.

    Also emits a parallel `kinds` index: { kind: [ {skin, key, x, y, z}, ... ] }
    so consumers can ask "any vendor in any zone" without doing the
    skin-to-kind join themselves.
    """
    by_skin: dict[str, list] = collections.defaultdict(list)
    by_kind: dict[str, list] = collections.defaultdict(list)
    for key, agg in state.items():
        for entry in agg.actors.values():
            row_skin = {
                'key':    key,
                'kind':   entry.kind,
                'x':      entry.x,
                'y':      entry.y,
                'z':      entry.z,
                'floor':  entry.floor,
                'sessions_seen': len(entry.sessions_seen),
            }
            by_skin[entry.skin].append(row_skin)
            row_kind = dict(row_skin)
            row_kind['skin'] = entry.skin
            by_kind[entry.kind].append(row_kind)

    payload = {
        'updated_at': int(time.time()),
        'by_skin': by_skin,
        'by_kind': by_kind,
    }
    path = out_dir / '_actor_index.json'
    tmp = path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, indent=None, separators=(',', ':'))
    os.replace(tmp, path)
    return path


def emit_all(state: dict[str, KeyAgg], data_dir: Path, sidecar_dir: Path,
             only_keys: Optional[set[str]] = None) -> None:
    """Write merged JSON files.

    `only_keys` (when given) restricts the per-zone curated emission to
    just those keys -- typical case is "the zones that picked up new
    data this cycle" from merge_all.last_touched_keys.  The global
    summaries (coverage, actor index, saturated) are still emitted
    every call since they're cheap and any zone change can affect them.

    Pass only_keys=None or an empty set to mean "emit every zone in
    state" (e.g. after a full rebuild).
    """
    t0 = time.time()
    if not state:
        print('[emit] no aggregated data, nothing to write')
        return
    # only_keys semantics:
    #   None       -> emit everything (default, what the legacy callers want)
    #   set([..])  -> emit just those zones + still re-emit globals
    #   set()      -> nothing changed this cycle, skip the whole emit
    if only_keys is None:
        keys_to_emit: Iterable[str] = state.keys()
    elif not only_keys:
        print('[emit] no zones changed; skipping all writes')
        return
    else:
        # Intersect with state -- guards against stale keys in only_keys.
        keys_to_emit = [k for k in only_keys if k in state]

    emitted = 0
    for key in keys_to_emit:
        agg = state[key]
        path = emit_curated(data_dir, agg)
        emitted += 1
        cells = sum(len(m) for m in agg.cells_by_floor.values())
        print(f'  -> {path}  cells={cells} actors={len(agg.actors)} sessions={len(agg.sessions)}')

    cov = emit_coverage(data_dir, state)
    print(f'  -> {cov}')
    idx = emit_actor_index(data_dir, state)
    print(f'  -> {idx}')

    sidecar_dir.mkdir(parents=True, exist_ok=True)
    sat = emit_saturated(sidecar_dir, state)
    print(f'  -> {sat} (consumed by recorder)')
    print(f'[emit] wrote {emitted} curated zones in {time.time()-t0:.2f}s')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='WarMap merger.')
    parser.add_argument('--dumps', type=Path, help='dumps directory (default: ../../scripts/WarMapRecorder/dumps)')
    parser.add_argument('--out',   type=Path, help='output data/zones directory (default: ../../data/zones)')
    parser.add_argument('--sidecar', type=Path, help='sidecar drop directory for saturated.json (default: ../../scripts/WarMapData)')
    parser.add_argument('--once',  action='store_true', help='one-shot merge then exit')
    parser.add_argument('--watch', action='store_true', help='one initial pass + watch for new dumps')
    parser.add_argument('--include-incomplete', action='store_true', help='also merge sessions without a footer (live partial sessions)')
    args = parser.parse_args(argv)

    default_dumps, default_data, default_sidecar = find_repo_paths()
    dumps   = args.dumps   or default_dumps
    data    = args.out     or default_data
    sidecar = args.sidecar or default_sidecar

    if not dumps.exists():
        print(f'dumps directory not found: {dumps}', file=sys.stderr)
        return 1

    print(f'[merge] dumps:   {dumps}')
    print(f'[merge] data:    {data}')
    print(f'[merge] sidecar: {sidecar}')

    if args.once or not args.watch:
        state = merge_all(dumps, only_complete=not args.include_incomplete)
        emit_all(state, data, sidecar)
        return 0

    # Watch mode -- requires watchdog
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        print('watchdog not installed. Run: pip install watchdog', file=sys.stderr)
        return 2

    state = merge_all(dumps, only_complete=not args.include_incomplete)
    emit_all(state, data, sidecar)

    pending: set[str] = set()
    last_run = 0.0

    class Handler(FileSystemEventHandler):
        def on_modified(self, event):
            if event.is_directory:
                return
            if event.src_path.endswith(('.json', '.ndjson')):
                pending.add(event.src_path)

        def on_created(self, event):
            if not event.is_directory and event.src_path.endswith(('.json', '.ndjson')):
                pending.add(event.src_path)

    obs = Observer()
    obs.schedule(Handler(), str(dumps), recursive=False)
    obs.start()
    print(f'[merge] watching {dumps} (Ctrl-C to stop)')

    try:
        while True:
            time.sleep(2.0)
            now = time.time()
            if pending and (now - last_run) > 3.0:
                pending.clear()
                last_run = now
                print(f'\n[merge] re-running due to file change')
                state = merge_all(dumps, only_complete=not args.include_incomplete)
                emit_all(state, data, sidecar)
    except KeyboardInterrupt:
        print('\n[merge] stopping')
    finally:
        obs.stop()
        obs.join()
    return 0


if __name__ == '__main__':
    sys.exit(main())
