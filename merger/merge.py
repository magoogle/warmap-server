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
    # Zone-link breadcrumb: how the player got to THIS zone.  The
    # recorder writes header.entered_via on zone-change transitions
    # when it can heuristically identify the source actor (a portal /
    # dungeon entrance / waypoint within ~5m of the player's last
    # position before the transition).  Server-side aggregation turns
    # these into a per-zone outbound link table.
    entered_via: Optional[dict] = None

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
        entered_via=header.get('entered_via'),
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
    """Aggregated geometry + actors for one merge key (zone or pit-world).

    PER-FLOOR KEYING (important).  Both `cells_by_floor` and
    `floors_meta` are keyed by *world_id* (the host's stable per-world
    hash), NOT by session-local floor index.  This is the fix for the
    cross-session floor-counter drift bug:  the recorder's floor counter
    is session-internal (1..N as the session enters new world_ids), so
    two sessions visiting the same multi-world zone in different orders
    used to assign different floor numbers to the same world -- a boss
    from world_03 ended up on what one session called floor 2 and
    another session called floor 3, and the merger mashed them
    together.

    Keying by world_id gives every world a stable slot regardless of
    when a given session reached it.  At emit time we sort the
    world_ids by their representative world name (alphabetical) and
    assign 1..N floor numbers deterministically -- so SnakeTemple_01
    is always floor 1, _02 always floor 2, _03 always floor 3, no
    matter what order any session entered them.

    Legacy/missing world_id (older dumps without to_world_id in their
    floor_change events): bucketed under negative integer keys
    (-floor_idx) to avoid collision with real world_ids; sorted last
    at emit time."""
    key: str
    key_type: str                                      # 'zone' or 'pit_world'
    grid_resolution: float = 0.5
    # cells_by_floor[world_id_or_neg_legacy][(cx, cy)] -> CellAgg
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
    # Per-floor breakdown (world_id -> FloorMetaAgg).  defaultdict so
    # callers can floors_meta[k].worlds.add(...) without a key check.
    floors_meta: dict[int, FloorMetaAgg] = field(
        default_factory=lambda: collections.defaultdict(FloorMetaAgg))
    # Saturation tracking: history of cells_total after each session merge
    cells_history: list[tuple[int, int]] = field(default_factory=list)
                                                    # [(unix_ts, cells_total), ...]
    # Outbound zone-link graph.  An "outbound link" answers "where
    # can I go FROM this zone?" -- a record for zone B with
    # entered_via.from_zone == A creates an outbound link on A's
    # KeyAgg with destination=B.  Built incrementally from
    # `header.entered_via` breadcrumbs across every session.
    # Key: (source_actor_skin, rounded source_x, rounded source_y,
    # to_zone) -- so the same portal at the same coords leading to
    # the same destination dedupes to one edge.  Value: dict with
    # count, first_seen, last_seen, source actor coords + sno_id
    # + kind, destination zone.  Surfaced via the new
    # /zones/{key}/links endpoint.
    outbound_links: dict[tuple, dict] = field(default_factory=dict)


def _resolve_floor_key(rec: Record, floor_idx: int) -> int:
    """Map a session-local floor_idx to a stable per-world key for the
    KeyAgg's cells_by_floor / floors_meta dicts.

    Preference order:
      1. rec.floor_world_ids[floor_idx]  -- explicit per-floor world_id
         from a floor_change event's `to_world_id` metadata.  Always
         present for floor 1 (set from header.world_id) and for any
         floor reached via a floor_change event in a recorder that
         carries to_world_id (post-WarMapRecorder@cf3a8df ish).
      2. rec.world_id                    -- header's world_id, valid
         only for floor 1 (the entry floor); falling back to this for
         later floors would conflate them.
      3. -floor_idx                      -- legacy sentinel for old
         dumps that lacked to_world_id.  Negative so it can't collide
         with a real world_id (which are unsigned 32-bit hashes).

    This is the single chokepoint where the recorder's session-local
    floor counter gets translated to a global stable key; everywhere
    else in the merger keys are world_ids."""
    wid = rec.floor_world_ids.get(floor_idx)
    if wid:
        return wid
    if floor_idx == 1 and rec.world_id:
        return rec.world_id
    return -int(floor_idx)


def merge_record_into(state: dict[str, KeyAgg], rec: Record) -> list[str]:
    """
    Merge record into the appropriate KeyAgg(s). Returns the list of keys
    that were touched (a pit record can touch many).
    """
    touched: list[str] = []

    if rec.activity_kind == 'pit':
        # Each floor's cells/actors belong to that floor's WORLD (template).
        # Pit `key = world name` -> each pit_world is its own merge bucket;
        # within the bucket there's effectively one floor.  We still key
        # cells_by_floor by the world_id so the emit path is uniform
        # with zone-keyed records.
        for floor_idx, cells in rec.grid_cells_by_floor.items():
            world = rec.floor_worlds.get(floor_idx)
            if not world:
                continue
            key = world
            agg = _get_or_create(state, key, 'pit_world')
            agg.activity_kinds.add('pit')
            agg.sessions.add(rec.session_id)
            agg.grid_resolution = rec.grid_resolution
            agg.worlds.add(world)
            wid = rec.floor_world_ids.get(floor_idx)
            if wid:
                agg.world_ids.add(wid)
            floor_key = _resolve_floor_key(rec, floor_idx)
            fm = agg.floors_meta[floor_key]
            fm.worlds.add(world)
            if wid: fm.world_ids.add(wid)
            fm.sessions.add(rec.session_id)
            cell_map = agg.cells_by_floor[floor_key]
            for cx, cy, w in cells:
                _vote_cell(cell_map, cx, cy, w)
            # Re-tag actors with the resolved floor key so _merge_actor's
            # ActorKey dedup is per-world rather than per-session-floor.
            for a in rec.actors:
                if a.get('floor') == floor_idx:
                    _merge_actor(agg.actors, a, rec.session_id, floor_key)
            touched.append(key)

    elif rec.activity_kind in ZONE_KEYED_ACTIVITIES:
        key = rec.zone
        agg = _get_or_create(state, key, 'zone')
        agg.activity_kinds.add(rec.activity_kind)
        agg.sessions.add(rec.session_id)
        agg.grid_resolution = rec.grid_resolution
        # Top-level worlds is the union of (header world) + (every per-
        # floor world we observed via floor_change events).
        if rec.world:    agg.worlds.add(rec.world)
        if rec.world_id: agg.world_ids.add(rec.world_id)
        for w in rec.floor_worlds.values():
            if w: agg.worlds.add(w)
        for wid in rec.floor_world_ids.values():
            if wid: agg.world_ids.add(wid)

        # Resolve every session-local floor_idx -> stable bucket key
        # (world_id or legacy sentinel) up front so cells, actors, and
        # floors_meta all agree on the same per-floor identity.
        floor_keys: dict[int, int] = {
            f: _resolve_floor_key(rec, f)
            for f in rec.grid_cells_by_floor.keys()
        }
        # Also include floors that show up only in actor entries (rare,
        # but possible when a session captured an actor on a floor it
        # never sampled cells for).
        for a in rec.actors:
            f = a.get('floor', 1)
            if f not in floor_keys:
                floor_keys[f] = _resolve_floor_key(rec, f)

        for floor_idx, floor_key in floor_keys.items():
            fm = agg.floors_meta[floor_key]
            fw  = rec.floor_worlds.get(floor_idx)    or rec.world
            fwi = rec.floor_world_ids.get(floor_idx) or rec.world_id
            if fw:  fm.worlds.add(fw)
            if fwi: fm.world_ids.add(fwi)
            fm.sessions.add(rec.session_id)
        for floor_idx, cells in rec.grid_cells_by_floor.items():
            cell_map = agg.cells_by_floor[floor_keys[floor_idx]]
            for cx, cy, w in cells:
                _vote_cell(cell_map, cx, cy, w)
        for a in rec.actors:
            f = a.get('floor', 1)
            _merge_actor(agg.actors, a, rec.session_id, floor_keys.get(f, _resolve_floor_key(rec, f)))
        touched.append(key)

    else:
        # Unknown activity kind -- skip.
        return []

    # ---- Zone-link breadcrumb -----------------------------------------
    # Recorder stamps `entered_via` into the new record's header at zone
    # change time -- "I came from zone X via actor Y at coords (a, b)".
    # That breadcrumb belongs as an OUTBOUND link on zone X's KeyAgg
    # (it's how to LEAVE X, not how to enter the new zone).  We populate
    # X's outbound_links here regardless of whether X's KeyAgg already
    # exists -- _get_or_create makes one as needed; if no actual record
    # for X has merged yet the outbound_links are the only contribution
    # and that's still useful data.
    ev = rec.entered_via
    if ev and ev.get('from_zone') and ev.get('actor_skin'):
        from_key  = ev['from_zone']
        from_agg  = state.get(from_key) or _get_or_create(state, from_key, 'zone')
        link_key  = (
            ev.get('actor_skin'),
            int(round(ev.get('actor_x') or 0)),
            int(round(ev.get('actor_y') or 0)),
            rec.zone,
        )
        link = from_agg.outbound_links.get(link_key)
        started = rec.started_at or int(time.time())
        if not link:
            link = {
                'to_zone':       rec.zone,
                'to_world':      rec.world,
                'to_world_id':   rec.world_id,
                'actor_skin':    ev.get('actor_skin'),
                'actor_kind':    ev.get('actor_kind'),
                'actor_sno':     ev.get('actor_sno'),
                'actor_type_id': ev.get('actor_type_id'),
                'actor_x':       ev.get('actor_x'),
                'actor_y':       ev.get('actor_y'),
                'actor_z':       ev.get('actor_z'),
                'actor_floor':   ev.get('actor_floor'),
                'first_seen':    started,
                'last_seen':     started,
                'count':         0,
            }
            from_agg.outbound_links[link_key] = link
        link['count']    += 1
        link['last_seen'] = max(link['last_seen'] or 0, started)
        if from_key not in touched:
            # Make sure from-zone gets re-emitted on this cycle so its
            # links.json reflects the new edge (or new count).
            touched.append(from_key)

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


def _merge_actor(actors: dict[ActorKey, ActorAgg], a: dict, session_id: str,
                 floor_key: Optional[int] = None) -> None:
    """Dedup an actor entry into the per-zone aggregate.

    `floor_key` is the resolved per-world bucket key (world_id, or the
    legacy negative sentinel) -- callers pass it from
    `_resolve_floor_key`.  When omitted (legacy callers / direct
    invocation by tests), falls back to the actor's session-local
    `floor` field; that path is only safe for single-floor zones since
    multi-world zones produce drift across sessions.

    The ActorKey carries the floor_key (not session-local floor),
    which is the whole point of the post-bug-fix keying: a boss
    sighting in world_03 always lands in the same ActorAgg regardless
    of whether the session called that world floor 2 or floor 3."""
    skin = a.get('skin')
    if not skin:
        return
    if _is_ignored_skin(skin):
        return
    rx = round(a.get('x', 0))
    ry = round(a.get('y', 0))
    if floor_key is None:
        floor_key = a.get('floor', 1)
    key = (skin, rx, ry, floor_key)
    if key not in actors:
        actors[key] = ActorAgg(
            skin=skin,
            kind=a.get('kind', '?'),
            x=a.get('x', 0),
            y=a.get('y', 0),
            z=a.get('z', 0),
            floor=floor_key,
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

def _compute_wall_dist(walkable: list[tuple[int, int]]) -> dict[tuple[int, int], int]:
    """4-connected BFS from each walkable cell that has at least one
    non-walkable / unmapped 4-neighbor.  Returns `{(cx, cy): distance}`
    for every walkable cell; distance 1 = "directly adjacent to a
    wall", larger = "deeper into the room".

    Mirrors the algorithm in WarPath's core/centerline.lua exactly so
    the precomputed values are interchangeable with what the plugin
    used to build locally.  We do it here on the server (Python, runs
    once per zone per merge cycle) so WarPath can skip the 1.4-second
    in-Lua BFS on big zones -- it just reads cell[3] directly from
    the nav file.

    Cost: linear in walkable cell count.  For Hawe_Verge (~620k
    walkable cells) this is well under a second in CPython.
    """
    cell_set = set(walkable)
    dist: dict[tuple[int, int], int] = {}
    queue: list[tuple[int, int, int]] = []
    ADJ = ((1, 0), (-1, 0), (0, 1), (0, -1))
    # Seed: every walkable cell with at least one non-walkable neighbor
    # gets distance 1 (it's "on the wall edge").
    for (cx, cy) in walkable:
        for dx, dy in ADJ:
            if (cx + dx, cy + dy) not in cell_set:
                dist[(cx, cy)] = 1
                queue.append((cx, cy, 1))
                break
    # BFS outward.  Head pointer instead of pop(0) so it stays O(N).
    head = 0
    while head < len(queue):
        cx, cy, d = queue[head]
        head += 1
        nd = d + 1
        for dx, dy in ADJ:
            nx, ny = cx + dx, cy + dy
            nk = (nx, ny)
            if nk in cell_set and nk not in dist:
                dist[nk] = nd
                queue.append((nx, ny, nd))
    return dist


def _split_cells_into_clusters(
        cell_map: dict[CellKey, CellAgg],
        grid_resolution: float) -> list[dict[CellKey, CellAgg]]:
    """Split a single per-world cell bucket into spatially-disjoint
    sub-buckets (one per physical room).

    Why: the host returns the same world_id for some sub-areas of a
    larger zone (notably undercity hub rooms vs their boss-room
    sub-areas reachable via portal -- e.g. BugCave_03's two
    physically-separate rooms both report world_id=4201036991).  The
    recorder's floor counter eventually catches this via the
    teleport-distance heuristic, but the resulting floor_change
    events still tag both rooms with the same to_world_id, so the
    merger's per-world keying collapses them back into one bucket
    with cells from two different physical spaces.

    This pass runs at emit time (not merge time) so it operates on
    fully-aggregated cells across every contributing session, then
    splits if the cells form 2+ disjoint clusters.

    Algorithm: flood-fill on a coarse super-grid where each
    super-cell is BUCKET_CELLS x BUCKET_CELLS real cells.  Two
    super-cells are connected when adjacent (4-connectivity) AND
    both contain at least one cell.  Disjoint super-cell components
    => disjoint physical rooms.  Tuned so that BugCave's 5km gap
    between rooms always splits while a single room with
    ~few-hundred-cell gaps from unwalked corners stays unified.

    Returns a list of cell_map dicts in deterministic order
    (smallest-min-x cluster first).  Single-cluster input returns a
    one-element list with the original dict.
    """
    if not cell_map:
        return [cell_map]
    # 50 cells * 0.5m/cell = 25m per super-cell.  Two clusters at
    # 100m+ separation always split; a single room with random
    # unwalked gaps (typical 5-15m) stays together via 4-adjacency.
    BUCKET_CELLS = 50

    # Index cells by their super-cell coordinate.
    buckets: dict[tuple[int, int], list[CellKey]] = collections.defaultdict(list)
    for ck in cell_map:
        cx, cy = ck
        # Floor-divide for negative coords: Python's // already does
        # mathematical floor (cx=-5 with BUCKET=50 -> -1, correct).
        bx, by = cx // BUCKET_CELLS, cy // BUCKET_CELLS
        buckets[(bx, by)].append(ck)

    if len(buckets) == 1:
        return [cell_map]

    # Flood-fill connected components of super-cells.
    bucket_keys = set(buckets.keys())
    visited: set[tuple[int, int]] = set()
    components: list[set[tuple[int, int]]] = []
    for start in bucket_keys:
        if start in visited:
            continue
        component: set[tuple[int, int]] = set()
        stack = [start]
        while stack:
            b = stack.pop()
            if b in visited:
                continue
            visited.add(b)
            component.add(b)
            bx, by = b
            for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                neighbor = (bx + dx, by + dy)
                if neighbor in bucket_keys and neighbor not in visited:
                    stack.append(neighbor)
        components.append(component)

    if len(components) <= 1:
        return [cell_map]

    # Build sub-cell-maps from the components, keep only what's in
    # this component's super-cells.
    sub_maps: list[dict[CellKey, CellAgg]] = []
    for comp in components:
        sub: dict[CellKey, CellAgg] = {}
        for b in comp:
            for ck in buckets[b]:
                sub[ck] = cell_map[ck]
        sub_maps.append(sub)

    # Deterministic order: smallest-min-x cluster first.  Stable
    # across re-merges so emit floor numbers don't jiggle.
    sub_maps.sort(key=lambda m: min(ck[0] for ck in m))
    return sub_maps


@dataclass
class _LogicalFloor:
    """One emit floor.  May correspond 1:1 with an internal world_key
    (the common case) or may be one of N split clusters of a single
    world_key (when same-world-id rooms got mashed into one bucket
    by the recorder's floor counter)."""
    world_key:    int                          # source bucket key (world_id or sentinel)
    cluster_idx:  int                          # 0 when no split, 0..N-1 when split
    cells:        dict[CellKey, CellAgg]       # cells belonging to this floor
    worlds:       set[str]                     # source FloorMetaAgg.worlds
    world_ids:    set[int]                     # source FloorMetaAgg.world_ids
    sessions:     set[str]                     # source FloorMetaAgg.sessions
    cell_x_range: tuple[int, int] = (0, 0)     # for actor placement bbox check


def _build_floor_layout(agg: KeyAgg) -> list[_LogicalFloor]:
    """Compute the ordered list of emit floors for a KeyAgg.

    1. For each internal world_key bucket, run cluster-split.  Most
       buckets produce one cluster; the bug-fix targets the few that
       produce multiple (same-world-id rooms physically separated).
    2. Each cluster becomes one _LogicalFloor.
    3. Order: by representative world name alpha (D4's NN suffixes
       sort logically), then by cluster's leftmost cell-x (so split
       rooms within the same world also sort deterministically).
       Worlds-without-names + legacy-sentinel keys sort last,
       preserving the prior behavior for old dumps.

    The returned list is the canonical emit ordering -- the index
    in the list is the emit floor number minus 1.  Both emit_curated
    and emit_actor_index use this so per-zone JSON and the universal
    actor index agree on floor numbers.
    """
    # Collect every key that contributes anything (cells, meta, or actors).
    all_keys = set(agg.cells_by_floor.keys()) | set(agg.floors_meta.keys())
    for a in agg.actors.values():
        all_keys.add(a.floor)

    floors: list[_LogicalFloor] = []
    for wkey in all_keys:
        cells = agg.cells_by_floor.get(wkey) or {}
        fm    = agg.floors_meta.get(wkey, FloorMetaAgg())
        clusters = _split_cells_into_clusters(cells, agg.grid_resolution)
        for ci, sub_map in enumerate(clusters):
            xs = [ck[0] for ck in sub_map] if sub_map else [0]
            floors.append(_LogicalFloor(
                world_key=wkey,
                cluster_idx=ci,
                cells=sub_map,
                worlds=set(fm.worlds),
                world_ids=set(fm.world_ids),
                sessions=set(fm.sessions),
                cell_x_range=(min(xs), max(xs)),
            ))

    def _sort_key(f: _LogicalFloor) -> tuple:
        if f.worlds:
            # Bucket 0: named worlds, alpha-sorted; cluster_idx breaks ties.
            return (0, sorted(f.worlds)[0], f.cell_x_range[0])
        if f.world_key < 0:
            # Bucket 1: legacy floor-idx fallback, preserve session-local order.
            return (1, -f.world_key, f.cell_x_range[0])
        # Bucket 2: world_id without a known name -- numeric.
        return (2, f.world_key, f.cell_x_range[0])

    floors.sort(key=_sort_key)
    return floors


def _build_actor_floor_lookup(
        floors: list[_LogicalFloor],
        grid_resolution: float) -> dict[tuple[int, int, int], int]:
    """Map (rounded_cx, rounded_cy, world_key) -> emit_idx for actor
    placement.  Only meaningful when a world_key got split into 2+
    clusters; for single-cluster floors all actors land on the same
    emit_idx anyway.

    We index by cell coord rather than by raw (x, y) so an actor's
    position rounds into the same cell as the surrounding walkable
    cells -- otherwise nearby-but-not-on-a-cell positions would
    miss their cluster.
    """
    lookup: dict[tuple[int, int, int], int] = {}
    for emit_idx, f in enumerate(floors, start=1):
        for (cx, cy) in f.cells:
            lookup[(cx, cy, f.world_key)] = emit_idx
    return lookup


def _emit_floor_for_actor(
        a: ActorAgg,
        floors: list[_LogicalFloor],
        actor_lookup: dict[tuple[int, int, int], int],
        grid_resolution: float) -> int:
    """Resolve an actor's emit floor.  Look up its cell first; on
    miss (actor sitting in an unwalked-cell tile, or a coord that
    rounded just outside any cluster's cells), fall back to nearest-
    cluster-by-x within the same world_key."""
    cx = round(a.x / grid_resolution)
    cy = round(a.y / grid_resolution)
    direct = actor_lookup.get((cx, cy, a.floor))
    if direct is not None:
        return direct
    # Fallback: among floors with the same world_key, pick the one whose
    # cell-x range contains the actor's cx (or is closest to it).
    candidates = [(i + 1, f) for i, f in enumerate(floors) if f.world_key == a.floor]
    if not candidates:
        # No matching world_key (e.g. actor on a floor with no cells at all).
        # Just pick floor 1 -- guaranteed a sane number, won't break the JSON.
        return 1
    best_emit = candidates[0][0]
    best_dist = abs(cx - candidates[0][1].cell_x_range[0])
    for emit_idx, f in candidates:
        lo, hi = f.cell_x_range
        if lo <= cx <= hi:
            return emit_idx     # exact bbox match
        d = min(abs(cx - lo), abs(cx - hi))
        if d < best_dist:
            best_dist = d
            best_emit = emit_idx
    return best_emit


def emit_curated(out_dir: Path, agg: KeyAgg) -> Path:
    """Write a curated `<key>.json` for one merge key. Returns the path."""
    out_dir.mkdir(parents=True, exist_ok=True)

    # ----- Floor layout (per-world buckets, optionally cluster-split) ----
    # KeyAgg.cells_by_floor is internally keyed by world_id (or a
    # negative legacy sentinel).  _build_floor_layout splits any bucket
    # whose cells form 2+ spatially-disjoint clusters into separate
    # logical floors -- handles undercity sub-rooms that share a
    # world_id (e.g. BugCave_03 hub vs BugCave_03 boss room).
    floors = _build_floor_layout(agg)
    actor_lookup = _build_actor_floor_lookup(floors, agg.grid_resolution)

    # Compute bbox + per-floor cell rows.
    bbox = None
    cells_out_by_floor: dict[str, list[list[int]]] = {}
    for emit_idx, f in enumerate(floors, start=1):
        rows: list[list[int]] = []
        for (cx, cy), agg_cell in f.cells.items():
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
        cells_out_by_floor[str(emit_idx)] = rows

    # Mobile-actor collapse.  Bosses + champions are single in-world
    # entities that move during their fight, so the recorder emits N
    # `actor` entries with the same skin/sno_id but different rounded
    # positions -- one per spot the boss happened to be when scanned.
    # Across multiple sessions this clusters into a starburst of dozens
    # of "Boss" pins in the boss room.  Visually noisy, semantically
    # wrong: there's exactly one boss.
    #
    # We collapse them here (server-side, applies uniformly to all
    # consumers) by grouping mobile-kind actors on the same floor with
    # the same (skin, sno_id) and emitting one centroid entry, weighted
    # by total_observations (so the centroid biases toward where the
    # boss spent the most TIME -- usually the room's intended center).
    # Pass-through unchanged for static kinds (interactables, doors,
    # shrines, etc.) -- those don't move and benefit from the per-
    # position dedup.
    MOBILE_KINDS = {'boss', 'champion'}

    actors_out: list[dict] = []
    # Cluster bucket: (skin, sno_id, emit_floor) -> list[ActorAgg].
    # Group by the actor's resolved EMIT floor (after cluster split),
    # not its raw world_key -- two physically-separate boss rooms with
    # the same world_id need their bosses kept apart, not centroided
    # together to a meaningless midpoint between rooms.
    mobile_clusters: dict[tuple, list[ActorAgg]] = {}
    for _key, a in agg.actors.items():
        emit_floor = _emit_floor_for_actor(a, floors, actor_lookup, agg.grid_resolution)
        if a.kind in MOBILE_KINDS and a.sno_id is not None:
            mobile_clusters.setdefault((a.skin, a.sno_id, emit_floor), []).append(a)
        else:
            d = {
                'skin': a.skin,
                'kind': a.kind,
                'x': a.x, 'y': a.y, 'z': a.z,
                'floor': emit_floor,
                'sessions_seen': len(a.sessions_seen),
                'total_observations': a.total_observations,
            }
            if a.type_id is not None:  d['type_id'] = a.type_id
            if a.sno_id is not None:   d['sno_id']  = a.sno_id
            if a.radius is not None:   d['radius']  = a.radius
            if a.is_boss:              d['is_boss']  = True
            if a.is_elite:             d['is_elite'] = True
            actors_out.append(d)

    # Emit one centroid per mobile cluster.  Single-entry clusters
    # (e.g. an elite that didn't move much, or a boss observed once)
    # behave identically to a pass-through emission -- the loop is
    # safe to use uniformly.
    for (skin, sno_id, emit_floor), entries in mobile_clusters.items():
        # Observation-weighted centroid.  Falls back to a uniform
        # average when total weight is 0 (shouldn't happen, but cheap
        # to handle defensively).
        total_w = sum(max(1, e.total_observations) for e in entries)
        wx = sum(e.x * max(1, e.total_observations) for e in entries) / total_w
        wy = sum(e.y * max(1, e.total_observations) for e in entries) / total_w
        wz = sum(e.z * max(1, e.total_observations) for e in entries) / total_w
        # Spread = max distance any sighting was from the centroid.
        # Useful both as a viewer hint (draw the boss diamond bigger
        # if it ranges over a wide arena) and as a diagnostic
        # ("a 30m spread on the same boss SNO -> floor detection
        # probably mis-grouped two separate rooms").
        spread = 0.0
        for e in entries:
            dx, dy = e.x - wx, e.y - wy
            d = (dx * dx + dy * dy) ** 0.5
            if d > spread: spread = d
        # Union session set + sum observations across the cluster.
        sessions_seen = set()
        total_obs = 0
        is_boss  = False
        is_elite = False
        type_id  = None
        radius   = None
        for e in entries:
            sessions_seen |= e.sessions_seen
            total_obs += e.total_observations
            if e.is_boss:  is_boss  = True
            if e.is_elite: is_elite = True
            if type_id is None and e.type_id is not None: type_id = e.type_id
            # Keep the largest non-null radius -- conservative for
            # rendering hit-tests on the viewer side.
            if e.radius is not None and (radius is None or e.radius > radius):
                radius = e.radius
        # Single-entry clusters skip the synthetic spread/centroid
        # math (it would round to the same coords anyway).
        ex0 = entries[0]
        d = {
            'skin': skin,
            'kind': ex0.kind,
            'x': round(wx, 1), 'y': round(wy, 1), 'z': round(wz, 1),
            'floor': emit_floor,
            'sessions_seen': len(sessions_seen),
            'total_observations': total_obs,
        }
        d['sno_id'] = sno_id
        if type_id is not None: d['type_id'] = type_id
        if radius  is not None: d['radius']  = radius
        if is_boss:  d['is_boss']  = True
        if is_elite: d['is_elite'] = True
        # Surface the cluster diagnostics for the viewer.  positions =
        # how many distinct rounded-position sightings collapsed into
        # this entry; spread_m = farthest sighting from the centroid.
        # Lets a viewer optionally draw a faint ring at radius=spread.
        if len(entries) > 1:
            d['cluster_positions'] = len(entries)
            d['cluster_spread_m'] = round(spread, 1)
        actors_out.append(d)

    saturated, sat_info = is_saturated(agg)

    # Per-floor diagnostic block.  Iterates the cluster-split layout
    # so a single world_key that produced N split floors gets N
    # entries with the SAME world_id list (correct -- they share a
    # world_id) but distinct emit floor numbers.  Surfaces a
    # 'split_of' field when this floor is one of multiple clusters
    # of the same world_key, so a viewer can flag "this is room 2
    # of 2 sub-areas with the same world_id".
    floors_meta_out: dict[str, dict] = {}
    # Count clusters per world_key so we know which entries are splits.
    clusters_per_wkey: dict[int, int] = collections.Counter(f.world_key for f in floors)
    for emit_idx, f in enumerate(floors, start=1):
        # cell_count = WALKABLE-cell count.  Counting walkable instead
        # of total (walkable + blocked) means status displays show the
        # number that's actually navigable, and matches what nav.json
        # ships in floors[fid].  WarPath's "X cells" status line is
        # the primary consumer.  full.json's cell_count is technically
        # higher (includes blocked) but a full-file consumer can
        # always count grid.floors[fid] directly if it cares.
        full_floor_cells = cells_out_by_floor.get(str(emit_idx)) or []
        walkable_count = sum(1 for c in full_floor_cells if c[2])
        entry = {
            'worlds':     sorted(f.worlds),
            'world_ids':  sorted(f.world_ids),
            'sessions':   len(f.sessions),
            'cell_count': walkable_count,
        }
        if clusters_per_wkey[f.world_key] > 1:
            # Diagnostic: this is one of N split clusters of a single
            # world_key.  cluster_idx is 0-based internally; surface
            # 1-based for human readability.
            entry['split_of'] = clusters_per_wkey[f.world_key]
            entry['split_idx'] = f.cluster_idx + 1
        floors_meta_out[str(emit_idx)] = entry

    # Outbound-link list: the per-edge dicts populated during
    # merge_record_into.  Sort by (count desc, to_zone asc) so the
    # most-trafficked exits are first -- a navigation planner can
    # take that as a confidence signal ("this edge was observed N
    # times across sessions => it's a real, reachable transition,
    # not noise from a one-off teleport bug").
    outbound_links_out = sorted(
        agg.outbound_links.values(),
        key=lambda l: (-(l.get('count') or 0), l.get('to_zone') or ''),
    )

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
        # Outbound-link graph: "from this zone, where can the player
        # go and via which actor?"  Built from header.entered_via
        # breadcrumbs across every session that ever LEFT this zone.
        'outbound_links':   outbound_links_out,
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

    # ---- Slim "meta" companion (everything EXCEPT grid.floors cells) ----
    # WarPath's plugin-loader was paying ~500ms parsing the full file on
    # every zone change in pure Lua, which felt like the game was
    # crashing on big zones (Hawe_Verge: 619k cells = 12MB JSON).  But
    # most zone changes never actually USE the cells -- they're only
    # touched by the lazy wall-distance BFS on the rare smooth_path
    # call.  Splitting the file lets WarPath load the metadata
    # synchronously (small, ~300KB max) and lazy-fetch the cells only
    # when wall_dist is actually needed.
    #
    # Meta payload = full payload with grid.floors replaced by an
    # empty placeholder + per-floor cell_count surfaced inside
    # floors_meta so consumers can still display "N cells" without
    # the cells themselves.
    meta_payload = dict(payload)
    meta_grid = dict(payload['grid'])
    # Empty dict (not removed) so consumers iterating grid.floors don't
    # KeyError; len() on each value just reports 0 in the meta-only
    # path.  cell_count for the status display is already in
    # floors_meta (added in the shared payload-building loop above).
    meta_grid['floors'] = {fid: [] for fid in cells_out_by_floor.keys()}
    meta_payload['grid'] = meta_grid
    # Marker bit so a consumer reading the meta file knows "cells
    # weren't included; fetch the full JSON if you actually need them."
    meta_payload['cells_omitted'] = True

    meta_path = out_dir / f'{_safe_filename(agg.key)}.meta.json'
    meta_tmp  = meta_path.with_suffix('.json.tmp')
    with meta_tmp.open('w', encoding='utf-8') as f:
        json.dump(meta_payload, f, indent=None, separators=(',', ':'))
    os.replace(meta_tmp, meta_path)

    # ---- Standalone outbound-link graph (`<key>.links.json`) -----------
    # Just the from-zone identity + outbound links list, slim enough
    # for a navigation planner to fetch one-per-zone-per-route-step
    # without pulling cells/actors metadata.  Served by /zones/{key}/links.
    links_payload = {
        'schema_version': SCHEMA_VERSION_SUPPORTED,
        'key':            agg.key,
        'key_type':       agg.key_type,
        'merged_at':      payload['merged_at'],
        'outbound_links': outbound_links_out,
    }
    links_path = out_dir / f'{_safe_filename(agg.key)}.links.json'
    links_tmp  = links_path.with_suffix('.json.tmp')
    with links_tmp.open('w', encoding='utf-8') as f:
        json.dump(links_payload, f, indent=None, separators=(',', ':'))
    os.replace(links_tmp, links_path)

    # ---- Navigation-only companion (`<key>.nav.json`) -------------------
    # Tailored for the WarPath plugin's actual needs.  The full file
    # (used by the viewer) carries everything the viewer renders.
    # The nav variant strips noise:
    #
    #   * cells:   walkable only, encoded as [cx, cy] (no walkable
    #              flag, no conf, no vote count)
    #   * actors:  filtered to navigation DESTINATIONS only --
    #              chests, bosses, altars, portals, vendors, etc.
    #              Loot drops (item / item_legendary / item_unique)
    #              and threats (elite / champion) are dropped --
    #              they're noise for "take me to X" queries.  Each
    #              actor entry slimmed to the fields a path
    #              caller actually reads (skin, kind, sno_id,
    #              x/y/z/floor).
    #   * other top-level fields preserved (worlds, world_ids,
    #     floors_meta, activity_kinds, etc.).
    #
    # Result: ~40-70% smaller than the full JSON.  WarPath fetches
    # it instead of the full .json on its lazy-cells path; the
    # viewer keeps using the full .json so blocked cells +
    # threat/item markers still render correctly.
    NAV_ACTOR_KINDS = {
        # Loot containers
        'chest', 'chest_helltide_random', 'chest_helltide_silent',
        'chest_helltide_targeted',
        # Boss spawn points
        'boss',
        # Altars + buff sources
        'shrine', 'pyre', 'well',
        # Hordes pylons
        'pylon', 'aether_structure',
        # Travel / transitions
        'portal', 'portal_town', 'portal_helltide',
        'dungeon_entrance', 'pit_exit', 'pit_floor_portal',
        'undercity_exit', 'traversal', 'waypoint', 'horde_gate',
        # Quest objectives
        'objective', 'enticement', 'glyph_gizmo',
        # NPCs you'd actually pathfind to
        'npc_vendor', 'warplans_vendor', 'tyrael',
        'bounty_npc', 'mercenary',
        # Activity-specific obelisks
        'pit_obelisk', 'undercity_obelisk',
        # Resource nodes
        'ore', 'herb',
        # Town infrastructure
        'stash', 'gizmo',
    }
    # Pit-specific filter: pit_world records have RANDOMLY-positioned
    # bosses + floor portals that change every run, so storing them
    # in the static catalog is misleading -- the position the merger
    # records is just where one player happened to find them last.
    # Kept for pit_world: dungeon_entrance (entry portal + next-level
    # markers; same kind appears twice on intermediate floors --
    # one in, one out, both useful), pit_exit (clickable level switch),
    # traversal (jumps), pit_obelisk (zone-key crafter).
    PIT_NAV_DROP = {'boss', 'pit_floor_portal'}
    NAV_ACTOR_FIELDS = ('skin', 'kind', 'sno_id', 'type_id',
                        'x', 'y', 'z', 'floor')

    is_pit_world = (agg.key_type == 'pit_world')

    nav_payload = dict(payload)
    nav_grid    = dict(payload['grid'])
    nav_floors  = {}
    for fid, cells in cells_out_by_floor.items():
        # cells row schema (full):   [cx, cy, walkable, conf, total]
        # nav row schema (this var): [cx, cy, wall_dist]
        # `wall_dist` is the BFS distance from this walkable cell to
        # the nearest non-walkable / unmapped cell.  Pre-computed
        # server-side (see _compute_wall_dist) so WarPath never has
        # to run an in-Lua BFS even on big zones.
        #
        # Tried parallel-arrays { cxs:[], cys:[], wds:[] } -- came
        # out 3% LARGER because per-tuple bracket compression beats
        # per-element commas + array scaffolding.  Stuck with the
        # tuple form.
        walkable_pairs = [(c[0], c[1]) for c in cells if c[2]]
        wd = _compute_wall_dist(walkable_pairs)
        nav_floors[fid] = [[cx, cy, wd[(cx, cy)]] for (cx, cy) in walkable_pairs]
    nav_grid['floors'] = nav_floors
    nav_payload['grid'] = nav_grid
    # Filter + slim the actors list down to nav destinations.  Pit
    # worlds get an extra subtraction: random-spawn entities (boss,
    # pit_floor_portal) get dropped because their positions are
    # session-specific noise -- the player finds them wherever, the
    # merger averages those positions across runs into something
    # meaningless for navigation.
    nav_actors = []
    for a in actors_out:
        kind = a.get('kind')
        if kind not in NAV_ACTOR_KINDS:
            continue
        if is_pit_world and kind in PIT_NAV_DROP:
            continue
        slim = {k: a[k] for k in NAV_ACTOR_FIELDS if k in a}
        nav_actors.append(slim)
    nav_payload['actors'] = nav_actors
    nav_payload['cells_format'] = 'nav_walkable_only'

    nav_path = out_dir / f'{_safe_filename(agg.key)}.nav.json'
    nav_tmp  = nav_path.with_suffix('.json.tmp')
    with nav_tmp.open('w', encoding='utf-8') as f:
        json.dump(nav_payload, f, indent=None, separators=(',', ':'))
    os.replace(nav_tmp, nav_path)

    # Also write a pre-compressed companion so /zones/{key} can serve the
    # gzipped bytes directly (Content-Encoding: gzip) without paying the
    # gzip CPU cost on every request.  With multiple uploaders pulling the
    # zone catalog in parallel, runtime gzip via GZipMiddleware was pegging
    # the server's CPU.  Pre-compression turns each /zones/{key} response
    # into a static-file read + sendfile -- effectively free.
    import gzip
    def _gzip_to(src_path: Path, gz_dst: Path) -> None:
        gz_tmp_local = gz_dst.with_suffix('.gz.tmp')
        with gz_tmp_local.open('wb') as f:
            # mtime=0 makes the .gz reproducible (same bytes for same JSON);
            # avoids spurious If-Modified-Since misses when content
            # didn't actually change.  compresslevel=6 is the gzip default.
            with gzip.GzipFile(fileobj=f, mode='wb', mtime=0, compresslevel=6) as gz:
                with src_path.open('rb') as src:
                    gz.write(src.read())
        os.replace(gz_tmp_local, gz_dst)

    _gzip_to(out_path,   out_dir / f'{_safe_filename(agg.key)}.json.gz')
    _gzip_to(meta_path,  out_dir / f'{_safe_filename(agg.key)}.meta.json.gz')
    _gzip_to(nav_path,   out_dir / f'{_safe_filename(agg.key)}.nav.json.gz')
    _gzip_to(links_path, out_dir / f'{_safe_filename(agg.key)}.links.json.gz')
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
        # Use the same layout emit_curated does so per-zone JSONs and
        # the universal index agree on which actor lands on which
        # emit floor (especially important when a world_key got
        # cluster-split into multiple floors -- the actor's floor
        # number depends on which cluster its position falls in).
        floors_layout = _build_floor_layout(agg)
        actor_lookup  = _build_actor_floor_lookup(floors_layout, agg.grid_resolution)
        for entry in agg.actors.values():
            emit_floor = _emit_floor_for_actor(
                entry, floors_layout, actor_lookup, agg.grid_resolution)
            row_skin = {
                'key':    key,
                'kind':   entry.kind,
                'x':      entry.x,
                'y':      entry.y,
                'z':      entry.z,
                'floor':  emit_floor,
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


def emit_links_index(out_dir: Path, state: dict[str, KeyAgg]) -> Path:
    """Universal cross-zone link graph: a single file aggregating every
    `<key>.links.json`'s outbound_links into one structure keyed by
    from-zone.

    Why a global index instead of per-zone fetches: WarPath's planner
    needs to run Dijkstra over the WHOLE graph to find a route, not
    just one hop.  Fetching ~100 small files on plugin load is
    slow + spammy on the server logs; one ~50-200 KB file (the merged
    edge set is small) is one network round-trip and parses in a
    couple of milliseconds in Lua.

    Format:
        {
          'schema_version': 1,
          'updated_at':     <epoch>,
          'by_source': {
            '<from_zone>': [
              { 'to_zone': str,
                'to_world': str|null,
                'to_world_id': int|null,
                'actor_skin': str,
                'actor_kind': str,
                'actor_sno': int|null,
                'actor_x': float, 'actor_y': float, 'actor_z': float,
                'actor_floor': int|null,
                'count': int,
                'first_seen': epoch,
                'last_seen':  epoch,
              },
              ...
            ],
            ...
          }
        }
    """
    by_source: dict[str, list] = {}
    for key, agg in state.items():
        if not agg.outbound_links:
            continue
        # Sort each zone's edges descending by recency so the most
        # frequently-used links float to the top -- the planner uses
        # this as a tie-breaker when multiple edges go to the same
        # destination.
        edges = sorted(
            agg.outbound_links.values(),
            key=lambda e: (e.get('last_seen') or 0, e.get('count') or 0),
            reverse=True,
        )
        by_source[key] = edges

    payload = {
        'schema_version': SCHEMA_VERSION_SUPPORTED,
        'updated_at':     int(time.time()),
        'by_source':      by_source,
    }
    path = out_dir / '_links_index.json'
    tmp  = path.with_suffix('.json.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, indent=None, separators=(',', ':'))
    os.replace(tmp, path)
    # Pre-compress so the API can serve gzip directly.  Same shape as
    # _actor_index.json which the existing _gzip_to handles.
    _gzip_to(path, out_dir / '_links_index.json.gz')
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
    lnk = emit_links_index(data_dir, state)
    print(f'  -> {lnk}')

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
