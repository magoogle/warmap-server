// ---------------------------------------------------------------------------
// WarMap live viewer.
//
// Polls /status; when last_merge.finished_at advances, refresh sidebar +
// active zone.  Renders:
//   - merged walkable / blocked grid (color = walkability, alpha = confidence)
//   - actors as colored diamonds w/ kind glyph (click to inspect)
//   - per-uploader live tracks (the last N samples of in-progress dumps)
//   - simulated path between two clicked points (JS A* over merged grid)
//
// Tabs: Zones / Uploaders / Path
// Admin modal: paste master key -> mint per-friend keys, disable, quarantine.
// ---------------------------------------------------------------------------

const STATUS_POLL_MS = 5000;
const TRACK_TAIL_N   = 200;        // last N samples drawn per live track

// ---- DOM lookups ---------------------------------------------------------
const D = {
    state:        document.getElementById('status-text'),
    autoRefresh:  document.getElementById('auto-refresh'),
    zoneList:     document.getElementById('zone-list'),
    zoneCount:    document.getElementById('zone-count'),
    uploaderList: document.getElementById('uploader-list'),
    uploaderCount:document.getElementById('uploader-count'),
    empty:        document.getElementById('empty-state'),
    zoneView:     document.getElementById('zone-view'),
    zoneTitle:    document.getElementById('zone-title'),
    zoneMeta:     document.getElementById('zone-meta'),
    zoneDetails:        document.getElementById('zone-details'),
    zoneDetailsToggle:  document.getElementById('zone-details-toggle'),
    canvas:       document.getElementById('zone-canvas'),
    tooltip:      document.getElementById('hover-tooltip'),
    floorCtl:     document.getElementById('floor-controls'),
    floorSelect:  document.getElementById('floor-select'),
    orientRotate: document.getElementById('orient-rotate'),
    orientFlipX:  document.getElementById('orient-flipx'),
    orientFlipY:  document.getElementById('orient-flipy'),
    orientState:  document.getElementById('orient-state'),
    actorPanel:   document.getElementById('actor-panel'),
    actorBody:    document.getElementById('actor-panel-body'),
    actorClose:   document.getElementById('actor-panel-close'),
    pathPickA:    document.getElementById('path-pick-a'),
    pathPickB:    document.getElementById('path-pick-b'),
    pathClear:    document.getElementById('path-clear'),
    pathStatus:   document.getElementById('path-status'),
    pathResult:   document.getElementById('path-result'),
    adminBtn:     document.getElementById('admin-btn'),
    adminModal:   document.getElementById('admin-modal'),
    adminClose:   document.getElementById('admin-close'),
    adminAuth:    document.getElementById('admin-auth'),
    adminPanel:   document.getElementById('admin-panel'),
    adminKeyInput:document.getElementById('admin-key-input'),
    adminKeySave: document.getElementById('admin-key-save'),
    mintName:     document.getElementById('mint-name'),
    mintTier:     document.getElementById('mint-tier'),
    mintNote:     document.getElementById('mint-note'),
    mintGo:       document.getElementById('mint-go'),
    mintResult:   document.getElementById('mint-result'),
    keyTableBody: document.getElementById('key-table-body'),
    resetZoneSelect: document.getElementById('reset-zone-select'),
    resetZoneGo:     document.getElementById('reset-zone-go'),
    resetResult:     document.getElementById('reset-result'),
    quarantineList:  document.getElementById('quarantine-list'),
    quarantineCount: document.getElementById('quarantine-count'),
};
const ctx = D.canvas.getContext('2d');

// ---- State ---------------------------------------------------------------
const S = {
    lastMergeT:    null,
    currentKey:    null,
    currentData:   null,
    currentFloor:  null,
    view:          { panX: 0, panY: 0, scale: 1.0 },
    drag:          null,
    selectedActor: null,
    hoveredActor:  null,
    zoneList:      [],
    // Map zone-key -> { in_progress, uploaders, last_active }.  Refreshed
    // every status-poll cycle from /live-zones.  Empty object means
    // "no zones are currently live" (or we haven't fetched yet).
    liveZones:     {},
    uploaders:     [],
    activeUploader:null,
    uploaderTracks:{},          // dump_name -> { zone, samples, complete, activity }
    pathMode:      null,        // null | 'pickA' | 'pickB'
    pathA:         null,        // {wx, wy} world coords
    pathB:         null,
    pathPath:      null,        // [{cx, cy}]
    cellSet:       null,        // Set of "cx,cy" walkable cells (for the active floor)
    cellRes:       0.5,
    // In-memory cache of merged zone JSONs.  Keyed by zone key.  Wiped
    // whenever refreshStatus() detects a new merge timestamp (since the
    // merger having run means every zone may have changed).  Combined with
    // the server's conditional-GET (304) handling, this makes re-visiting
    // a zone within the same session effectively instant.
    zoneCache:     {},
    loadingKey:    null,        // zone we're currently fetching, for stale-response detection
    adminKey:      localStorage.getItem('warmap_admin_key') || '',
    // Tier of the currently-authenticated key.  null until /whoami
    // resolves; 'admin' | 'uploader' | 'reader' afterward.  Used by
    // applyTierGating() to hide admin-only UI from non-admin users.
    tier:          null,
    userName:      '',
    // Server's dynamic skin-ignore patterns.  Mirror of /ignore-list,
    // refreshed on signin + after every "Ignore this skin" click.
    // Render loop skips actors whose skin matches any of these
    // (substring), so the bot's hide takes effect INSTANTLY in the
    // viewer instead of waiting for the next merge cycle to drop the
    // actor from the JSON.
    serverIgnorePatterns: [],
    // Per-skin viewer-side overrides: skins the user has toggled to
    // 'show in viewer anyway' even though they're in the server
    // ignore list.  Useful for "I want to see what I'm ignoring"
    // filter behavior.  Persists in localStorage so the operator's
    // visibility prefs survive reloads.
    viewerShowOverrides: (function () {
        try {
            const raw = localStorage.getItem('warmap_show_overrides') || '[]';
            return new Set(JSON.parse(raw));
        } catch (e) { return new Set(); }
    })(),
    // Map of (zone|skin|rx|ry|floor) -> { label, kind_override, note }.
    // Populated from GET /labels on signin + after every admin save.
    // Render loop applies overrides per actor; actor info panel shows
    // editable inputs to admins.
    actorLabels: {},
    // Orientation: world-axis -> canvas-axis transform.  D4's coord system
    // doesn't map cleanly to "north up", so we let the user dial it in.
    // rot:   90deg increments clockwise (0|1|2|3)
    // flipX: mirror cell X axis
    // flipY: mirror cell Y axis
    orient: JSON.parse(localStorage.getItem('warmap_orient') || '{}') || {},
    // Layer toggles.  Each value is a boolean visible-flag.  Persists
    // across sessions in localStorage; merged with defaults on boot so
    // newly-added categories show up enabled by default for existing users.
    // Initialized below (after LAYER_CATEGORIES + defaultLayerState are
    // declared -- const/let don't hoist, so we can't call defaultLayerState
    // from inside this object literal).
    layers: {},
};
// Defaults that match the D4 top-down view: 180deg rotation puts the
// player's "south" downward on the canvas.  Users can re-tune via the
// orient buttons; their choice persists in localStorage.
S.orient.rot   = S.orient.rot   ?? 2;
S.orient.flipX = S.orient.flipX ?? false;
S.orient.flipY = S.orient.flipY ?? false;

function saveOrient() {
    localStorage.setItem('warmap_orient', JSON.stringify({
        rot: S.orient.rot, flipX: S.orient.flipX, flipY: S.orient.flipY,
    }));
}

// Map a (cellX, cellY) pair through the user's orientation choice.  The
// caller still subtracts bbox.minx/miny -- this is the rotation around
// the bbox center.  Returns transformed cell-space coords (still ints
// after rotation since we rotate by 90deg multiples).
function applyOrient(cx, cy, bbox) {
    let x = cx, y = cy;
    if (S.orient.flipX) x = bbox.minx + (bbox.maxx - x);
    if (S.orient.flipY) y = bbox.miny + (bbox.maxy - y);
    const r = ((S.orient.rot % 4) + 4) % 4;
    if (r === 0) return { x, y };
    // Rotate around the bbox center
    const cxC = (bbox.minx + bbox.maxx) / 2;
    const cyC = (bbox.miny + bbox.maxy) / 2;
    const dx = x - cxC, dy = y - cyC;
    let rx, ry;
    if (r === 1)      { rx =  dy; ry = -dx; }   // 90 cw
    else if (r === 2) { rx = -dx; ry = -dy; }   // 180
    else              { rx = -dy; ry =  dx; }   // 270 cw
    return { x: cxC + rx, y: cyC + ry };
}

// ---- Actor styling -------------------------------------------------------
const ACTOR_STYLE = {
    chest_helltide_random:   { c: '#ffcc00', sym: 'C',  label: 'Chest' },
    chest_helltide_silent:   { c: '#aaa0ff', sym: 'C',  label: 'Silent' },
    chest_helltide_targeted: { c: '#ff8800', sym: 'C',  label: 'Tortured' },
    chest:                   { c: '#ffaa44', sym: 'C',  label: 'Chest' },
    portal:                  { c: '#cc88ff', sym: 'P',  label: 'Portal' },
    portal_town:             { c: '#79c0ff', sym: 'T',  label: 'TownPortal' },
    portal_helltide:         { c: '#ff5544', sym: 'H',  label: 'HellPortal' },
    dungeon_entrance:        { c: '#ff66cc', sym: 'D',  label: 'Dungeon' },
    pit_exit:                { c: '#ffd700', sym: 'X',  label: 'Pit Exit' },
    pit_floor_portal:        { c: '#ff8c00', sym: 'F',  label: 'Pit Floor' },
    undercity_exit:          { c: '#ffd700', sym: 'U',  label: 'UC Exit' },
    traversal:               { c: '#88ddff', sym: 't',  label: 'Trav' },
    waypoint:                { c: '#88ff88', sym: 'W',  label: 'Waypoint' },
    stash:                   { c: '#ddddff', sym: 'S',  label: 'Stash' },
    shrine:                  { c: '#88ffaa', sym: 's',  label: 'Shrine' },
    pyre:                    { c: '#ff5544', sym: '^',  label: 'Pyre' },
    objective:               { c: '#ffff00', sym: '!',  label: 'Obj' },
    enticement:              { c: '#ffaa00', sym: '*',  label: 'Beacon' },
    glyph_gizmo:             { c: '#ff00ff', sym: 'G',  label: 'Glyph' },
    pit_obelisk:             { c: '#ff66cc', sym: 'O',  label: 'Pit-key' },
    undercity_obelisk:       { c: '#cc66ff', sym: 'O',  label: 'Obelisk' },
    warplans_vendor:         { c: '#ff9900', sym: 'V',  label: 'WarPlans' },
    tyrael:                  { c: '#ffffff', sym: 'T',  label: 'Tyrael' },
    horde_gate:              { c: '#cc4444', sym: 'H',  label: 'HrdGate' },
    bounty_npc:              { c: '#7d3cff', sym: 'B',  label: 'Raven' },
    mercenary:               { c: '#cc99ff', sym: 'M',  label: 'Merc' },
    gizmo:                   { c: '#a0c0ff', sym: 'G',  label: 'Gizmo' },
    ore:                     { c: '#888888', sym: 'o',  label: 'Ore' },
    herb:                    { c: '#66cc66', sym: 'h',  label: 'Herb' },
    npc_vendor:              { c: '#dddd66', sym: 'V',  label: 'Vendor' },
    npc:                     { c: '#aaaaaa', sym: 'n',  label: 'NPC' },
    // Hostile mobs (non-interactable; useful for "boss room" identification)
    boss:                    { c: '#ff0033', sym: 'B',  label: 'Boss' },
    elite:                   { c: '#ff7733', sym: 'e',  label: 'Elite' },
    champion:                { c: '#ffaa55', sym: 'c',  label: 'Champion' },
    // Hordes interactables (pylons = the wave-modifier "boons" players pick)
    pylon:                   { c: '#33ff99', sym: 'p',  label: 'Pylon' },
    aether_structure:        { c: '#99ccff', sym: 'a',  label: 'Aether' },
    // Wells (XP wells, season pacts, healing wells, etc.)
    well:                    { c: '#ffff66', sym: 'w',  label: 'Well' },
    // Generic catch-all for interactables that didn't match a specific kind.
    // Greyed out so it doesn't dominate the map; user can filter to it
    // when investigating "what's that thing the bot ignored".
    interactable:            { c: '#666666', sym: '?',  label: 'Other' },
    // Event-doors / boss-fight firewalls.  These are transient barriers
    // that gate boss arenas during fights.  Distinct color (electric
    // purple) so they pop visually -- when you see one, the surrounding
    // unsampled-cells region is "boss arena" and not "actually unwalkable."
    event_door:              { c: '#c478ff', sym: 'F',  label: 'Event Door' },
    // Loot items (ground drops).  Color hierarchy mirrors D4's in-game
    // tooltip colors so users get the same mental model: gold = unique,
    // orange = legendary, white = everything else.  The Items layer
    // is default-OFF (see defaultLayerState below) because ground
    // drops accumulate into starbursts across sessions and clutter
    // pathing maps -- toggle on when analyzing loot density.
    item_unique:             { c: '#ffd700', sym: 'U',  label: 'Unique' },
    item_legendary:          { c: '#ff9020', sym: 'L',  label: 'Legendary' },
    item:                    { c: '#e6edf3', sym: 'i',  label: 'Item' },
};
// ---------------------------------------------------------------------------
// Layer system -- group actor `kind`s into operator-friendly categories so
// the sidebar's Layers tab can offer a small set of toggles instead of one
// per kind.  Each kind appears in exactly one category; the renderer skips
// any actor whose category is hidden.  Walkable grid + uploader tracks +
// path overlay get their own pseudo-categories.
// ---------------------------------------------------------------------------
const KIND_CATEGORY = {
    // Loot / pickups
    chest_helltide_random:   'loot',
    chest_helltide_silent:   'loot',
    chest_helltide_targeted: 'loot',
    chest:                   'loot',
    ore:                     'loot',
    herb:                    'loot',
    stash:                   'loot',
    // Threats (non-interactable hostile actors)
    boss:                    'threats',
    elite:                   'threats',
    champion:                'threats',
    // Travel (portals, exits, dungeons, waypoints)
    portal:                  'travel',
    portal_town:             'travel',
    portal_helltide:         'travel',
    dungeon_entrance:        'travel',
    pit_exit:                'travel',
    pit_floor_portal:        'travel',
    undercity_exit:          'travel',
    traversal:               'travel',
    waypoint:                'travel',
    horde_gate:              'travel',
    // Activity objectives + interactives
    objective:               'objectives',
    enticement:              'objectives',
    pyre:                    'objectives',
    shrine:                  'objectives',
    glyph_gizmo:             'objectives',
    pylon:                   'objectives',
    aether_structure:        'objectives',
    pit_obelisk:             'objectives',
    undercity_obelisk:       'objectives',
    well:                    'objectives',
    event_door:              'objectives',   // boss-arena gates / firewalls
    // NPCs / vendors
    npc:                     'npcs',
    npc_vendor:              'npcs',
    warplans_vendor:         'npcs',
    tyrael:                  'npcs',
    bounty_npc:              'npcs',
    mercenary:               'npcs',
    // Loot items (ground drops -- distinct from chests in the 'loot'
    // bucket which are clickable containers).  Own category so users
    // can hide all three sub-kinds together with one toggle without
    // losing chests/ore/herbs.
    item:                    'items',
    item_legendary:          'items',
    item_unique:             'items',
    // Catch-all
    gizmo:                   'other',
    interactable:            'other',
};

// Display metadata for the Layers panel.  Color is the canonical actor color
// for the most representative kind in the category; sub is a count summary
// updated live by the renderer.
const LAYER_CATEGORIES = [
    { id: 'loot',       label: 'Loot',         color: '#ffcc00', desc: 'chests, ore, herbs, stash' },
    { id: 'threats',    label: 'Threats',      color: '#ff0033', desc: 'boss / elite / champion' },
    { id: 'travel',     label: 'Travel',       color: '#cc88ff', desc: 'portals, exits, waypoints' },
    { id: 'objectives', label: 'Objectives',   color: '#88ffaa', desc: 'shrines, pyres, glyphs, pylons' },
    { id: 'npcs',       label: 'NPCs',         color: '#dddd66', desc: 'vendors, mercenary, bounty' },
    // Items: ground drops (uniques, legendaries, gear bases).  Default
    // OFF -- per-session drop positions are random so re-playing the
    // same zone accumulates a starburst that drowns out the static
    // map.  Toggle on for loot-density / drop-location analysis.
    { id: 'items',      label: 'Items',        color: '#ffd700', desc: 'ground drops: uniques, legendaries, gear (off by default)' },
    { id: 'other',      label: 'Other',        color: '#888888', desc: 'gizmos, generic interactables' },
];

// Layer ids that start hidden on first load.  Subsequent toggles persist
// to localStorage via the existing layer-state machinery, so a user who
// turns Items on once doesn't have to redo it every page refresh.
const DEFAULT_HIDDEN_LAYERS = new Set(['actors_items']);

const PSEUDO_LAYERS = [
    { id: 'walkable_grid',   label: 'Walkable grid',   color: '#3fb950', desc: 'walkable + blocked cells' },
    { id: 'uploader_tracks', label: 'Uploader tracks', color: '#3fff8b', desc: 'live session paths' },
    { id: 'path_overlay',    label: 'Path simulator',  color: '#58a6ff', desc: 'A->B path + endpoints' },
];

// Default: everything visible EXCEPT layers in DEFAULT_HIDDEN_LAYERS
// (currently 'actors_items' -- ground drops are noisy and most users
// only want them on for specific analyses).
function defaultLayerState() {
    const s = {};
    for (const c of LAYER_CATEGORIES) {
        const id = 'actors_' + c.id;
        s[id] = !DEFAULT_HIDDEN_LAYERS.has(id);
    }
    for (const p of PSEUDO_LAYERS)    s[p.id]            = true;
    return s;
}

// Now that LAYER_CATEGORIES + PSEUDO_LAYERS + defaultLayerState exist,
// populate S.layers.  Has to live here (not in the S object literal
// above) because const/let don't hoist -- referencing LAYER_CATEGORIES
// before its declaration trips ReferenceError.
(function _initLayers() {
    let saved = {};
    try { saved = JSON.parse(localStorage.getItem('warmap_layers') || '{}') || {}; } catch (e) {}
    S.layers = Object.assign(defaultLayerState(), saved);
})();

const KIND_OVERRIDES = {
    pit_obelisk:'Pit Obelisk', undercity_obelisk:'Undercity Obelisk',
    warplans_vendor:'War Plans Vendor', tyrael:'Tyrael', horde_gate:'Horde Gate',
    glyph_gizmo:'Glyph Gizmo', bounty_npc:'Raven', stash:'Stash',
    waypoint:'Waypoint', pyre:'Pyre', traversal:'Traversal', portal:'Portal',
    portal_town:'Town Portal', portal_helltide:'Helltide Portal',
    dungeon_entrance:'Dungeon Entrance', pit_exit:'Pit Exit',
    pit_floor_portal:'Pit Floor Portal',
    undercity_exit:'Undercity Floor Switch',
    objective:'Objective', enticement:'Beacon',
    boss:'Boss', elite:'Elite', champion:'Champion',
    pylon:'Hordes Pylon', aether_structure:'Aether Structure',
    well:'Well', interactable:'Other Interactable',
};

// Get the effective kind for an actor, honoring server-side label overrides.
function effectiveActorKind(a) {
    const lbl = lookupActorLabel(a);
    return (lbl && lbl.kind_override) || a.kind;
}

function actorDisplayName(a) {
    // Custom label set by an admin in the actor info panel.  Takes
    // precedence over every auto-derived name below.
    const lbl = lookupActorLabel(a);
    if (lbl && lbl.label) return lbl.label;
    if (KIND_OVERRIDES[a.kind]) return KIND_OVERRIDES[a.kind];
    const skin = a.skin || '';
    if (skin.includes('Helltide_RewardChest_Random')) return 'Helltide Chest';
    if (skin.includes('Helltide_SilentChest'))        return 'Silent Chest';
    const ttg = skin.match(/usz_rewardGizmo_(\w+)/);
    if (ttg) return 'Tortured Gift (' + ttg[1] + ')';
    if (/^TWN_.*_(VLG|CHD)_[MFC]\d+$/.test(skin) || /_[MFC]\d+$/.test(skin)) {
        if (skin.includes('_CHD_')) return 'Child';
        return 'Villager';
    }
    let s = skin
        .replace(/^TWN_[A-Za-z0-9]+_[A-Za-z0-9]+_/, '')
        .replace(/^Merc_Hideout_NPC_/,'Mercenary: ')
        .replace(/^NPC_QST_X2_/, '')
        .replace(/^S07_Bounty_Meta_/, 'Bounty: ')
        .replace(/^(Crafter|Vendor|Service|Stable)_/, '');
    if (!s.includes(' ')) s = s.replace(/([a-z])([A-Z])/g, '$1 $2');
    if (s && s.length < 40) return s;
    return (ACTOR_STYLE[a.kind] && ACTOR_STYLE[a.kind].label) || a.kind || '?';
}

// ---- Fetch helpers -------------------------------------------------------
// Every read endpoint on the server now requires X-WarMap-Key.  The viewer
// loads the operator's admin key from localStorage at boot (S.adminKey) so
// we attach it to every fetch.  When no key is set, requests still go out
// (server returns 401, surfaced as "disconnected: HTTP 401" in the status
// line, which is the user's cue to enter their key in the input above the
// sidebar).
function authHeaders() {
    return S.adminKey ? { 'X-WarMap-Key': S.adminKey } : {};
}
async function getJSON(p, opts) {
    const init = Object.assign({ cache: 'no-store' }, opts || {});
    init.headers = Object.assign({}, init.headers, authHeaders());
    const r = await fetch(p, init);
    if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`);
    return r.json();
}
async function getText(p) {
    const r = await fetch(p, { cache: 'no-store', headers: authHeaders() });
    if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`);
    return r.text();
}
function adminFetch(path, init) {
    init = init || {};
    init.headers = Object.assign({}, init.headers, { 'X-WarMap-Key': S.adminKey });
    init.cache = 'no-store';
    return fetch(path, init);
}

// ---- Server ignore list mirror -------------------------------------------
// Pulled from GET /ignore-list and used by both:
//   * the render loop, to skip actors whose skin matches a pattern
//     (so 'Ignore this skin' clicks take effect instantly in the
//     viewer without waiting for the next merge cycle to drop them
//     from the merged JSON)
//   * the Layers tab's 'Hidden skins' filter section, which lists
//     each pattern with a 'show in viewer anyway' toggle and an
//     admin-only 'remove from server' button.
async function refreshServerIgnoreList() {
    try {
        const r = await getJSON('/ignore-list');
        S.serverIgnorePatterns = (r && r.patterns) || [];
        renderHiddenSkinsSection();
    } catch (e) {
        // Non-fatal: render still works with the previous list (or [])
    }
}

// Returns true if the actor's skin should be hidden from the viewer
// because the server is ignoring it AND the user hasn't overridden it
// to 'show anyway' for this session.
function isHiddenSkin(skin) {
    if (!skin) return false;
    if (S.viewerShowOverrides.has(skin)) return false;
    const patterns = S.serverIgnorePatterns;
    for (let i = 0; i < patterns.length; i++) {
        if (skin.indexOf(patterns[i]) !== -1) return true;
    }
    return false;
}

function persistShowOverrides() {
    try {
        localStorage.setItem('warmap_show_overrides',
            JSON.stringify([...S.viewerShowOverrides]));
    } catch (e) {}
}

// ---- Actor label overrides -----------------------------------------------
// Lookup key matches the server's actor_labels primary key.  Note: rx/ry
// in the merger come from rounded x/y, so we round here too -- the
// viewer's actor list has the float positions.
function actorLabelKey(zone, skin, x, y, floor) {
    const rx = Math.round(x || 0);
    const ry = Math.round(y || 0);
    return `${zone || ''}|${skin || ''}|${rx}|${ry}|${floor || 1}`;
}

function lookupActorLabel(a) {
    if (!S.actorLabels || !a) return null;
    return S.actorLabels[actorLabelKey(S.currentKey, a.skin, a.x, a.y, a.floor)] || null;
}

async function refreshActorLabels() {
    try {
        const r = await getJSON('/labels');
        const map = {};
        for (const l of (r && r.labels) || []) {
            const key = `${l.zone}|${l.skin}|${l.rx}|${l.ry}|${l.floor}`;
            map[key] = l;
        }
        S.actorLabels = map;
    } catch (e) {
        // Non-fatal -- render still works without labels.
    }
}

// ---- Sign-in overlay -----------------------------------------------------
// Centered card, shown on boot when no API key is in localStorage and
// again whenever a fetch returns 401.  Single input + Continue button.
function showSigninOverlay(errorMsg) {
    const o = document.getElementById('signin-overlay');
    const inp = document.getElementById('signin-input');
    const err = document.getElementById('signin-error');
    if (!o) return;
    if (err) err.textContent = errorMsg || '';
    o.hidden = false;
    setTimeout(() => inp && inp.focus(), 50);
}
function hideSigninOverlay() {
    const o = document.getElementById('signin-overlay');
    if (o) o.hidden = true;
}
async function attemptSignin(key) {
    const trimmed = (key || '').trim();
    if (!trimmed) { showSigninOverlay('paste a key first'); return false; }
    let tierInfo = null;
    try {
        // Hit /whoami (gated) which both validates the key AND tells us
        // the tier, so we can gate UI in one round-trip.
        const r = await fetch('/whoami', {
            cache: 'no-store',
            headers: { 'X-WarMap-Key': trimmed },
        });
        if (r.status === 401) { showSigninOverlay('rejected'); return false; }
        if (!r.ok)            { showSigninOverlay(`HTTP ${r.status}`); return false; }
        tierInfo = await r.json();
    } catch (e) {
        showSigninOverlay('network error');
        return false;
    }
    S.adminKey = trimmed;
    S.tier     = tierInfo?.tier || 'reader';
    S.userName = tierInfo?.name || '';
    localStorage.setItem('warmap_admin_key', trimmed);
    applyTierGating();
    hideSigninOverlay();
    refreshStatus();
    refreshZoneList();
    if (S.tier === 'admin') refreshUploaders();
    return true;
}

// ---- Tier gating ---------------------------------------------------------
// Sets a CSS class on <body> based on the user's tier; the stylesheet
// does the actual show/hide via [data-admin-only] selectors.  Reader-
// and uploader-tier keys never see the Uploaders tab or Admin button;
// they get zone download access instead (handled separately below).
function applyTierGating() {
    const body = document.body;
    if (!body) return;
    body.classList.remove('tier-admin', 'tier-uploader', 'tier-reader');
    if (S.tier === 'admin')         body.classList.add('tier-admin');
    else if (S.tier === 'uploader') body.classList.add('tier-uploader');
    else if (S.tier === 'reader')   body.classList.add('tier-reader');
    // The download button shows for everyone with a valid key, but only
    // when a zone is selected; loadZone() flips its hidden state.
}
document.getElementById('signin-btn')?.addEventListener('click', () => {
    const inp = document.getElementById('signin-input');
    if (inp) attemptSignin(inp.value);
});
document.getElementById('signin-input')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        e.preventDefault();
        attemptSignin(e.target.value);
    }
});

// ---- Header live stats ---------------------------------------------------
// Shorter, more-glanceable replacement for the single status text node.
// Highlights individual numbers when they change so an active server
// pulses subtly in the corner.
function fmtAgo(t) {
    if (!t) return 'never';
    const sec = Math.max(0, Math.round(Date.now() / 1000 - t));
    if (sec < 60)    return sec + 's';
    if (sec < 3600)  return Math.round(sec / 60) + 'm';
    if (sec < 86400) return Math.round(sec / 3600) + 'h';
    return Math.round(sec / 86400) + 'd';
}
function setHeaderStat(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    if (el.textContent !== String(value)) {
        el.textContent = value;
        // Restart the pulse animation by removing + re-adding the class.
        el.classList.remove('fresh');
        // eslint-disable-next-line no-unused-expressions
        el.offsetWidth;
        el.classList.add('fresh');
    }
}

// ---- Status + zone list --------------------------------------------------
async function refreshStatus() {
    try {
        const s = await getJSON('/status');
        const m = s.last_merge || {};
        const fin = m.finished_at;
        // New 3-stat header layout.  Old single-text status node is hidden
        // in HTML but kept around in the DOM for any code that still pokes it.
        setHeaderStat('stat-dumps-v', s.dumps_count.toLocaleString());
        setHeaderStat('stat-zones-v', s.zones_count.toLocaleString());
        setHeaderStat('stat-merge-v', fin ? fmtAgo(fin) + ' ago' : 'never');
        if (D.state) D.state.textContent =
            `${s.dumps_count} dumps · ${s.zones_count} zones · last merge ${fin ? prettyAgo(fin) : 'never'}`;
        if (fin && fin !== S.lastMergeT) {
            S.lastMergeT = fin;
            // The merger ran -- every zone may have changed.  Wipe the
            // in-memory zone cache so the next click on each zone gets
            // the fresh JSON.  The browser cache layer handles the case
            // where individual zones haven't actually changed (server
            // returns 304, no body downloaded).
            S.zoneCache = {};
            await refreshZoneList();
            await refreshUploaders();
            if (S.currentKey) await loadZone(S.currentKey, false);
        }
        // Live-zone indicator update: cheap (one indexed query on the
        // sessions table), runs every poll regardless of merge state
        // so the LIVE badge appears within ~5s of someone starting a
        // recording.  Independent of the merge timestamp because new
        // uploads predate the next merge by definition.
        await refreshLiveZones();
    } catch (e) {
        if (D.state) D.state.textContent = `disconnected: ${e.message}`;
        // Auth failures specifically: show the sign-in overlay rather than
        // leaving the user staring at "disconnected" text.  Heuristic:
        // /status returning 401 (which getJSON throws as "HTTP 401").
        if (/HTTP 401/.test(String(e.message))) {
            showSigninOverlay('your saved key was rejected -- paste a current one');
        }
    }
}

async function refreshZoneList() {
    try {
        const z = await getJSON('/zones');
        S.zoneList = (z.zones || []).filter(k => !k.startsWith('_') && k !== 'coverage');
        D.zoneCount.textContent = `(${S.zoneList.length})`;
        renderZoneList();
    } catch {}
}

// Pull the live-zones map (zones with in-progress uploads right now).
// Re-renders the sidebar so the LIVE badge shows up immediately --
// the user shouldn't have to wait for the next merge cycle to see
// that someone just started recording in a zone.  Failures swallowed;
// a missing live-indicator is strictly better than a broken sidebar.
async function refreshLiveZones() {
    try {
        const r = await getJSON('/live-zones');
        const next = r.zones || {};
        // Only re-render when the *set* of live keys changes (or any
        // count flipped).  Avoids the whole zone list re-painting every
        // 5s on a server with steady live data.
        const oldKeys = Object.keys(S.liveZones).sort().join('|');
        const newKeys = Object.keys(next).sort().join('|');
        const oldCounts = Object.entries(S.liveZones).map(([k, v]) => `${k}:${v.in_progress}`).sort().join('|');
        const newCounts = Object.entries(next).map(([k, v]) => `${k}:${v.in_progress}`).sort().join('|');
        S.liveZones = next;
        if (oldKeys !== newKeys || oldCounts !== newCounts) renderZoneList();
    } catch {}
}

// Mirror of the recorder's activity classifier so we can bucket zones in
// the sidebar without round-tripping to the server for activity_kind.
const TOWN_ZONES = new Set([
    'Skov_Temis','Scos_Cerrigar','Kehj_Caldeum','Hawe_Backwater',
    'Hawe_Tarsarak','Hawe_Zarbinzet','Naha_KurastDocks','Frac_Menestad',
    'Step_Jirandai','Kehj_IronWolves_Kehjan','Frac_Tundra_S','Scos_Coast',
]);
function categoryFor(key) {
    if (TOWN_ZONES.has(key))                return { id: 'towns',     label: 'Towns'              };
    if (key.startsWith('PIT_'))             return { id: 'pits',      label: 'Pits'               };
    if (key.startsWith('DGN_'))             return { id: 'nmds',      label: 'Nightmare Dungeons' };
    if (key.startsWith('X1_Undercity_'))    return { id: 'undercity', label: 'Undercity'          };
    if (key.startsWith('S05_BSK_'))         return { id: 'hordes',    label: 'Hordes'             };
    if (key === 'coverage' || key.startsWith('_')) {
        return { id: 'system', label: 'System' };
    }
    return { id: 'overworld', label: 'Overworld' };
}
const CATEGORY_ORDER = ['towns', 'overworld', 'pits', 'nmds', 'undercity', 'hordes', 'system'];

function renderZoneList() {
    D.zoneList.innerHTML = '';

    // Bucket
    const buckets = {};
    for (const key of S.zoneList) {
        const cat = categoryFor(key);
        (buckets[cat.id] ??= { label: cat.label, zones: [] }).zones.push(key);
    }
    // Stable, alphabetical within bucket
    for (const id of Object.keys(buckets)) buckets[id].zones.sort();

    // Render in fixed order; categories with zero entries are skipped
    for (const id of CATEGORY_ORDER) {
        const b = buckets[id];
        if (!b || b.zones.length === 0) continue;

        const det = document.createElement('details');
        det.className = 'zone-cat';
        det.dataset.cat = id;
        // Persist open/closed state across refreshes via localStorage.
        const stored = localStorage.getItem('zone_cat_' + id);
        const containsCurrent = b.zones.includes(S.currentKey);
        det.open = (stored == null) ? (containsCurrent || id === 'towns') : (stored === '1');
        det.addEventListener('toggle', () => {
            localStorage.setItem('zone_cat_' + id, det.open ? '1' : '0');
        });

        const sum = document.createElement('summary');
        sum.innerHTML = `${b.label} <span class="muted">${b.zones.length}</span>`;
        det.appendChild(sum);

        const ul = document.createElement('ul');
        ul.className = 'zone-list zone-cat-list';
        for (const key of b.zones) {
            const li = document.createElement('li');
            li.dataset.key = key;
            if (key === S.currentKey) li.classList.add('active');
            const n = document.createElement('div');
            // Strip the category prefix from display so "Skov_Temis" stays
            // readable but "PIT_Cave_Coast" appears as "Cave_Coast" inside
            // the Pits category.  Towns + overworld keep full name.
            n.className = 'zone-name';
            n.textContent = id === 'pits' ? key.replace(/^PIT_/, '')
                          : id === 'nmds' ? key.replace(/^DGN_/, '')
                          : id === 'undercity' ? key.replace(/^X1_Undercity_/, '')
                          : id === 'hordes' ? key.replace(/^S05_BSK_/, '')
                          : key;
            n.title = key;     // full key always visible on hover
            li.appendChild(n);

            // LIVE indicator: pulsing dot + count.  Inserted only when
            // /live-zones reports an in-progress session for this key
            // (filtered to last_active >= now-300s server-side, so we
            // can trust the presence here).  Tooltip shows uploader
            // count + how recently the last sample landed.
            const live = S.liveZones && S.liveZones[key];
            if (live) {
                const badge = document.createElement('span');
                badge.className = 'zone-live-badge';
                const dot = document.createElement('span');
                dot.className = 'live-dot';
                badge.appendChild(dot);
                const lbl = document.createElement('span');
                lbl.className = 'live-label';
                // Count -- usually 1, but show explicit number when
                // multiple uploaders are simultaneously in this zone
                // (rare but possible during synchronous group play).
                lbl.textContent = live.in_progress > 1
                    ? `LIVE x${live.in_progress}`
                    : 'LIVE';
                badge.appendChild(lbl);
                badge.title =
                    `Active recording: ${live.in_progress} session${live.in_progress === 1 ? '' : 's'} ` +
                    `from ${live.uploaders} uploader${live.uploaders === 1 ? '' : 's'} ` +
                    `(last sample ${prettyAgo(live.last_active)})`;
                li.appendChild(badge);
                li.classList.add('has-live');
            }

            li.addEventListener('click', () => loadZone(key, true));
            ul.appendChild(li);
        }
        det.appendChild(ul);
        D.zoneList.appendChild(det);
    }
}

// ---- Uploaders -----------------------------------------------------------
async function refreshUploaders() {
    try {
        const u = await getJSON('/uploaders');
        S.uploaders = u.uploaders || [];
        D.uploaderCount.textContent = `(${S.uploaders.length})`;
        renderUploaderList();
        if (S.activeUploader) await fetchUploaderTracks(S.activeUploader);
    } catch {}
}

function renderUploaderList() {
    D.uploaderList.innerHTML = '';
    for (const u of S.uploaders) {
        const li = document.createElement('li');
        li.classList.add('uploader');
        li.dataset.cid = u.client_id;
        if (u.client_id === S.activeUploader) li.classList.add('active');
        const name = document.createElement('div');
        name.className = 'zone-name'; name.textContent = u.client_id;
        li.appendChild(name);
        const stats = document.createElement('div');
        stats.className = 'zone-stats';
        if (u.in_progress) {
            const live = document.createElement('span');
            live.className = 'badge live';
            live.textContent = `${u.in_progress} live`;
            stats.appendChild(live);
        }
        const total = document.createElement('span');
        total.className = 'badge';
        total.textContent = `${u.sessions} session${u.sessions === 1 ? '' : 's'}`;
        stats.appendChild(total);
        if (u.last_active) stats.appendChild(document.createTextNode(' · ' + prettyAgo(u.last_active)));
        li.appendChild(stats);
        if (u.zones && u.zones.length) {
            const z = document.createElement('div');
            z.className = 'zone-stats';
            z.textContent = u.zones.slice(0, 3).join(', ') + (u.zones.length > 3 ? '...' : '');
            li.appendChild(z);
        }
        li.addEventListener('click', () => selectUploader(u.client_id));
        D.uploaderList.appendChild(li);
    }
}

async function selectUploader(cid) {
    S.activeUploader = (S.activeUploader === cid) ? null : cid;
    document.querySelectorAll('#uploader-list li').forEach(li => {
        li.classList.toggle('active', li.dataset.cid === S.activeUploader);
    });
    if (S.activeUploader) await fetchUploaderTracks(S.activeUploader);
    else { S.uploaderTracks = {}; render(); }
}

async function fetchUploaderTracks(cid) {
    try {
        const d = await getJSON('/dumps');
        const mine = (d.dumps || []).filter(x => x.client_id === cid);
        const tracks = {};
        for (const meta of mine.slice(0, 8)) {
            try {
                const text = await getText('/dumps/' + encodeURIComponent(meta.name));
                const samples = [];
                for (const line of text.split('\n')) {
                    if (!line) continue;
                    let o; try { o = JSON.parse(line); } catch { continue; }
                    if (o.type === 'sample' && typeof o.x === 'number') {
                        samples.push({ x: o.x, y: o.y, z: o.z, floor: o.floor || 1 });
                    }
                }
                tracks[meta.name] = {
                    zone:     meta.zone,
                    samples:  samples.slice(-TRACK_TAIL_N),
                    complete: meta.complete,
                    activity: meta.activity,
                };
            } catch {}
        }
        S.uploaderTracks = tracks;
    } catch { S.uploaderTracks = {}; }
    render();
}

// ---- Zone load + render --------------------------------------------------
async function loadZone(key, resetView) {
    S.currentKey = key;
    S.loadingKey = key;
    document.querySelectorAll('.zone-list li').forEach(li => {
        li.classList.toggle('active', li.dataset.key === key);
    });
    D.empty.hidden = true;
    D.zoneView.hidden = false;
    // Reveal the download button now that a specific zone is loaded
    // (it's hidden in the empty-state to keep the chrome clean).
    const dl = document.getElementById('zone-download');
    if (dl) dl.hidden = false;

    // Cache hit: paint instantly, skip the network entirely.
    const cached = S.zoneCache[key];
    if (cached) {
        applyZoneData(cached, resetView);
        return;
    }

    // Cache miss: show loading state immediately so the click feels
    // responsive even on slow networks / fresh-merge re-fetches.
    D.zoneTitle.textContent = key;
    D.zoneMeta.innerHTML = '<span class="muted">loading...</span>';
    S.currentData = null;
    S.cellSet = null;
    // Clear the canvas so the user doesn't see stale data from the
    // previous zone while the new one is fetching.
    const w = D.canvas.width, h = D.canvas.height;
    ctx.fillStyle = '#06090d';
    ctx.fillRect(0, 0, w, h);
    ctx.fillStyle = '#8b949e'; ctx.font = '14px sans-serif';
    ctx.fillText(`loading ${key}...`, 20, 24);

    try {
        // Default browser cache (NOT 'no-store') so the server's ETag /
        // Last-Modified handling can return 304 with no body when nothing
        // changed.  Cuts re-visit cost from full payload to header-only.
        const d = await getJSON('/zones/' + encodeURIComponent(key), { cache: 'default' });
        // Race guard: user might have clicked another zone while this one
        // was in flight.  Discard if so.
        if (S.loadingKey !== key) return;
        S.zoneCache[key] = d;
        applyZoneData(d, resetView);
    } catch (e) {
        if (S.loadingKey === key) {
            D.zoneTitle.textContent = key;
            D.zoneMeta.textContent = `failed: ${e.message}`;
        }
    }
}

// Render whatever zone data we already have (from cache or fresh fetch).
// Pulled out of loadZone so cache hits skip every step of the network path.
function applyZoneData(d, resetView) {
    S.currentData = d;
    D.zoneTitle.textContent = d.key;
    const floors = Object.keys(d.grid?.floors || {});
    if (floors.length > 1) {
        D.floorCtl.hidden = false;
        D.floorSelect.innerHTML = '';
        for (const f of floors) {
            const opt = document.createElement('option');
            opt.value = f; opt.textContent = `floor ${f}`;
            D.floorSelect.appendChild(opt);
        }
        if (!floors.includes(S.currentFloor)) S.currentFloor = floors[0];
        D.floorSelect.value = S.currentFloor;
    } else {
        D.floorCtl.hidden = true;
        S.currentFloor = floors[0] || '1';
    }
    if (resetView) {
        S.view = { panX: 0, panY: 0, scale: 1.0 };
        S.selectedActor = null;
        S.pathA = S.pathB = S.pathPath = null;
        renderActorPanel();
        updatePathStatus();
    }
    rebuildCellSet();
    renderMeta();
    render();
}

function rebuildCellSet() {
    if (!S.currentData) { S.cellSet = null; S.wallDist = null; return; }
    const cells = S.currentData.grid?.floors?.[S.currentFloor] || [];
    const set = new Set();
    for (const c of cells) {
        if (c[2]) set.add(c[0] + ',' + c[1]);
    }
    S.cellSet = set;
    S.cellRes = S.currentData.grid?.resolution || 0.5;
    // Build the wall-distance map immediately so the next path simulation
    // doesn't pay the BFS cost.  Cheap (linear in #walkable cells), and
    // a zone load is rare relative to path picks.
    S.wallDist = computeWallDistance(set);
}

function renderMeta() {
    if (!S.currentData) return;
    const cells   = (S.currentData.grid?.floors?.[S.currentFloor] || []).length;
    const actors  = (S.currentData.actors || []).filter(a => a.floor == null || String(a.floor) === S.currentFloor).length;
    const sat     = S.currentData.saturated ? ' · saturated' : '';
    const sess    = S.currentData.sessions_merged || 0;
    const merged  = S.currentData.merged_at ? prettyAgo(S.currentData.merged_at) : 'never';
    D.zoneMeta.innerHTML =
        `${cells.toLocaleString()} cells · ${actors} actors · ${sess} session${sess === 1 ? '' : 's'}${sat} · ` +
        `<span class="muted">merged ${merged}</span>`;
    // Re-populate the (possibly hidden) details panel so it's always
    // fresh -- cheap, runs only when a zone is loaded or floor changes.
    renderZoneDetails();
}

// ---- Rich zone-details panel ----------------------------------------------
// Surfaced via the "Details v" toggle next to the zone header.  Pulls every
// non-bulky field from the merged JSON and renders a compact two-column
// grid plus inline breakdowns for per-floor and per-actor-kind counts.
// Updated whenever currentData changes or the user changes floors.
function renderZoneDetails() {
    if (!D.zoneDetails) return;
    const z = S.currentData;
    if (!z) { D.zoneDetails.innerHTML = ''; return; }

    const esc = s => String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
    const fmtNum = n => Number(n || 0).toLocaleString();
    const fmtTs  = ts => ts ? new Date(ts * 1000).toLocaleString() : '--';

    // ---- Per-floor breakdown (cells walkable + actors + worlds + ids) ----
    const floors = z.grid?.floors || {};
    const floorsMeta = z.grid?.floors_meta || {};
    const actorsAll = z.actors || [];
    const floorIds = Object.keys(floors).sort((a, b) => Number(a) - Number(b));
    // Display helpers for the worlds/ids columns: fold long lists to
    // "first +N more" so the table row doesn't blow out horizontally
    // when a zone has multiple worlds across re-runs.
    const fmtList = (arr, max = 2) => {
        if (!Array.isArray(arr) || arr.length === 0) return '<span class="muted">--</span>';
        const head = arr.slice(0, max).map(esc).join(', ');
        if (arr.length <= max) return head;
        return `${head} <span class="muted">+${arr.length - max}</span>`;
    };
    const floorRows = floorIds.map(fid => {
        const cells   = (floors[fid] || []).length;
        const actorsF = actorsAll.filter(a => String(a.floor) === fid).length;
        const meta    = floorsMeta[fid] || {};
        const cur = String(S.currentFloor) === fid ? ' (active)' : '';
        return `<tr><td>floor ${esc(fid)}${cur}</td>` +
               `<td>${fmtNum(cells)}</td>` +
               `<td>${fmtNum(actorsF)}</td>` +
               `<td>${fmtList(meta.worlds)}</td>` +
               `<td>${fmtList(meta.world_ids)}</td>` +
               `<td>${fmtNum(meta.sessions || 0)}</td></tr>`;
    }).join('');

    // ---- Per-kind actor breakdown (active floor only -- matches what the
    // viewer is currently rendering) ----
    const kindCounts = {};
    for (const a of actorsAll) {
        if (a.floor != null && String(a.floor) !== String(S.currentFloor)) continue;
        const k = a.kind || '?';
        kindCounts[k] = (kindCounts[k] || 0) + 1;
    }
    const kindRows = Object.entries(kindCounts)
        .sort((a, b) => b[1] - a[1])
        .map(([k, n]) => `<tr><td>${esc(k)}</td><td>${fmtNum(n)}</td></tr>`).join('');

    // ---- Headline grid (2-col x 2-col = 4 cells per row) ----
    const bbox = z.grid?.bbox;
    const bboxStr = (Array.isArray(bbox) && bbox.length === 4)
        ? `${bbox[0].toFixed(1)} -> ${bbox[2].toFixed(1)} x ` +
          `${bbox[1].toFixed(1)} -> ${bbox[3].toFixed(1)}`
        : '--';
    const extentStr = (Array.isArray(bbox) && bbox.length === 4)
        ? `${(bbox[2] - bbox[0]).toFixed(1)} x ${(bbox[3] - bbox[1]).toFixed(1)} m`
        : '--';
    const sat = z.saturated;
    const satReason = z.saturation_info?.reason || '';
    const satClass = sat ? 'ok' : (satReason ? 'warn' : '');
    const satText  = sat ? 'yes' : (satReason ? `no (${esc(satReason)})` : 'no');
    const acts = (z.activity_kinds && z.activity_kinds.length)
        ? z.activity_kinds.map(esc).join(', ') : '--';

    const totalActors = actorsAll.length;
    const totalCells  = floorIds.reduce((acc, fid) => acc + (floors[fid] || []).length, 0);

    // World identifiers: top-level lists from the merger's KeyAgg.
    // Fall back to "--" for older curated JSONs (pre-worlds-tracking) so
    // the dropdown stays informative even before a re-merge sweep
    // upgrades the on-disk files.
    const worlds    = Array.isArray(z.worlds)    ? z.worlds    : [];
    const worldIds  = Array.isArray(z.world_ids) ? z.world_ids : [];
    const worldsStr   = worlds.length    ? worlds.map(esc).join(', ')        : '<span class="muted">-- (re-merge to populate)</span>';
    const worldIdsStr = worldIds.length  ? worldIds.map(String).join(', ')   : '<span class="muted">-- (re-merge to populate)</span>';

    D.zoneDetails.innerHTML = `
      <div class="det-grid">
        <span class="k">key</span>           <span class="v">${esc(z.key || '--')}</span>
        <span class="k">type</span>          <span class="v">${esc(z.key_type || '--')}</span>
        <span class="k">activity</span>      <span class="v">${acts}</span>
        <span class="k">schema</span>        <span class="v">v${esc(z.schema_version ?? '--')}</span>
        <span class="k">worlds</span>        <span class="v">${worldsStr}</span>
        <span class="k">world ids</span>     <span class="v">${worldIdsStr}</span>
        <span class="k">sessions</span>      <span class="v">${fmtNum(z.sessions_merged)}</span>
        <span class="k">saturated</span>     <span class="v ${satClass}">${satText}</span>
        <span class="k">merged</span>        <span class="v">${fmtTs(z.merged_at)}</span>
        <span class="k">resolution</span>    <span class="v">${z.grid?.resolution ?? '--'} m/cell</span>
        <span class="k">extent</span>        <span class="v">${extentStr}</span>
        <span class="k">bbox</span>          <span class="v">${bboxStr}</span>
        <span class="k">floors</span>        <span class="v">${floorIds.length}</span>
        <span class="k">total cells</span>   <span class="v">${fmtNum(totalCells)}</span>
        <span class="k">total actors</span>  <span class="v">${fmtNum(totalActors)}</span>
        <span class="k">cur floor</span>     <span class="v">${esc(S.currentFloor ?? '--')}</span>
      </div>
      ${floorRows ? `
      <div class="det-subsection">
        <h4>per-floor</h4>
        <table class="det-table">
          <thead><tr>
            <th>floor</th><th>cells</th><th>actors</th>
            <th>world(s)</th><th>world id(s)</th><th>sessions</th>
          </tr></thead>
          <tbody>${floorRows}</tbody>
        </table>
      </div>` : ''}
      ${kindRows ? `
      <div class="det-subsection">
        <h4>actors on floor ${esc(S.currentFloor ?? '?')} by kind</h4>
        <table class="det-table">
          <thead><tr><th>kind</th><th>count</th></tr></thead>
          <tbody>${kindRows}</tbody>
        </table>
      </div>` : ''}
    `;
}

// Toggle handler: flip aria-expanded + show/hide the panel.  Wired once
// at module load; works whether or not a zone is currently loaded.
if (D.zoneDetailsToggle) {
    D.zoneDetailsToggle.addEventListener('click', () => {
        const expanded = D.zoneDetailsToggle.getAttribute('aria-expanded') === 'true';
        const next = !expanded;
        D.zoneDetailsToggle.setAttribute('aria-expanded', String(next));
        D.zoneDetails.hidden = !next;
        if (next) renderZoneDetails();   // populate-on-open in case state changed
    });
}

D.floorSelect.addEventListener('change', e => {
    S.currentFloor = e.target.value;
    S.selectedActor = null;
    renderActorPanel();
    rebuildCellSet();
    renderMeta();
    render();
});

function refreshOrientState() {
    const r = S.orient.rot * 90;
    const parts = [`rot ${r}°`];
    if (S.orient.flipX) parts.push('flipX');
    if (S.orient.flipY) parts.push('flipY');
    D.orientState.textContent = parts.join(' · ');
    D.orientFlipX.classList.toggle('armed', S.orient.flipX);
    D.orientFlipY.classList.toggle('armed', S.orient.flipY);
}
D.orientRotate.addEventListener('click', () => {
    S.orient.rot = (S.orient.rot + 1) % 4;
    saveOrient(); refreshOrientState(); render();
});
D.orientFlipX.addEventListener('click', () => {
    S.orient.flipX = !S.orient.flipX;
    saveOrient(); refreshOrientState(); render();
});
D.orientFlipY.addEventListener('click', () => {
    S.orient.flipY = !S.orient.flipY;
    saveOrient(); refreshOrientState(); render();
});
refreshOrientState();

// ---- Canvas rendering ----------------------------------------------------
function bboxOfCells(cells) {
    let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    for (const c of cells) {
        if (c[0] < minx) minx = c[0];
        if (c[1] < miny) miny = c[1];
        if (c[0] > maxx) maxx = c[0];
        if (c[1] > maxy) maxy = c[1];
    }
    return { minx, miny, maxx, maxy };
}

let drawState = null;

function render() {
    if (!S.currentData || !S.currentFloor) return;
    const cells = S.currentData.grid?.floors?.[S.currentFloor] || [];
    const w = D.canvas.width, h = D.canvas.height;
    ctx.fillStyle = '#06090d';
    ctx.fillRect(0, 0, w, h);
    if (!cells.length) {
        ctx.fillStyle = '#8b949e'; ctx.font = '14px sans-serif';
        ctx.fillText('(no cells on this floor)', 20, 24);
        return;
    }
    const rawBbox = bboxOfCells(cells);
    // Transform every corner of rawBbox to find the post-orient bbox.
    const corners = [
        applyOrient(rawBbox.minx, rawBbox.miny, rawBbox),
        applyOrient(rawBbox.maxx, rawBbox.miny, rawBbox),
        applyOrient(rawBbox.minx, rawBbox.maxy, rawBbox),
        applyOrient(rawBbox.maxx, rawBbox.maxy, rawBbox),
    ];
    const bbox = {
        minx: Math.min(...corners.map(c => c.x)),
        miny: Math.min(...corners.map(c => c.y)),
        maxx: Math.max(...corners.map(c => c.x)),
        maxy: Math.max(...corners.map(c => c.y)),
    };
    const pad = 20;
    const dx = bbox.maxx - bbox.minx + 1;
    const dy = bbox.maxy - bbox.miny + 1;
    const fitScale = Math.min((w - 2*pad)/dx, (h - 2*pad)/dy);
    const scale = fitScale * S.view.scale;
    const offX = (w - dx*scale)/2 + S.view.panX - bbox.minx*scale;
    const offY = (h - dy*scale)/2 + S.view.panY - bbox.miny*scale;
    drawState = { bbox, rawBbox, scale, offX, offY };

    const cellSize = Math.max(1, scale);
    if (S.layers?.walkable_grid !== false) {
        // Draw a subtle "no-data" tint over the entire zone bbox FIRST,
        // so anywhere the recorder hasn't sampled yet (boss arenas
        // gated by event-doors / firewalls, off-path corners, etc.)
        // shows up as a faint blue-grey rather than the same scary
        // jet-black as "outside the world entirely."  Walkable + blocked
        // cells get drawn on top of this tint, overwriting it where we
        // do have data.
        const bbX = offX + bbox.minx*scale;
        const bbY = (h - offY) - bbox.maxy*scale;
        const bbW = (bbox.maxx - bbox.minx + 1) * scale;
        const bbH = (bbox.maxy - bbox.miny + 1) * scale;
        ctx.fillStyle = 'rgba(50, 60, 80, 0.55)';
        ctx.fillRect(bbX, bbY, bbW, bbH);

        for (const c of cells) {
            const t = applyOrient(c[0], c[1], rawBbox);
            const walk = c[2], conf = c[3];
            const x = offX + t.x*scale;
            const y = (h - offY) - t.y*scale - cellSize;
            ctx.fillStyle = walk
                ? `rgba(63, 185, 80, ${0.35 + 0.65*conf})`
                : `rgba(207, 52, 52, ${0.35 + 0.65*conf})`;
            ctx.fillRect(x, y, cellSize, cellSize);
        }
    }

    // Actors -- filter by floor + active layer toggles
    const layers = S.layers || {};
    const actors = (S.currentData.actors || []).filter(a => a.floor == null || String(a.floor) === S.currentFloor);
    const cellRes = S.cellRes;
    const showLabels = scale >= 5;
    const hits = [];
    for (const a of actors) {
        if (typeof a.x !== 'number') continue;
        if (a.x === 0 && a.y === 0) continue;
        // Layer gate: skip the actor if its category is hidden.  Unknown
        // kinds fall into 'other' so they can be hidden via that toggle
        // without needing to land in KIND_CATEGORY explicitly.
        const effKind = effectiveActorKind(a);
        const cat = KIND_CATEGORY[effKind] || 'other';
        if (layers['actors_' + cat] === false) continue;
        // Server-ignore-list filter: hide actors whose skin matches any
        // dynamic-ignore pattern unless the user has clicked 'show in
        // viewer anyway' for that specific skin.  Provides instant hide
        // for newly-clicked ignores (before the merger drops them from
        // the merged JSON on its next cycle).
        if (isHiddenSkin(a.skin)) continue;
        const t = applyOrient(a.x / cellRes, a.y / cellRes, rawBbox);
        const x = offX + t.x*scale, y = (h - offY) - t.y*scale;
        const style = ACTOR_STYLE[effKind] || { c:'#999', sym:'?' };
        const isSel = (a === S.selectedActor), isHov = (a === S.hoveredActor);
        // Cluster-spread ring: server-side mobile-actor collapse
        // condenses N positional sightings of a boss/champion into one
        // centroid + a `cluster_spread_m` field.  Drawing a faint ring
        // at world-radius=spread tells the user "this single pin
        // represents a boss that ranged X meters around this point".
        // Only drawn when meaningful (>1m) so single-position bosses
        // don't get a 0-radius ring.
        if (a.cluster_spread_m && a.cluster_spread_m > 1) {
            const ringPx = a.cluster_spread_m / cellRes * scale;
            if (ringPx > 4) {
                ctx.beginPath();
                ctx.arc(x, y, ringPx, 0, 2 * Math.PI);
                ctx.strokeStyle = isSel
                    ? 'rgba(255, 215, 0, 0.55)'
                    : 'rgba(248, 81, 73, 0.32)';
                ctx.lineWidth = 1;
                ctx.setLineDash([3, 3]);
                ctx.stroke();
                ctx.setLineDash([]);
            }
        }
        if (isSel || isHov) {
            ctx.beginPath(); ctx.arc(x, y, isSel ? 13 : 10, 0, 2*Math.PI);
            ctx.strokeStyle = isSel ? '#ffd700' : '#fff'; ctx.lineWidth = isSel ? 2 : 1.5; ctx.stroke();
        }
        ctx.save();
        ctx.translate(x, y); ctx.rotate(Math.PI/4);
        ctx.fillStyle = style.c; ctx.strokeStyle = '#000'; ctx.lineWidth = 1;
        const sz = isSel ? 7 : 5;
        ctx.fillRect(-sz,-sz,sz*2,sz*2); ctx.strokeRect(-sz,-sz,sz*2,sz*2);
        ctx.restore();
        if (scale >= 3) {
            ctx.fillStyle = '#000'; ctx.font = 'bold 9px Consolas, monospace';
            ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
            ctx.fillText(style.sym, x, y); ctx.textAlign = 'start';
        }
        if (showLabels || isSel) {
            ctx.fillStyle = style.c; ctx.font = (isSel?'11px ':'10px ') + 'Consolas, monospace';
            ctx.textBaseline = 'middle';
            ctx.fillText(actorDisplayName(a), x + 11, y - 1);
        }
        hits.push({ x, y, r: 10, actor: a });
    }
    drawState.hits = hits;

    // Uploader live tracks (for the active zone)
    if (S.layers?.uploader_tracks !== false && S.activeUploader && S.uploaderTracks) {
        for (const [name, t] of Object.entries(S.uploaderTracks)) {
            if (t.zone !== S.currentKey) continue;
            const samples = t.samples || [];
            if (samples.length < 2) continue;
            ctx.beginPath();
            for (let i = 0; i < samples.length; i++) {
                const s = samples[i];
                const tt = applyOrient(s.x / cellRes, s.y / cellRes, rawBbox);
                const sx = offX + tt.x * scale;
                const sy = (h - offY) - tt.y * scale;
                if (i === 0) ctx.moveTo(sx, sy); else ctx.lineTo(sx, sy);
            }
            ctx.strokeStyle = t.complete ? 'rgba(255, 200, 60, 0.65)' : 'rgba(80, 220, 140, 0.85)';
            ctx.lineWidth = 2; ctx.stroke();
            const last = samples[samples.length - 1];
            const lt = applyOrient(last.x / cellRes, last.y / cellRes, rawBbox);
            const lx = offX + lt.x * scale;
            const ly = (h - offY) - lt.y * scale;
            ctx.beginPath();
            ctx.fillStyle = t.complete ? '#ffc83c' : '#3fff8b';
            ctx.arc(lx, ly, 4, 0, 2*Math.PI); ctx.fill();
            ctx.strokeStyle = '#000'; ctx.lineWidth = 1; ctx.stroke();
        }
    }

    // Path simulator overlay
    if (S.layers?.path_overlay !== false) {
    if (S.pathA) drawWorldDot(S.pathA, '#58a6ff', 'A');
    if (S.pathB) drawWorldDot(S.pathB, '#58a6ff', 'B');
    if (S.pathPath && S.pathPath.length >= 2) {
        ctx.beginPath();
        for (let i = 0; i < S.pathPath.length; i++) {
            const c = S.pathPath[i];
            const t = applyOrient(c.cx, c.cy, rawBbox);
            const px = offX + t.x * scale + scale/2;
            const py = (h - offY) - t.y * scale - scale/2;
            if (i === 0) ctx.moveTo(px, py); else ctx.lineTo(px, py);
        }
        ctx.strokeStyle = '#58a6ff'; ctx.lineWidth = 2; ctx.stroke();
    }
    } // end path_overlay layer gate
}

function drawWorldDot(p, color, label) {
    const h = D.canvas.height;
    const cellRes = S.cellRes;
    const t = applyOrient(p.wx / cellRes, p.wy / cellRes, drawState.rawBbox);
    const px = drawState.offX + t.x * drawState.scale;
    const py = (h - drawState.offY) - t.y * drawState.scale;
    ctx.beginPath(); ctx.arc(px, py, 6, 0, 2*Math.PI);
    ctx.fillStyle = color; ctx.fill();
    ctx.strokeStyle = '#000'; ctx.lineWidth = 1.5; ctx.stroke();
    if (label) {
        ctx.fillStyle = '#fff'; ctx.font = 'bold 10px sans-serif';
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillText(label, px, py); ctx.textAlign = 'start';
    }
}

// ---- Pan / zoom / click / hover ------------------------------------------
function canvasFromEvent(e) {
    const r = D.canvas.getBoundingClientRect();
    const sx = D.canvas.width / r.width, sy = D.canvas.height / r.height;
    return { x: (e.clientX - r.left) * sx, y: (e.clientY - r.top) * sy };
}
function canvasToWorld(cx, cy) {
    if (!drawState) return null;
    const h = D.canvas.height;
    // Step 1: canvas -> post-orient cell coords
    const tx = (cx - drawState.offX) / drawState.scale;
    const ty = ((h - drawState.offY) - cy) / drawState.scale;
    // Step 2: invert the orient transform to get raw cell coords
    const inv = invertOrient(tx, ty, drawState.rawBbox);
    return { wx: inv.x * S.cellRes, wy: inv.y * S.cellRes };
}

function invertOrient(tx, ty, bbox) {
    // Inverse of applyOrient: undo rotation, then undo flip.
    let x = tx, y = ty;
    const r = ((S.orient.rot % 4) + 4) % 4;
    if (r !== 0) {
        const cxC = (bbox.minx + bbox.maxx) / 2;
        const cyC = (bbox.miny + bbox.maxy) / 2;
        const dx = x - cxC, dy = y - cyC;
        if      (r === 1) { x = cxC - dy; y = cyC + dx; }   // inverse of 90 cw
        else if (r === 2) { x = cxC - dx; y = cyC - dy; }
        else              { x = cxC + dy; y = cyC - dx; }
    }
    if (S.orient.flipY) y = bbox.miny + (bbox.maxy - y);
    if (S.orient.flipX) x = bbox.minx + (bbox.maxx - x);
    return { x, y };
}
function pickActor(cx, cy) {
    if (!drawState || !drawState.hits) return null;
    let best = null, bd2 = Infinity;
    for (const h of drawState.hits) {
        const d2 = (h.x - cx)**2 + (h.y - cy)**2;
        if (d2 < bd2 && d2 <= h.r * h.r * 1.5) { best = h; bd2 = d2; }
    }
    return best ? best.actor : null;
}

D.canvas.addEventListener('mousedown', e => {
    S.drag = { x: e.clientX, y: e.clientY, panX: S.view.panX, panY: S.view.panY, moved: false };
});

window.addEventListener('mouseup', e => {
    if (S.drag && !S.drag.moved) {
        const { x, y } = canvasFromEvent(e);
        if (S.pathMode === 'pickA' || S.pathMode === 'pickB') {
            const w = canvasToWorld(x, y);
            if (w) {
                if (S.pathMode === 'pickA') S.pathA = w; else S.pathB = w;
                S.pathMode = null;
                D.pathPickA.classList.remove('armed');
                D.pathPickB.classList.remove('armed');
                if (S.pathA && S.pathB) computePath();
                else updatePathStatus();
                render();
            }
        } else {
            const a = pickActor(x, y);
            S.selectedActor = a;
            renderActorPanel();
            render();
        }
    }
    S.drag = null;
});

window.addEventListener('mousemove', e => {
    if (S.drag) {
        const dx = e.clientX - S.drag.x, dy = e.clientY - S.drag.y;
        if (Math.abs(dx)+Math.abs(dy) > 3) S.drag.moved = true;
        const r = D.canvas.getBoundingClientRect();
        const sx = D.canvas.width / r.width, sy = D.canvas.height / r.height;
        S.view.panX = S.drag.panX + dx*sx; S.view.panY = S.drag.panY - dy*sy;
        render();
    } else {
        const { x, y } = canvasFromEvent(e);
        const hit = pickActor(x, y);
        if (hit !== S.hoveredActor) { S.hoveredActor = hit; render(); }
        if (hit) {
            const name = actorDisplayName(hit);
            D.tooltip.innerHTML =
                `<div class="h-name">${esc(name)}</div>` +
                `<div class="h-skin">${esc(hit.skin || '?')}</div>` +
                `<div class="h-meta">${esc(hit.kind || '?')} · (${hit.x?.toFixed?.(1)}, ${hit.y?.toFixed?.(1)}, ${hit.z?.toFixed?.(1)})</div>`;
            const r = D.canvas.getBoundingClientRect();
            D.tooltip.style.left = (e.clientX - r.left + 12) + 'px';
            D.tooltip.style.top  = (e.clientY - r.top + 12) + 'px';
            D.tooltip.hidden = false;
        } else {
            D.tooltip.hidden = true;
        }
    }
});

D.canvas.addEventListener('wheel', e => {
    e.preventDefault();
    // Zoom toward the cursor: keep whatever world-point sits under the
    // mouse fixed on screen as the scale changes.
    //
    // The render pipeline composes scale as `scale = fitScale * S.view.scale`
    // and centers the bbox via:
    //   screen_x = w/2 + scale * (world_x - bbox.midx) + panX
    //   screen_y = h/2 + scale * (bbox.midy_inv - world_y) - panY     (Y flipped)
    //
    // Holding (world_x, world_y) under (mx, my) constant before and after
    // a scale-by-R change yields:
    //   newPanX = R * oldPanX - (R - 1) * (mx - w/2)
    //   newPanY = R * oldPanY + (R - 1) * (my - h/2)
    //
    // Note the sign flip on Y: panY accumulates inverted in the transform
    // (see mousemove drag handler: panY -= dy*sy on a downward drag).
    const rect = D.canvas.getBoundingClientRect();
    // Match the drag handler's scaling: clientX/Y are CSS px but the
    // canvas backing store is D.canvas.width/.height pixels.
    const sx = D.canvas.width  / rect.width;
    const sy = D.canvas.height / rect.height;
    const mx = (e.clientX - rect.left) * sx;
    const my = (e.clientY - rect.top)  * sy;
    const w  = D.canvas.width;
    const h  = D.canvas.height;
    const f  = e.deltaY > 0 ? 0.9 : 1.1;
    const oldScale = S.view.scale;
    const newScale = Math.max(0.1, Math.min(80, oldScale * f));
    if (newScale === oldScale) { return; }     // hit a clamp; nothing to do
    const r = newScale / oldScale;
    S.view.panX = r * S.view.panX - (r - 1) * (mx - w / 2);
    S.view.panY = r * S.view.panY + (r - 1) * (my - h / 2);
    S.view.scale = newScale;
    render();
}, { passive: false });

D.canvas.addEventListener('mouseleave', () => { D.tooltip.hidden = true; S.hoveredActor = null; render(); });

// ---- Floating canvas controls (zoom in/out, fit, zoom %) ----------------
// Buttons live in .canvas-controls (top-right of canvas-wrap).  They map
// to the same math as the wheel handler but anchored at canvas center,
// so clicking + or - zooms the middle of the view.  Keyboard shortcut
// 'f' fits the zone to the viewport.
function zoomBy(factor) {
    const w = D.canvas.width;
    const h = D.canvas.height;
    const oldScale = S.view.scale;
    const newScale = Math.max(0.1, Math.min(80, oldScale * factor));
    if (newScale === oldScale) return;
    // Anchor at canvas center so button-driven zoom feels stable.
    const r = newScale / oldScale;
    S.view.panX = r * S.view.panX;
    S.view.panY = r * S.view.panY;
    S.view.scale = newScale;
    updateZoomReadout();
    render();
}
function fitToZone() {
    S.view = { panX: 0, panY: 0, scale: 1.0 };
    updateZoomReadout();
    render();
}
function updateZoomReadout() {
    const el = document.getElementById('canvas-zoom-pct');
    if (el) el.textContent = Math.round(S.view.scale * 100) + '%';
}
document.getElementById('canvas-zoom-in') ?.addEventListener('click', () => zoomBy(1.25));
document.getElementById('canvas-zoom-out')?.addEventListener('click', () => zoomBy(0.8));
document.getElementById('canvas-fit')     ?.addEventListener('click', () => fitToZone());
window.addEventListener('keydown', (e) => {
    // Skip when user is typing in an input
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName)) return;
    if (e.key === 'f' || e.key === 'F') { fitToZone(); }
    if (e.key === '+' || e.key === '=') { zoomBy(1.25); }
    if (e.key === '-' || e.key === '_') { zoomBy(0.8); }
});
// Update readout on every wheel zoom too -- the wheel handler doesn't
// route through zoomBy().  Cheap to call render() once but we just need
// to read the post-render scale, so subscribe via a small wrapper.
const _origRender = window.render;   // noop -- the IIFE pattern means
// `render` isn't on window; instead we hook by modifying view.scale.
// Simplest: add an after-render callback by walking through wheel handler.
// The wheel handler at line ~1027 mutates S.view.scale and calls render();
// piggy-back by listening to the canvas wheel event AFTER its handler.
D.canvas.addEventListener('wheel', updateZoomReadout, { passive: true });

// ---- Actor info panel ----------------------------------------------------
function renderActorPanel() {
    const a = S.selectedActor;
    if (!a) { D.actorPanel.hidden = true; return; }
    D.actorPanel.hidden = false;
    const rows = [];
    const row = (k, v, cls) => {
        if (v === undefined || v === null || v === '') return;
        rows.push(`<div class="row${cls?' '+cls:''}"><span class="k">${esc(k)}</span><span class="v">${esc(String(v))}</span></div>`);
    };
    row('name',   actorDisplayName(a), 'kind');
    row('kind',   a.kind);
    row('skin',   a.skin, 'skin');
    row('id',     a.id);
    row('type_id',a.type_id);
    row('sno_id', a.sno_id);
    row('x', typeof a.x === 'number' ? a.x.toFixed(2) : a.x);
    row('y', typeof a.y === 'number' ? a.y.toFixed(2) : a.y);
    row('z', typeof a.z === 'number' ? a.z.toFixed(2) : a.z);
    row('floor', a.floor);
    row('radius', a.radius);
    row('sessions seen', a.sessions_seen);
    row('observations',  a.total_observations);
    // Server-side mobile-actor cluster diagnostics.  Only set on
    // bosses/champions whose multiple sightings collapsed into one
    // centroid entry.  Lets the user tell at a glance whether they're
    // looking at a real single point (no rows) or a collapsed pin
    // (e.g. "12 positions, 6.4m spread" => boss room with the boss
    // ranging 6m around the centroid).
    if (a.cluster_positions) row('positions', a.cluster_positions);
    if (a.cluster_spread_m)  row('spread',    a.cluster_spread_m + ' m');
    if (a.is_boss)  row('flag', 'BOSS');
    if (a.is_elite) row('flag', 'ELITE');

    // Admin-only label + kind override editor.  Renames just THIS actor
    // (keyed by zone+skin+rounded_pos+floor) without affecting other
    // actors of the same skin.  Useful for naming dungeon-portal
    // entrances by their destination dungeon, calling out a specific
    // chest, etc.
    if (S.tier === 'admin' && S.currentKey && a.skin) {
        const existing = lookupActorLabel(a) || {};
        const allKinds = Object.keys(ACTOR_STYLE).sort();
        const kindOptions = ['<option value="">(use auto-detected: ' + esc(a.kind || '?') + ')</option>']
            .concat(allKinds.map(k => `<option value="${esc(k)}"${k === existing.kind_override ? ' selected' : ''}>${esc(ACTOR_STYLE[k].label || k)} — <code>${esc(k)}</code></option>`))
            .join('');
        rows.push(
            `<div class="actor-edit">
                <div class="actor-edit-row">
                    <label>Rename
                        <input type="text" class="actor-edit-label"
                               placeholder="${esc(actorDisplayName(a))}"
                               value="${esc(existing.label || '')}" maxlength="80">
                    </label>
                </div>
                <div class="actor-edit-row">
                    <label>Reclassify
                        <select class="actor-edit-kind">${kindOptions}</select>
                    </label>
                </div>
                <div class="actor-actions">
                    <button class="actor-save-btn">Save label</button>
                    ${(existing.label || existing.kind_override)
                        ? '<button class="actor-clear-btn ghost">Clear</button>' : ''}
                    <span class="actor-edit-status muted"></span>
                </div>
            </div>`);
    }

    // Admin-only "Ignore this skin" action (separate from the label
    // editor above).  Hides every actor with this skin via the global
    // server ignore list -- different scope from per-actor labels.
    if (S.tier === 'admin' && a.skin) {
        rows.push(
            `<div class="actor-actions">
                <button class="ignore-skin-btn" data-skin="${esc(a.skin)}"
                        title="Add this skin to the server-side ignore list. Merger will drop it on next cycle; recorder picks up the change on its next /reload via the uploader.">
                    Ignore this skin
                </button>
                <span class="ignore-skin-status muted"></span>
            </div>`);
    }
    D.actorBody.innerHTML = rows.join('');

    // ---- Wire the label editor (admin only) ----
    const saveBtn  = D.actorBody.querySelector('.actor-save-btn');
    const clearBtn = D.actorBody.querySelector('.actor-clear-btn');
    const labelInp = D.actorBody.querySelector('.actor-edit-label');
    const kindSel  = D.actorBody.querySelector('.actor-edit-kind');
    const editStat = D.actorBody.querySelector('.actor-edit-status');
    async function saveLabel(label, kindOverride) {
        if (editStat) editStat.textContent = 'saving...';
        try {
            const r = await adminFetch('/admin/labels', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    zone:  S.currentKey,
                    skin:  a.skin,
                    rx:    Math.round(a.x || 0),
                    ry:    Math.round(a.y || 0),
                    floor: a.floor || 1,
                    label,
                    kind_override: kindOverride,
                }),
            });
            if (!r.ok) {
                if (editStat) editStat.textContent = 'failed: HTTP ' + r.status;
                return;
            }
            await refreshActorLabels();
            renderActorPanel();   // re-render with the new state
            render();
        } catch (e) {
            if (editStat) editStat.textContent = 'failed: ' + (e?.message || e);
        }
    }
    if (saveBtn) {
        saveBtn.addEventListener('click', () => {
            saveLabel(labelInp ? labelInp.value : '', kindSel ? kindSel.value : '');
        });
    }
    if (clearBtn) {
        clearBtn.addEventListener('click', () => saveLabel('', ''));
    }
    if (labelInp) {
        labelInp.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                saveLabel(labelInp.value, kindSel ? kindSel.value : '');
            }
        });
    }

    // ---- Wire the "Ignore this skin" button (separate scope) ----
    const btn = D.actorBody.querySelector('.ignore-skin-btn');
    const status = D.actorBody.querySelector('.ignore-skin-status');
    if (btn) {
        btn.addEventListener('click', async () => {
            const skin = btn.dataset.skin;
            btn.disabled = true;
            if (status) status.textContent = 'sending...';
            try {
                const r = await adminFetch('/admin/ignore', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ pattern: skin, note: 'viewer-click' }),
                });
                if (!r.ok) {
                    if (status) status.textContent = 'failed: HTTP ' + r.status;
                    btn.disabled = false;
                    return;
                }
                const j = await r.json();
                if (status) status.textContent = j.added ? 'added ✓' : 'already ignored';
                btn.textContent = 'Ignored ✓';
                // Instant viewer-side hide: add to the local mirror and
                // re-render right away.  isHiddenSkin() will start
                // returning true for this skin so the actor (and any
                // others with the same skin) disappears from the map
                // immediately, without waiting for the next merge cycle
                // to drop them from the JSON.
                if (S.serverIgnorePatterns.indexOf(skin) === -1) {
                    S.serverIgnorePatterns.push(skin);
                }
                S.selectedActor = null;
                renderActorPanel();
                renderHiddenSkinsSection();
                render();
                // Also trigger an immediate server re-merge so the
                // canonical JSON drops it within seconds (rather than
                // 30s when the next scheduler tick fires).
                adminFetch('/merge', { method: 'POST' });
            } catch (e) {
                if (status) status.textContent = 'failed: ' + (e?.message || e);
                btn.disabled = false;
            }
        });
    }
}
D.actorClose.addEventListener('click', () => { S.selectedActor = null; renderActorPanel(); render(); });

// ---- Tabs ----------------------------------------------------------------
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        const target = tab.dataset.tab;
        document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t === tab));
        document.querySelectorAll('.tab-pane').forEach(p => {
            const match = p.dataset.tab === target;
            p.hidden = !match;
            p.classList.toggle('active', match);
        });
        // Lazy-render the Layers panel on first activation; subsequent
        // clicks just re-show the cached DOM.
        if (target === 'layers') renderLayerPanel();
    });
});

// ---- Layer panel ---------------------------------------------------------
//
// Categories (loot/threats/travel/...) and the three pseudo-layers
// (walkable_grid, uploader_tracks, path_overlay) each get a checkbox row.
// Toggling persists to localStorage and re-renders the canvas.
function persistLayers() {
    try { localStorage.setItem('warmap_layers', JSON.stringify(S.layers)); } catch (e) {}
}
function setLayer(id, on) {
    S.layers[id] = !!on;
    persistLayers();
    render();
    // Visually update the row's "disabled" styling without a full re-render.
    const row = document.querySelector(`[data-layer="${id}"]`);
    if (row) row.classList.toggle('disabled', !on);
}
// ---- Hidden-skins filter section -----------------------------------------
// Lives at the bottom of the Layers tab.  Lists every dynamic ignore
// pattern, with a toggle to override (show in viewer anyway) and an
// admin-only 'remove from server list' button.
function renderHiddenSkinsSection() {
    const list  = document.getElementById('hidden-skins-list');
    const count = document.getElementById('hidden-skins-count');
    if (!list) return;
    const patterns = S.serverIgnorePatterns || [];
    if (count) count.textContent = patterns.length ? `(${patterns.length})` : '';
    if (!patterns.length) {
        list.innerHTML = '<div class="hint muted">No skins ignored yet.  '
            + 'Click any actor on the map and use "Ignore this skin" '
            + 'to add patterns here.</div>';
        return;
    }
    const isAdmin = S.tier === 'admin';
    const rows = patterns.slice().sort().map(p => {
        const overridden = S.viewerShowOverrides.has(p);
        const checked   = !overridden;
        const removeBtn = isAdmin
            ? `<button class="hidden-skin-remove" data-pat="${esc(p)}"
                       title="Remove from server's ignore list (un-ignore everywhere)">✕</button>`
            : '';
        return `<label class="layer-row${overridden ? '' : ' disabled'}" data-skin-pat="${esc(p)}">
            <input type="checkbox" data-hidden-skin="${esc(p)}" ${checked ? 'checked' : ''}>
            <span class="lbl"><code>${esc(p)}</code>${overridden ? '<span class="sub">(showing)</span>' : ''}</span>
            ${removeBtn}
        </label>`;
    }).join('');
    list.innerHTML = rows;
    list.querySelectorAll('input[data-hidden-skin]').forEach(inp => {
        inp.addEventListener('change', () => {
            const pat = inp.dataset.hiddenSkin;
            if (inp.checked) {
                // Checked = "hide" (the default state); remove from
                // overrides so the filter applies again.
                S.viewerShowOverrides.delete(pat);
            } else {
                // Unchecked = "show in viewer anyway".
                S.viewerShowOverrides.add(pat);
            }
            persistShowOverrides();
            renderHiddenSkinsSection();
            render();
        });
    });
    list.querySelectorAll('.hidden-skin-remove').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.preventDefault();
            const pat = btn.dataset.pat;
            if (!confirm(`Remove "${pat}" from the server's ignore list?\n\n` +
                'The pattern will be dropped server-side -- new actors with this ' +
                'skin will start appearing again on future recordings.  Existing ' +
                'merged data is unaffected (the merger already dropped them).')) return;
            try {
                const r = await adminFetch('/admin/ignore/' + encodeURIComponent(pat), { method: 'DELETE' });
                if (!r.ok) { alert('Remove failed: HTTP ' + r.status); return; }
                S.serverIgnorePatterns = S.serverIgnorePatterns.filter(x => x !== pat);
                S.viewerShowOverrides.delete(pat);
                persistShowOverrides();
                renderHiddenSkinsSection();
                render();
            } catch (e) {
                alert('Remove failed: ' + (e?.message || e));
            }
        });
    });
}

function renderLayerPanel() {
    const host = document.getElementById('layer-panel');
    if (!host) return;
    const sectionRow = (id, label, color, desc) => {
        const on = S.layers[id] !== false;
        return `
            <label class="layer-row${on ? '' : ' disabled'}" data-layer="${id}">
                <input type="checkbox" data-layer-input="${id}" ${on ? 'checked' : ''}>
                <span class="swatch-dot" style="background:${color}"></span>
                <span class="lbl">${esc(label)}<span class="sub">${esc(desc)}</span></span>
            </label>`;
    };
    let html = '<div class="layer-section"><h3>Actors</h3>';
    for (const c of LAYER_CATEGORIES) {
        html += sectionRow('actors_' + c.id, c.label, c.color, c.desc);
    }
    html += '</div><div class="layer-section"><h3>Map overlays</h3>';
    for (const p of PSEUDO_LAYERS) {
        html += sectionRow(p.id, p.label, p.color, p.desc);
    }
    html += '</div>';
    host.innerHTML = html;
    host.querySelectorAll('input[data-layer-input]').forEach(inp => {
        inp.addEventListener('change', () => {
            setLayer(inp.dataset.layerInput, inp.checked);
        });
    });
}
// Quick actions
document.getElementById('layers-show-all')?.addEventListener('click', () => {
    for (const k of Object.keys(S.layers)) S.layers[k] = true;
    persistLayers();
    renderLayerPanel();
    render();
});
document.getElementById('layers-hide-all')?.addEventListener('click', () => {
    for (const k of Object.keys(S.layers)) S.layers[k] = false;
    persistLayers();
    renderLayerPanel();
    render();
});
document.getElementById('layers-reset')?.addEventListener('click', () => {
    S.layers = defaultLayerState();
    persistLayers();
    renderLayerPanel();
    render();
});

// ---- Path simulator ------------------------------------------------------
D.pathPickA.addEventListener('click', () => armPathPick('pickA'));
D.pathPickB.addEventListener('click', () => armPathPick('pickB'));
D.pathClear.addEventListener('click', () => {
    S.pathA = S.pathB = S.pathPath = null;
    S.pathMode = null;
    D.pathPickA.classList.remove('armed');
    D.pathPickB.classList.remove('armed');
    updatePathStatus();
    render();
});

function armPathPick(mode) {
    S.pathMode = (S.pathMode === mode) ? null : mode;
    D.pathPickA.classList.toggle('armed', S.pathMode === 'pickA');
    D.pathPickB.classList.toggle('armed', S.pathMode === 'pickB');
    updatePathStatus();
}

function updatePathStatus() {
    if (S.pathMode === 'pickA')      D.pathStatus.textContent = 'Click on the map to set point A.';
    else if (S.pathMode === 'pickB') D.pathStatus.textContent = 'Click on the map to set point B.';
    else if (S.pathA && !S.pathB)    D.pathStatus.textContent = 'A set. Click "Pick B" then click on the map.';
    else if (S.pathPath)             D.pathStatus.textContent = `Path: ${S.pathPath.length} cells.`;
    else                             D.pathStatus.textContent = 'Click "Pick A" then click on the map. Then "Pick B" + click.';
    if (!S.pathPath) D.pathResult.innerHTML = '';
}

function computePath() {
    if (!S.pathA || !S.pathB || !S.cellSet) return;
    const sx = Math.round(S.pathA.wx / S.cellRes);
    const sy = Math.round(S.pathA.wy / S.cellRes);
    const gx = Math.round(S.pathB.wx / S.cellRes);
    const gy = Math.round(S.pathB.wy / S.cellRes);
    const startCell = nearestWalkable(sx, sy);
    const goalCell  = nearestWalkable(gx, gy);
    if (!startCell || !goalCell) {
        S.pathPath = null;
        D.pathStatus.textContent = 'Picked points outside known walkable area.';
        D.pathResult.innerHTML = '';
        return;
    }
    const t0 = performance.now();
    const path = aStar(startCell, goalCell);
    const dt = (performance.now() - t0).toFixed(0);
    S.pathPath = path;
    if (path) {
        const dist = (path.length * S.cellRes).toFixed(1);
        D.pathStatus.textContent = `${path.length} cells · ~${dist}m · ${dt}ms`;
        D.pathResult.innerHTML =
            `<div class="row"><span class="k">A</span><span>(${S.pathA.wx.toFixed(1)}, ${S.pathA.wy.toFixed(1)})</span></div>` +
            `<div class="row"><span class="k">B</span><span>(${S.pathB.wx.toFixed(1)}, ${S.pathB.wy.toFixed(1)})</span></div>` +
            `<div class="row"><span class="k">cells</span><span>${path.length}</span></div>` +
            `<div class="row"><span class="k">distance</span><span>~${dist}m</span></div>` +
            `<div class="row"><span class="k">elapsed</span><span>${dt}ms</span></div>`;
    } else {
        D.pathStatus.textContent = 'No path found in merged cells.';
        D.pathResult.innerHTML = '';
    }
    render();
}

function nearestWalkable(cx, cy) {
    if (S.cellSet.has(cx + ',' + cy)) return { cx, cy };
    for (let r = 1; r < 30; r++) {
        for (let dx = -r; dx <= r; dx++) {
            for (let dy = -r; dy <= r; dy++) {
                if (Math.abs(dx) !== r && Math.abs(dy) !== r) continue;
                const k = (cx+dx) + ',' + (cy+dy);
                if (S.cellSet.has(k)) return { cx: cx+dx, cy: cy+dy };
            }
        }
    }
    return null;
}

// ---- Wall-distance transform (centerline routing) ----------------------
// For each walkable cell, BFS-distance to the nearest non-walkable cell
// (or boundary).  Used by aStar() to penalize cells near walls so paths
// prefer the middle of corridors -- avoids the "hug the wall" look A*
// gets when shortest distance is the only objective.
//
// Multi-source BFS seeded with every walkable cell that has at least
// one non-walkable 4-neighbor (those have dist=1).  Linear in #walkable
// cells, runs once per zone-load via rebuildCellSet().
function computeWallDistance(cellSet) {
    const dist = new Map();
    const queue = [];
    const ADJ = [[1,0],[-1,0],[0,1],[0,-1]];
    // Seed: walkable cells touching a non-walkable get dist=1
    for (const k of cellSet) {
        const [cx, cy] = k.split(',').map(Number);
        for (let i = 0; i < 4; i++) {
            const nk = (cx + ADJ[i][0]) + ',' + (cy + ADJ[i][1]);
            if (!cellSet.has(nk)) {
                dist.set(k, 1);
                queue.push([cx, cy, 1]);
                break;
            }
        }
    }
    // BFS outward.  Use a head pointer instead of shift() so we stay
    // O(N) rather than O(N^2).
    let head = 0;
    while (head < queue.length) {
        const [cx, cy, d] = queue[head++];
        const nextD = d + 1;
        for (let i = 0; i < 4; i++) {
            const nx = cx + ADJ[i][0];
            const ny = cy + ADJ[i][1];
            const nk = nx + ',' + ny;
            if (!cellSet.has(nk)) continue;
            if (dist.has(nk)) continue;
            dist.set(nk, nextD);
            queue.push([nx, ny, nextD]);
        }
    }
    return dist;
}

// Centerline-routing tunables.  PENALTY_RADIUS sets how many cells away
// from a wall stop incurring penalty (beyond this, full center-of-room
// flat).  PENALTY_PER_STEP scales the gradient.
//
// At cellRes=0.5m, PENALTY_RADIUS=4 means cells within ~2m of a wall
// get progressively penalized.  Penalty per step is 0.6, so the cell
// directly against a wall (dist=1) costs +0.6*3=1.8 extra; +1.2 for
// dist=2; +0.6 for dist=3; 0 for dist>=4.  These add to the base
// movement cost (1.0 cardinal / 1.4142 diagonal).
const PATH_PENALTY_RADIUS   = 4;
const PATH_PENALTY_PER_STEP = 0.6;

function edgePenalty(cx, cy) {
    const d = (S.wallDist && S.wallDist.get(cx + ',' + cy)) || PATH_PENALTY_RADIUS;
    if (d >= PATH_PENALTY_RADIUS) return 0;
    return (PATH_PENALTY_RADIUS - d) * PATH_PENALTY_PER_STEP;
}

function aStar(start, goal) {
    if (!S.cellSet) return null;
    if (start.cx === goal.cx && start.cy === goal.cy) return [start];
    const open = new Map();
    const came = new Map();
    const g    = new Map();
    const sk   = start.cx + ',' + start.cy;
    g.set(sk, 0);
    open.set(sk, h(start, goal));
    const NEIGHBORS = [
        [1,0,1],[-1,0,1],[0,1,1],[0,-1,1],
        [1,1,1.4142],[1,-1,1.4142],[-1,1,1.4142],[-1,-1,1.4142],
    ];
    let iter = 0;
    while (open.size) {
        iter++; if (iter > 60000) return null;
        let bestKey = null, bestF = Infinity;
        for (const [k, f] of open) if (f < bestF) { bestF = f; bestKey = k; }
        if (!bestKey) return null;
        const [cx, cy] = bestKey.split(',').map(Number);
        if (cx === goal.cx && cy === goal.cy) {
            const out = [];
            let key = bestKey;
            while (key) {
                const [x, y] = key.split(',').map(Number);
                out.push({ cx: x, cy: y });
                key = came.get(key);
            }
            out.reverse();
            return out;
        }
        open.delete(bestKey);
        const gc = g.get(bestKey);
        for (const [dx, dy, cost] of NEIGHBORS) {
            const nx = cx + dx, ny = cy + dy;
            const nk = nx + ',' + ny;
            if (!S.cellSet.has(nk)) continue;
            // Centerline: each step's true cost = base movement +
            // proximity-to-wall penalty.  Heuristic h() stays
            // unchanged (Manhattan-ish, admissible) so A* still
            // terminates cleanly -- the penalty is non-negative
            // and only inflates g, not h.
            const tentative = gc + cost + edgePenalty(nx, ny);
            if (tentative < (g.get(nk) ?? Infinity)) {
                came.set(nk, bestKey);
                g.set(nk, tentative);
                open.set(nk, tentative + h({cx: nx, cy: ny}, goal));
            }
        }
    }
    return null;
}
function h(a, b) {
    const dx = Math.abs(a.cx - b.cx), dy = Math.abs(a.cy - b.cy);
    return (dx + dy) + (1.4142 - 2) * Math.min(dx, dy);
}

// ---- Admin modal ---------------------------------------------------------
D.adminBtn.addEventListener('click', openAdmin);
D.adminClose.addEventListener('click', () => { D.adminModal.hidden = true; });
D.adminModal.addEventListener('click', e => {
    if (e.target === D.adminModal) D.adminModal.hidden = true;
});

function openAdmin() {
    D.adminModal.hidden = false;
    if (S.adminKey) {
        D.adminAuth.hidden = true;
        D.adminPanel.hidden = false;
        loadKeyTable();
        loadResetZoneList();
        loadQuarantineList();
    } else {
        D.adminAuth.hidden = false;
        D.adminPanel.hidden = true;
    }
}

D.adminKeySave.addEventListener('click', async () => {
    const k = D.adminKeyInput.value.trim();
    if (k.length < 16) return;
    S.adminKey = k;
    const r = await adminFetch('/admin/keys');
    if (r.status === 401 || r.status === 403) {
        alert('Bad admin key.');
        return;
    }
    localStorage.setItem('warmap_admin_key', k);
    D.adminAuth.hidden = true;
    D.adminPanel.hidden = false;
    loadKeyTable();
    loadResetZoneList();
    loadQuarantineList();
});

async function loadKeyTable() {
    try {
        const r = await adminFetch('/admin/keys');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        const rows = (d.keys || []).map(k => {
            const lastSeen = k.last_used ? prettyAgo(k.last_used) : 'never';
            const cls = k.enabled ? '' : 'disabled';
            const tier = k.tier || 'uploader';
            return `<tr class="${cls}">
                <td>${esc(k.name)}${k.note ? ` <span class="muted">(${esc(k.note)})</span>` : ''}</td>
                <td><span class="tier-pill tier-${esc(tier)}">${esc(tier)}</span></td>
                <td>${k.uploads}</td>
                <td>${esc(lastSeen)}</td>
                <td>${k.enabled ? 'enabled' : '<span style="color:#cf3434">disabled</span>'}</td>
                <td>
                    <button data-act="${k.enabled?'disable':'enable'}" data-name="${esc(k.name)}">${k.enabled?'disable':'enable'}</button>
                    <button data-act="delete" data-name="${esc(k.name)}">delete</button>
                    ${tier === 'uploader' ? `<button data-act="quarantine" data-name="${esc(k.name)}">quarantine</button>` : ''}
                </td>
            </tr>`;
        }).join('');
        D.keyTableBody.innerHTML = rows || '<tr><td colspan="6" class="muted">No keys yet. Mint one above.</td></tr>';
        D.keyTableBody.querySelectorAll('button').forEach(b => {
            b.addEventListener('click', () => keyAction(b.dataset.act, b.dataset.name));
        });
    } catch (e) {
        D.keyTableBody.innerHTML = `<tr><td colspan="6">Error: ${esc(e.message)}</td></tr>`;
    }
}

async function keyAction(act, name) {
    let url, method = 'POST';
    if (act === 'disable')        url = `/admin/keys/${encodeURIComponent(name)}/disable`;
    else if (act === 'enable')    url = `/admin/keys/${encodeURIComponent(name)}/enable`;
    else if (act === 'delete') {
        if (!confirm(`Delete key for ${name}?\n\nThis also removes ALL their dumps + sessions and triggers a re-merge so they disappear from Uploaders + zones.\n\nUse "disable" instead if you just want to revoke their key while keeping their historical contributions.`)) return;
        url = `/admin/keys/${encodeURIComponent(name)}`; method = 'DELETE';
    } else if (act === 'quarantine') {
        if (!confirm(`Move ALL of ${name}'s uploads to quarantine and disable their key?`)) return;
        url = `/admin/quarantine_uploader/${encodeURIComponent(name)}`;
    }
    const r = await adminFetch(url, { method });
    if (!r.ok) { alert('Failed: HTTP ' + r.status); return; }
    await loadKeyTable();
    await refreshUploaders();
}

// ---- Zone cleanup -------------------------------------------------------
//
// Populate the dropdown from the public /zones list (same one the sidebar
// uses).  We re-fetch on every admin open so newly-created zones show up
// without a viewer reload.
async function loadResetZoneList() {
    try {
        // Use getJSON so the X-WarMap-Key header is attached -- /zones is
        // gated, so a bare fetch() here would 401 even when the rest of
        // the viewer is authenticated.
        const d = await getJSON('/zones');
        const zones = (d.zones || []).slice().sort();
        D.resetZoneSelect.innerHTML =
            '<option value="">-- pick a zone --</option>' +
            zones.map(z => `<option value="${esc(z)}">${esc(z)}</option>`).join('');
    } catch (e) {
        D.resetZoneSelect.innerHTML = `<option value="">(error: ${esc(e.message)})</option>`;
    }
}

D.resetZoneGo.addEventListener('click', async () => {
    const key = D.resetZoneSelect.value;
    if (!key) {
        D.resetResult.textContent = 'pick a zone first';
        return;
    }
    const ok = confirm(
        `Reset zone "${key}"?\n\n` +
        `This will:\n` +
        `  - quarantine every dump that contributed to this zone\n` +
        `  - delete the merged JSON\n` +
        `  - trigger a re-merge\n\n` +
        `The zone disappears until someone records new sessions for it.\n` +
        `Quarantined dumps stay on disk in quarantine/ for audit.\n\n` +
        `Continue?`
    );
    if (!ok) return;
    D.resetResult.textContent = 'resetting...';
    const r = await adminFetch('/admin/zone_reset/' + encodeURIComponent(key), { method: 'POST' });
    if (!r.ok) {
        D.resetResult.textContent = `error: HTTP ${r.status}`;
        return;
    }
    const d = await r.json();
    D.resetResult.innerHTML =
        `<div><b>Reset ${esc(key)}</b></div>` +
        `<div>quarantined ${d.count} dump(s)` +
            (d.merged_json_deleted ? ', deleted merged JSON' : ', no merged JSON existed') +
            (d.scan_errors ? `, ${d.scan_errors} dump(s) unreadable` : '') +
        `</div>` +
        `<div class="muted">Re-merge running in background.  Refresh in a few seconds.</div>`;
    // Refresh views that may have changed
    await loadResetZoneList();
    await loadQuarantineList();
    // Nudge the sidebar so the user sees the deleted zone disappear without
    // waiting for the next auto-refresh tick.
    try { await refreshZoneList(); } catch (_) { /* ignore */ }
});

// ---- Quarantine list ----------------------------------------------------
async function loadQuarantineList() {
    try {
        const r = await adminFetch('/admin/quarantine');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        const items = d.items || [];
        D.quarantineCount.textContent = items.length ? `(${items.length})` : '(empty)';
        if (!items.length) {
            D.quarantineList.textContent = 'Nothing in quarantine.';
            return;
        }
        // Each entry is either a string filename or {name, size, mtime}.
        // Group by uploader prefix (everything before the first __).
        const grouped = {};
        for (const it of items) {
            const name = typeof it === 'string' ? it : it.name;
            const m = name && name.match(/^([^_]+)__/);
            const uploader = m ? m[1] : '(unknown)';
            (grouped[uploader] = grouped[uploader] || []).push(it);
        }
        const html = Object.entries(grouped)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([up, list]) => `
                <details class="quarantine-group">
                    <summary>${esc(up)} <span class="muted">(${list.length})</span></summary>
                    <ul class="quarantine-files">${
                        list.map(it => {
                            const name = typeof it === 'string' ? it : it.name;
                            const size = (typeof it === 'object' && it.size != null)
                                ? ` <span class="muted">${prettyBytes(it.size)}</span>` : '';
                            return `<li><code>${esc(name)}</code>${size}</li>`;
                        }).join('')
                    }</ul>
                </details>`).join('');
        D.quarantineList.innerHTML = html;
    } catch (e) {
        D.quarantineList.textContent = 'error: ' + e.message;
    }
}

function prettyBytes(n) {
    if (n < 1024)        return n + ' B';
    if (n < 1024*1024)   return (n/1024).toFixed(1) + ' KB';
    return (n/(1024*1024)).toFixed(1) + ' MB';
}

D.mintGo.addEventListener('click', async () => {
    const name = D.mintName.value.trim();
    const tier = (D.mintTier?.value || 'uploader').trim();
    const note = D.mintNote.value.trim();
    if (!name) { D.mintResult.textContent = 'name required'; return; }
    const r = await adminFetch('/admin/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, tier, note }),
    });
    if (!r.ok) { D.mintResult.textContent = `error ${r.status}`; return; }
    const k = await r.json();
    // Different "what to send" copy depending on tier -- uploaders run
    // install.bat, readers paste the key into the viewer's sign-in card.
    const handoff = (k.tier === 'reader')
        ? `Send this to ${esc(k.name)}. They paste it into the viewer's "Enter API Key" prompt.`
        : `Send this to ${esc(k.name)}. They paste it during install.bat (or use update-warmap-server.bat).`;
    D.mintResult.innerHTML =
        `<div><b>${esc(k.name)}</b>'s key <span class="muted">(${esc(k.tier || 'uploader')})</span>:</div>` +
        `<div>${esc(k.key)} <button class="copy-btn" id="mint-copy">copy</button></div>` +
        `<div class="muted" style="margin-top:0.4rem">${handoff}</div>`;
    document.getElementById('mint-copy').addEventListener('click', () => {
        navigator.clipboard.writeText(k.key);
    });
    D.mintName.value = ''; D.mintNote.value = '';
    if (D.mintTier) D.mintTier.value = 'uploader';     // reset to default
    await loadKeyTable();
});

// ---- Util ----------------------------------------------------------------
function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function prettyAgo(ts) {
    const dt = Date.now()/1000 - ts;
    if (dt < 60)   return `${dt.toFixed(0)}s ago`;
    if (dt < 3600) return `${(dt/60).toFixed(0)}m ago`;
    return `${(dt/3600).toFixed(1)}h ago`;
}

async function pollLoop() {
    while (true) {
        if (D.autoRefresh.checked) await refreshStatus();
        await new Promise(r => setTimeout(r, STATUS_POLL_MS));
    }
}

// Top-level safety net: any uncaught error during init or runtime
// surfaces a visible banner instead of a silent dead viewer.  Without
// this, a regression in render() / refreshStatus() / etc. just leaves
// the page blank with --/--/-- in the header stats.
function showFatalBanner(msg) {
    let b = document.getElementById('fatal-banner');
    if (!b) {
        b = document.createElement('div');
        b.id = 'fatal-banner';
        b.style.cssText = `
            position:fixed; top:0; left:0; right:0; z-index:9999;
            background:#3a0d11; color:#ffb4bc; padding:0.6rem 1rem;
            font:0.85rem/1.4 -apple-system,Segoe UI,sans-serif;
            border-bottom:1px solid #6b1c25; box-shadow:0 4px 12px rgba(0,0,0,0.5);
        `;
        document.body && document.body.appendChild(b);
    }
    b.innerHTML = `<strong>Viewer error.</strong> ${msg} &mdash;
        <a href="javascript:location.reload()" style="color:inherit;text-decoration:underline">reload</a> /
        <a href="javascript:localStorage.clear();location.reload()" style="color:inherit;text-decoration:underline">clear cache + reload</a>`;
}
window.addEventListener('error',  (e) => showFatalBanner('JS error: ' + (e.message || 'unknown')));
window.addEventListener('unhandledrejection', (e) => showFatalBanner('promise rejection: ' + (e.reason?.message || e.reason || 'unknown')));

// ---- Zone JSON download --------------------------------------------------
// Fetches the merged zone JSON via the gated /zones/{key} endpoint and
// triggers a browser download.  Available to every authenticated user
// (admin, uploader, reader) -- the whole point of the reader tier is
// that a friend can run this and walk away with the catalog.
document.getElementById('zone-download')?.addEventListener('click', async () => {
    if (!S.currentKey) return;
    const btn = document.getElementById('zone-download');
    const originalLabel = btn?.textContent;
    if (btn) { btn.disabled = true; btn.textContent = 'Downloading…'; }
    try {
        const r = await fetch('/zones/' + encodeURIComponent(S.currentKey), {
            cache:   'no-store',
            headers: authHeaders(),
        });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        // Build a blob URL and click an invisible <a download> -- standard
        // browser-side download trick.  Server already sends the right
        // Content-Type; we just want it saved to disk.
        const blob = await r.blob();
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url;
        a.download = S.currentKey + '.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (e) {
        alert('Download failed: ' + (e?.message || e));
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = originalLabel || 'Download'; }
    }
});

(async function init() {
    try {
        // Sign-in gate: if no key in localStorage, every gated endpoint
        // returns 401.  Show the overlay and let attemptSignin() resume
        // the boot sequence once the user pastes a key.
        if (!S.adminKey) {
            showSigninOverlay();
        } else {
            // Validate the saved key + grab the tier so we can gate UI
            // before any data renders.  /whoami is cheap (just an auth
            // check + tier lookup); on failure we still let init finish
            // so pollLoop is up and the user can sign in again via the
            // overlay without a page reload.
            let whoamiOk = false;
            try {
                const r = await fetch('/whoami', {
                    cache: 'no-store',
                    headers: { 'X-WarMap-Key': S.adminKey },
                });
                if (r.status === 401) {
                    showSigninOverlay('saved key was rejected');
                } else if (!r.ok) {
                    showFatalBanner('whoami HTTP ' + r.status);
                } else {
                    const w = await r.json();
                    S.tier     = w.tier || 'reader';
                    S.userName = w.name || '';
                    applyTierGating();
                    whoamiOk = true;
                }
            } catch (e) {
                showFatalBanner('whoami failed: ' + (e?.message || e));
            }
            if (whoamiOk) try {
                await refreshStatus();
                await refreshZoneList();
                if (S.tier === 'admin') await refreshUploaders();
                refreshServerIgnoreList();   // populate hidden-skins filter
                refreshActorLabels();        // populate per-actor label overrides
            } catch (e) {
                // 401 -> attemptSignin path already showed the overlay
                // via refreshStatus's catch.  For any other error,
                // surface it so the user knows something's wrong.
                if (!/HTTP 401/.test(String(e?.message || e))) {
                    showFatalBanner('initial fetch failed: ' + (e?.message || e));
                }
            }
        }
        pollLoop();
        requestAnimationFrame(() => {
            const r = D.canvas.parentElement.getBoundingClientRect();
            D.canvas.width  = Math.max(800, r.width);
            D.canvas.height = Math.max(600, r.height);
            if (S.currentData) render();
            try { updateZoomReadout(); } catch (e) {}
        });
        try { renderLayerPanel(); } catch (e) { /* tab not opened yet, ok */ }
    } catch (e) {
        showFatalBanner('init crashed: ' + (e?.message || e) + ' (line ' + (e?.lineNumber || '?') + ')');
    }
})();
