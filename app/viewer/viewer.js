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
    uploaders:     [],
    activeUploader:null,
    uploaderTracks:{},          // dump_name -> { zone, samples, complete, activity }
    pathMode:      null,        // null | 'pickA' | 'pickB'
    pathA:         null,        // {wx, wy} world coords
    pathB:         null,
    pathPath:      null,        // [{cx, cy}]
    cellSet:       null,        // Set of "cx,cy" walkable cells (for the active floor)
    cellRes:       0.5,
    adminKey:      localStorage.getItem('warmap_admin_key') || '',
    // Orientation: world-axis -> canvas-axis transform.  D4's coord system
    // doesn't map cleanly to "north up", so we let the user dial it in.
    // rot:   90deg increments clockwise (0|1|2|3)
    // flipX: mirror cell X axis
    // flipY: mirror cell Y axis
    orient: JSON.parse(localStorage.getItem('warmap_orient') || '{}') || {},
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
};
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
};

function actorDisplayName(a) {
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
async function getJSON(p, opts) {
    const r = await fetch(p, Object.assign({ cache: 'no-store' }, opts || {}));
    if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`);
    return r.json();
}
async function getText(p) {
    const r = await fetch(p, { cache: 'no-store' });
    if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`);
    return r.text();
}
function adminFetch(path, init) {
    init = init || {};
    init.headers = Object.assign({}, init.headers, { 'X-WarMap-Key': S.adminKey });
    init.cache = 'no-store';
    return fetch(path, init);
}

// ---- Status + zone list --------------------------------------------------
async function refreshStatus() {
    try {
        const s = await getJSON('/status');
        const m = s.last_merge || {};
        const fin = m.finished_at;
        D.state.textContent = `${s.dumps_count} dumps · ${s.zones_count} zones · last merge ${fin ? prettyAgo(fin) : 'never'}`;
        if (fin && fin !== S.lastMergeT) {
            S.lastMergeT = fin;
            await refreshZoneList();
            await refreshUploaders();
            if (S.currentKey) await loadZone(S.currentKey, false);
        }
    } catch (e) {
        D.state.textContent = `disconnected: ${e.message}`;
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
    document.querySelectorAll('.zone-list li').forEach(li => {
        li.classList.toggle('active', li.dataset.key === key);
    });
    D.empty.hidden = true;
    D.zoneView.hidden = false;
    try {
        const d = await getJSON('/zones/' + encodeURIComponent(key));
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
    } catch (e) {
        D.zoneTitle.textContent = key;
        D.zoneMeta.textContent = `failed: ${e.message}`;
    }
}

function rebuildCellSet() {
    if (!S.currentData) { S.cellSet = null; return; }
    const cells = S.currentData.grid?.floors?.[S.currentFloor] || [];
    const set = new Set();
    for (const c of cells) {
        if (c[2]) set.add(c[0] + ',' + c[1]);
    }
    S.cellSet = set;
    S.cellRes = S.currentData.grid?.resolution || 0.5;
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

    // Actors
    const actors = (S.currentData.actors || []).filter(a => a.floor == null || String(a.floor) === S.currentFloor);
    const cellRes = S.cellRes;
    const showLabels = scale >= 5;
    const hits = [];
    for (const a of actors) {
        if (typeof a.x !== 'number') continue;
        if (a.x === 0 && a.y === 0) continue;
        const t = applyOrient(a.x / cellRes, a.y / cellRes, rawBbox);
        const x = offX + t.x*scale, y = (h - offY) - t.y*scale;
        const style = ACTOR_STYLE[a.kind] || { c:'#999', sym:'?' };
        const isSel = (a === S.selectedActor), isHov = (a === S.hoveredActor);
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
    if (S.activeUploader && S.uploaderTracks) {
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
    const f = e.deltaY > 0 ? 0.9 : 1.1;
    S.view.scale = Math.max(0.1, Math.min(80, S.view.scale * f));
    render();
}, { passive: false });

D.canvas.addEventListener('mouseleave', () => { D.tooltip.hidden = true; S.hoveredActor = null; render(); });

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
    if (a.is_boss)  row('flag', 'BOSS');
    if (a.is_elite) row('flag', 'ELITE');
    D.actorBody.innerHTML = rows.join('');
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
    });
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
            const nk = (cx+dx) + ',' + (cy+dy);
            if (!S.cellSet.has(nk)) continue;
            const tentative = gc + cost;
            if (tentative < (g.get(nk) ?? Infinity)) {
                came.set(nk, bestKey);
                g.set(nk, tentative);
                const node = { cx: cx+dx, cy: cy+dy };
                open.set(nk, tentative + h(node, goal));
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
            return `<tr class="${cls}">
                <td>${esc(k.name)}${k.note ? ` <span class="muted">(${esc(k.note)})</span>` : ''}</td>
                <td>${k.uploads}</td>
                <td>${esc(lastSeen)}</td>
                <td>${k.enabled ? 'enabled' : '<span style="color:#cf3434">disabled</span>'}</td>
                <td>
                    <button data-act="${k.enabled?'disable':'enable'}" data-name="${esc(k.name)}">${k.enabled?'disable':'enable'}</button>
                    <button data-act="delete" data-name="${esc(k.name)}">delete</button>
                    <button data-act="quarantine" data-name="${esc(k.name)}">quarantine</button>
                </td>
            </tr>`;
        }).join('');
        D.keyTableBody.innerHTML = rows || '<tr><td colspan="5" class="muted">No keys yet. Mint one above.</td></tr>';
        D.keyTableBody.querySelectorAll('button').forEach(b => {
            b.addEventListener('click', () => keyAction(b.dataset.act, b.dataset.name));
        });
    } catch (e) {
        D.keyTableBody.innerHTML = `<tr><td colspan="5">Error: ${esc(e.message)}</td></tr>`;
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
        const r = await fetch('/zones');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
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
    const note = D.mintNote.value.trim();
    if (!name) { D.mintResult.textContent = 'name required'; return; }
    const r = await adminFetch('/admin/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, note }),
    });
    if (!r.ok) { D.mintResult.textContent = `error ${r.status}`; return; }
    const k = await r.json();
    D.mintResult.innerHTML =
        `<div><b>${esc(k.name)}</b>'s key:</div>` +
        `<div>${esc(k.key)} <button class="copy-btn" id="mint-copy">copy</button></div>` +
        `<div class="muted" style="margin-top:0.4rem">Send this string to ${esc(k.name)}. They paste it in install.bat.</div>`;
    document.getElementById('mint-copy').addEventListener('click', () => {
        navigator.clipboard.writeText(k.key);
    });
    D.mintName.value = ''; D.mintNote.value = '';
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

(async function init() {
    await refreshStatus();
    await refreshZoneList();
    await refreshUploaders();
    pollLoop();
    requestAnimationFrame(() => {
        const r = D.canvas.parentElement.getBoundingClientRect();
        D.canvas.width  = Math.max(800, r.width);
        D.canvas.height = Math.max(600, r.height);
        if (S.currentData) render();
    });
})();
