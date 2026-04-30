// ---------------------------------------------------------------------------
// WarMap live viewer.
//
// Polls /status every 5s; when last_merge.finished_at advances, the sidebar
// re-fetches /zones and the currently-open zone re-fetches its data so the
// canvas redraws with fresh merged data.
//
// Renders:
//   - walkable/blocked grid (color by walkability, intensity by confidence)
//   - actors as colored diamonds with kind glyphs + smart display names
//   - click-to-select an actor for full metadata (skin, ids, x/y/z, ...)
//   - hover tooltip with quick info
// ---------------------------------------------------------------------------

const STATUS_POLL_MS = 5000;

const elState        = document.getElementById('status-text');
const elZoneList     = document.getElementById('zone-list');
const elZoneCount    = document.getElementById('zone-count');
const elEmpty        = document.getElementById('empty-state');
const elZoneView     = document.getElementById('zone-view');
const elZoneTitle    = document.getElementById('zone-title');
const elZoneMeta     = document.getElementById('zone-meta');
const elCanvas       = document.getElementById('zone-canvas');
const elTooltip      = document.getElementById('hover-tooltip');
const elFloorCtl     = document.getElementById('floor-controls');
const elFloorSelect  = document.getElementById('floor-select');
const elAutoRefresh  = document.getElementById('auto-refresh');
const elActorPanel   = document.getElementById('actor-panel');
const elActorBody    = document.getElementById('actor-panel-body');
const elActorClose   = document.getElementById('actor-panel-close');
const ctx = elCanvas.getContext('2d');

let lastMergeT = null;
let currentKey = null;
let currentData = null;
let currentFloor = null;
let view = { panX: 0, panY: 0, scale: 1.0 };
let dragState = null;
let selectedActor = null;
let hoveredActor  = null;
let zoneListData = [];

// ---- Actor styling --------------------------------------------------------
// Color + single-letter glyph + short label per actor kind.  Mirrors the
// local viewer's vocabulary so labels read consistently.
const ACTOR_STYLE = {
    chest_helltide_random:   { c: '#ffcc00', sym: 'C',  label: 'Chest' },
    chest_helltide_silent:   { c: '#aaa0ff', sym: 'C',  label: 'Silent' },
    chest_helltide_targeted: { c: '#ff8800', sym: 'C',  label: 'Tortured' },
    chest:                   { c: '#ffaa44', sym: 'C',  label: 'Chest' },
    portal:                  { c: '#cc88ff', sym: 'P',  label: 'Portal' },
    portal_town:             { c: '#79c0ff', sym: 'T',  label: 'TownPortal' },
    portal_helltide:         { c: '#ff5544', sym: 'H',  label: 'HellPortal' },
    dungeon_entrance:        { c: '#ff66cc', sym: 'D',  label: 'Dungeon' },
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
    pit_obelisk:       'Pit Obelisk',
    undercity_obelisk: 'Undercity Obelisk',
    warplans_vendor:   'War Plans Vendor',
    tyrael:            'Tyrael',
    horde_gate:        'Horde Gate',
    glyph_gizmo:       'Glyph Gizmo',
    bounty_npc:        'Raven',
    stash:             'Stash',
    waypoint:          'Waypoint',
    pyre:              'Pyre',
    traversal:         'Traversal',
    portal:            'Portal',
    portal_town:       'Town Portal',
    portal_helltide:   'Helltide Portal',
    dungeon_entrance:  'Dungeon Entrance',
    objective:         'Objective',
    enticement:        'Beacon',
};

// Derive a short readable name from an actor's skin string.
function actorDisplayName(actor) {
    if (KIND_OVERRIDES[actor.kind]) return KIND_OVERRIDES[actor.kind];
    const skin = actor.skin || '';

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
        .replace(/^Merc_Hideout_NPC_/,    'Mercenary: ')
        .replace(/^NPC_QST_X2_/,          '')
        .replace(/^S07_Bounty_Meta_/,     'Bounty: ')
        .replace(/^(Crafter|Vendor|Service|Stable)_/, '');

    if (!s.includes(' ')) s = s.replace(/([a-z])([A-Z])/g, '$1 $2');

    if (s && s.length < 40) return s;
    return (ACTOR_STYLE[actor.kind] && ACTOR_STYLE[actor.kind].label) || actor.kind || '?';
}

// ---- Fetch helpers --------------------------------------------------------
async function fetchJSON(path) {
    const r = await fetch(path, { cache: 'no-store' });
    if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
    return r.json();
}

async function refreshStatus() {
    try {
        const s = await fetchJSON('/status');
        const merge = s.last_merge || {};
        const finished = merge.finished_at;
        const ago = finished ? prettyAgo(finished) : 'never';
        elState.textContent =
            `${s.dumps_count} dumps · ${s.zones_count} zones · last merge ${ago}`;
        if (finished && finished !== lastMergeT) {
            lastMergeT = finished;
            await refreshZoneList();
            if (currentKey) await loadZone(currentKey, /*resetView=*/false);
        }
    } catch (e) {
        elState.textContent = `disconnected: ${e.message}`;
    }
}

async function refreshZoneList() {
    try {
        const z = await fetchJSON('/zones');
        zoneListData = (z.zones || []).filter(k => !k.startsWith('_') && k !== 'coverage');
        elZoneCount.textContent = `(${zoneListData.length})`;
        renderZoneList();
    } catch (e) {
        elState.textContent = `zone list failed: ${e.message}`;
    }
}

function renderZoneList() {
    elZoneList.innerHTML = '';
    for (const key of zoneListData) {
        const li = document.createElement('li');
        li.dataset.key = key;
        if (key === currentKey) li.classList.add('active');
        const name = document.createElement('div');
        name.className = 'zone-name';
        name.textContent = key;
        li.appendChild(name);
        li.addEventListener('click', () => loadZone(key, /*resetView=*/true));
        elZoneList.appendChild(li);
    }
}

async function loadZone(key, resetView) {
    currentKey = key;
    document.querySelectorAll('.zone-list li').forEach(li => {
        li.classList.toggle('active', li.dataset.key === key);
    });
    elEmpty.hidden = true;
    elZoneView.hidden = false;
    try {
        const d = await fetchJSON('/zones/' + encodeURIComponent(key));
        currentData = d;
        elZoneTitle.textContent = d.key;

        const floors = Object.keys(d.grid?.floors || {});
        if (floors.length > 1) {
            elFloorCtl.hidden = false;
            elFloorSelect.innerHTML = '';
            for (const f of floors) {
                const opt = document.createElement('option');
                opt.value = f; opt.textContent = `floor ${f}`;
                elFloorSelect.appendChild(opt);
            }
            if (!floors.includes(currentFloor)) currentFloor = floors[0];
            elFloorSelect.value = currentFloor;
        } else {
            elFloorCtl.hidden = true;
            currentFloor = floors[0] || '1';
        }

        if (resetView) {
            view = { panX: 0, panY: 0, scale: 1.0 };
            selectedActor = null;
            renderActorPanel();
        }
        renderMeta();
        render();
    } catch (e) {
        elZoneTitle.textContent = key;
        elZoneMeta.textContent = `failed: ${e.message}`;
    }
}

function renderMeta() {
    if (!currentData) return;
    const cells = (currentData.grid?.floors?.[currentFloor] || []).length;
    const actors = (currentData.actors || []).filter(a =>
        a.floor == null || String(a.floor) === currentFloor).length;
    const sat = currentData.saturated ? ' · saturated' : '';
    const sessions = currentData.sessions_merged || 0;
    const merged = currentData.merged_at ? prettyAgo(currentData.merged_at) : 'never';
    elZoneMeta.innerHTML =
        `${cells.toLocaleString()} cells · ${actors} actors · ` +
        `${sessions} session${sessions === 1 ? '' : 's'}${sat} · ` +
        `<span class="muted">merged ${merged}</span>`;
}

elFloorSelect.addEventListener('change', e => {
    currentFloor = e.target.value;
    selectedActor = null;
    renderActorPanel();
    renderMeta();
    render();
});

// ---- Canvas rendering -----------------------------------------------------
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
    if (!currentData || !currentFloor) return;
    const cells = currentData.grid?.floors?.[currentFloor] || [];
    const w = elCanvas.width, h = elCanvas.height;
    ctx.fillStyle = '#06090d';
    ctx.fillRect(0, 0, w, h);

    if (cells.length === 0) {
        ctx.fillStyle = '#8b949e';
        ctx.font = '14px sans-serif';
        ctx.fillText('(no cells on this floor)', 20, 24);
        return;
    }

    const bbox = bboxOfCells(cells);
    const padding = 20;
    const dx = bbox.maxx - bbox.minx + 1;
    const dy = bbox.maxy - bbox.miny + 1;
    const fitScale = Math.min((w - 2*padding)/dx, (h - 2*padding)/dy);
    const scale = fitScale * view.scale;
    const offX = (w - dx*scale)/2 + view.panX - bbox.minx*scale;
    const offY = (h - dy*scale)/2 + view.panY - bbox.miny*scale;
    drawState = { bbox, scale, offX, offY, fitScale };

    // Cells
    const cellSize = Math.max(1, scale);
    for (const c of cells) {
        const cx = c[0], cy = c[1], walk = c[2], conf = c[3];
        const x = offX + cx*scale;
        const y = (h - offY) - cy*scale - cellSize;
        if (walk) {
            ctx.fillStyle = `rgba(63, 185, 80, ${0.35 + 0.65*conf})`;
        } else {
            ctx.fillStyle = `rgba(207, 52, 52, ${0.35 + 0.65*conf})`;
        }
        ctx.fillRect(x, y, cellSize, cellSize);
    }

    // Actors
    const actors = (currentData.actors || []).filter(a =>
        a.floor == null || String(a.floor) === currentFloor);
    const cellRes = currentData.grid?.resolution || 0.5;
    const showLabels = scale >= 5;
    const hits = [];
    for (const a of actors) {
        if (typeof a.x !== 'number') continue;
        if (a.x === 0 && a.y === 0) continue;
        const ax = a.x / cellRes;
        const ay = a.y / cellRes;
        const x = offX + ax*scale;
        const y = (h - offY) - ay*scale;
        const style = ACTOR_STYLE[a.kind] || { c: '#999999', sym: '?', label: a.kind || '?' };
        const isSelected = (a === selectedActor);
        const isHovered  = (a === hoveredActor);

        // Selection / hover ring
        if (isSelected || isHovered) {
            ctx.beginPath();
            ctx.arc(x, y, isSelected ? 13 : 10, 0, Math.PI * 2);
            ctx.strokeStyle = isSelected ? '#ffd700' : '#ffffff';
            ctx.lineWidth = isSelected ? 2 : 1.5;
            ctx.stroke();
        }

        // Filled diamond (rotated square)
        ctx.save();
        ctx.translate(x, y);
        ctx.rotate(Math.PI / 4);
        ctx.fillStyle = style.c;
        ctx.strokeStyle = '#000';
        ctx.lineWidth = 1;
        const sz = isSelected ? 7 : 5;
        ctx.fillRect(-sz, -sz, sz * 2, sz * 2);
        ctx.strokeRect(-sz, -sz, sz * 2, sz * 2);
        ctx.restore();

        // Glyph in center (only when zoomed enough that it's readable)
        if (scale >= 3) {
            ctx.fillStyle = '#000';
            ctx.font = 'bold 9px Consolas, monospace';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(style.sym, x, y);
            ctx.textAlign = 'start';
        }

        // Label trailing the marker, if zoomed in
        if (showLabels || isSelected) {
            ctx.fillStyle = style.c;
            ctx.font = (isSelected ? '11px ' : '10px ') + 'Consolas, monospace';
            ctx.textBaseline = 'middle';
            ctx.fillText(actorDisplayName(a), x + 11, y - 1);
        }

        hits.push({ x, y, r: 10, actor: a });
    }
    drawState.hits = hits;
}

// ---- Pan + zoom + click + hover ------------------------------------------
function canvasCoordsFromEvent(e) {
    const rect = elCanvas.getBoundingClientRect();
    const sx = elCanvas.width / rect.width;
    const sy = elCanvas.height / rect.height;
    return { x: (e.clientX - rect.left) * sx, y: (e.clientY - rect.top) * sy };
}

function pickActor(cx, cy) {
    if (!drawState || !drawState.hits) return null;
    let best = null, bestD2 = Infinity;
    for (const h of drawState.hits) {
        const d2 = (h.x - cx)**2 + (h.y - cy)**2;
        if (d2 < bestD2 && d2 <= h.r * h.r * 1.5) { best = h; bestD2 = d2; }
    }
    return best ? best.actor : null;
}

elCanvas.addEventListener('mousedown', e => {
    dragState = { x: e.clientX, y: e.clientY, panX: view.panX, panY: view.panY, moved: false };
});

window.addEventListener('mouseup', e => {
    if (dragState && !dragState.moved) {
        // Click without drag -> select actor (or clear if missed)
        const { x, y } = canvasCoordsFromEvent(e);
        const a = pickActor(x, y);
        selectedActor = a;
        renderActorPanel();
        render();
    }
    dragState = null;
});

window.addEventListener('mousemove', e => {
    if (dragState) {
        const dx = e.clientX - dragState.x;
        const dy = e.clientY - dragState.y;
        if (Math.abs(dx) + Math.abs(dy) > 3) dragState.moved = true;
        const rect = elCanvas.getBoundingClientRect();
        const sx = elCanvas.width / rect.width;
        const sy = elCanvas.height / rect.height;
        view.panX = dragState.panX + dx * sx;
        view.panY = dragState.panY - dy * sy;
        render();
    } else {
        const { x, y } = canvasCoordsFromEvent(e);
        const hit = pickActor(x, y);
        if (hit !== hoveredActor) {
            hoveredActor = hit;
            render();
        }
        if (hit) {
            const name = actorDisplayName(hit);
            elTooltip.innerHTML =
                `<div class="h-name">${escapeHTML(name)}</div>` +
                `<div class="h-skin">${escapeHTML(hit.skin || '?')}</div>` +
                `<div class="h-meta">${escapeHTML(hit.kind || '?')} · ` +
                `(${hit.x?.toFixed?.(1)}, ${hit.y?.toFixed?.(1)}, ${hit.z?.toFixed?.(1)})</div>`;
            const rect = elCanvas.getBoundingClientRect();
            elTooltip.style.left = (e.clientX - rect.left + 12) + 'px';
            elTooltip.style.top  = (e.clientY - rect.top + 12) + 'px';
            elTooltip.hidden = false;
        } else {
            elTooltip.hidden = true;
        }
    }
});

elCanvas.addEventListener('wheel', e => {
    e.preventDefault();
    const factor = e.deltaY > 0 ? 0.9 : 1.1;
    view.scale *= factor;
    view.scale = Math.max(0.1, Math.min(80, view.scale));
    render();
}, { passive: false });

elCanvas.addEventListener('mouseleave', () => {
    elTooltip.hidden = true;
    hoveredActor = null;
    render();
});

// ---- Actor info panel -----------------------------------------------------
function renderActorPanel() {
    const a = selectedActor;
    if (!a) { elActorPanel.hidden = true; return; }
    elActorPanel.hidden = false;
    const rows = [];
    const row = (k, v, cls) => {
        if (v === undefined || v === null || v === '') return;
        rows.push(
            `<div class="row${cls ? ' ' + cls : ''}">` +
            `<span class="k">${escapeHTML(k)}</span>` +
            `<span class="v">${escapeHTML(String(v))}</span></div>`);
    };
    row('name',     actorDisplayName(a), 'kind');
    row('kind',     a.kind);
    row('skin',     a.skin, 'skin');
    row('id',       a.id);
    row('type_id',  a.type_id);
    row('sno_id',   a.sno_id);
    row('x',        typeof a.x === 'number' ? a.x.toFixed(2) : a.x);
    row('y',        typeof a.y === 'number' ? a.y.toFixed(2) : a.y);
    row('z',        typeof a.z === 'number' ? a.z.toFixed(2) : a.z);
    row('floor',    a.floor);
    row('radius',   a.radius);
    row('sessions seen',  a.sessions_seen);
    row('observations',   a.total_observations);
    if (a.is_boss)  row('flag', 'BOSS');
    if (a.is_elite) row('flag', 'ELITE');
    elActorBody.innerHTML = rows.join('');
}

elActorClose.addEventListener('click', () => {
    selectedActor = null;
    renderActorPanel();
    render();
});

// ---- Util -----------------------------------------------------------------
function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, c => (
        { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]
    ));
}

function prettyAgo(ts) {
    const dt = Date.now()/1000 - ts;
    if (dt < 60) return `${dt.toFixed(0)}s ago`;
    if (dt < 3600) return `${(dt/60).toFixed(0)}m ago`;
    return `${(dt/3600).toFixed(1)}h ago`;
}

async function pollLoop() {
    while (true) {
        if (elAutoRefresh.checked) await refreshStatus();
        await new Promise(r => setTimeout(r, STATUS_POLL_MS));
    }
}

// ---- Boot -----------------------------------------------------------------
(async function init() {
    await refreshStatus();
    await refreshZoneList();
    pollLoop();
    requestAnimationFrame(() => {
        const rect = elCanvas.parentElement.getBoundingClientRect();
        elCanvas.width  = Math.max(800, rect.width);
        elCanvas.height = Math.max(600, rect.height);
        if (currentData) render();
    });
})();
