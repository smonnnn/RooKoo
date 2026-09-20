// RooKoo — a serverless, peer-to-peer video meeting room.
//
// Built on the NostrP2P library from the Bombus p2p branch: Nostr keypairs
// give every participant an identity, Nostr relays carry WebRTC signaling,
// and chat / presence / file metadata flow over the encrypted data-channel
// mesh with gossip flooding. Audio/video runs over a separate WebRTC mesh
// whose SDP/ICE is tunnelled through those data channels, so no server ever
// sees a frame.

import { NostrP2P } from './nostr-p2p.js';
import { store } from './store.js';
import { generateSecretKey, getPublicKey, nip19, bytesToHex, hexToBytes } from './nostr-deps.js';

const FILE_CHUNK = 48 * 1024;
const SEEN_MAX = 5000;
const RELAYABLE = new Set([
    'hello', 'peers', 'chat', 'chat_history', 'file_meta', 'file_req',
    'media_state', 'media_leave',
]);

// CSS filters applied on a canvas, so the effect is baked into the outgoing
// video and therefore visible to everyone — not just a local preview.
const FILTERS = {
    none: '',
    grayscale: 'grayscale(1)',
    sepia: 'sepia(.85)',
    invert: 'invert(1)',
    warm: 'sepia(.45) saturate(1.7) hue-rotate(-18deg)',
    cool: 'sepia(.45) saturate(1.7) hue-rotate(160deg)',
    vivid: 'saturate(1.7) contrast(1.12)',
    bright: 'brightness(1.4) contrast(1.05)',
    dark: 'brightness(.6) contrast(1.15)',
    blur: 'blur(4px)',
    pixelate: 'pixelate',
};

// ---------------------------------------------------------------- state ----
const state = {
    p2p: null,
    self_npub: null,
    username: null,
    users: {},            // npub -> {npub, username}
    seen: new Set(),
};

const media = {
    joined: false,
    rawStream: null,      // camera + mic from getUserMedia
    outStream: null,      // canvas video (filtered) + mic
    screenStream: null,
    sourceVideo: null,
    canvas: null,
    ctx: null,
    pixelCanvas: null,
    pixelCtx: null,
    outVideoTrack: null,
    micOn: true,
    camOn: true,
    filter: 'none',
    peers: {},            // npub -> RTCPeerConnection
    peerState: {},        // npub -> {joined, mic, cam, filter, sharing}
    pendingIce: {},       // npub -> [candidate]
    booted: false,
};

let chatLog = [];         // array of message objects
const chatMids = new Set();
const tiles = {};         // npub -> tile element
const usersWithTiles = new Set();
const fileMeta = {};      // hash -> {hash, name, size}
const incoming = {};      // hash -> {chunks, got, total, name, size, from, blob?}
let rafId = 0;
let heartbeatTick = 0;

// -------------------------------------------------------------- helpers ----
function uid() {
    return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10);
}
function shortNpub(npub) { return npub ? npub.slice(0, 12) + '…' : '?'; }
function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function nameOf(npub) {
    if (npub === state.self_npub) return state.username || 'You';
    return state.users[npub]?.username || shortNpub(npub);
}
function initials(npub) {
    const n = nameOf(npub).trim();
    return (n[0] || '?').toUpperCase();
}
function toast(text, ms = 3200) {
    const wrap = document.getElementById('toast-wrap');
    const el = document.createElement('div');
    el.className = 'toast';
    el.textContent = text;
    wrap.appendChild(el);
    setTimeout(() => el.remove(), ms);
}
function fmtSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1024 / 1024).toFixed(1) + ' MB';
}
function b64FromBuf(buf) {
    const bytes = new Uint8Array(buf);
    let bin = '';
    const CH = 0x8000;
    for (let i = 0; i < bytes.length; i += CH) {
        bin += String.fromCharCode.apply(null, bytes.subarray(i, i + CH));
    }
    return btoa(bin);
}
function bufFromB64(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
}
async function sha256hex(buf) {
    const digest = await crypto.subtle.digest('SHA-256', buf);
    return Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, '0')).join('');
}

// --------------------------------------------------------------- gossip ----
function markSeen(mid) {
    if (!mid) return true;
    if (state.seen.has(mid)) return false;
    state.seen.add(mid);
    if (state.seen.size > SEEN_MAX) {
        const it = state.seen.values();
        for (let i = 0; i < 1000; i++) state.seen.delete(it.next().value);
    }
    return true;
}

function gossip(msg, except = []) {
    msg.mid = msg.mid || uid();
    state.p2p.broadcast(msg, except);
}

function sendDirect(npub, msg) {
    if (!npub || npub === state.self_npub) return;
    msg.mid = msg.mid || uid();
    msg.to = npub;
    if (state.p2p.isConnected(npub)) {
        try { state.p2p.send(npub, msg); return; } catch { /* fall through */ }
    }
    state.p2p.broadcast(msg, []);
}

// The library verifies every message's signature before delivering it, so a
// forwarded message must be re-broadcast byte-for-byte (changing it would
// invalidate the signature). Loops are prevented by the seen-set above.
function forward(msg, except, from) {
    state.p2p.broadcast(msg, [...except, from].filter(Boolean));
}

async function handleMessage(npub, msg) {
    if (!msg || typeof msg !== 'object' || !msg.type) return;
    if (!markSeen(msg.mid)) return;

    const from = msg.sender || npub;
    const directedElsewhere = msg.to && msg.to !== state.self_npub;
    if (directedElsewhere) {
        forward(msg, [npub], from);
        return;
    }

    if (msg.username) rememberUser(from, msg.username);
    if (msg.peers) for (const p of msg.peers) addKnownPeer(p, false);

    switch (msg.type) {
        case 'hello':
            addKnownPeer(from, true);
            renderPeople();
            break;
        case 'peers':
            renderPeople();
            break;
        case 'chat':
            addChat({ type: 'chat', mid: msg.mid, sender: from, text: msg.text, at: msg.at || Date.now() });
            renderChat();
            break;
        case 'chat_history':
            for (const m of (msg.messages || [])) {
                if (!m || !m.mid) continue;
                addChat(m);
            }
            renderChat();
            break;
        case 'file_meta':
            registerFile(msg.file, from);
            addChat({ type: 'file', mid: msg.mid, sender: from, file: msg.file, at: msg.at || Date.now() });
            renderChat();
            break;
        case 'file_req':
            await serveFile(msg.hash, from);
            break;
        case 'file_chunk':
            await receiveChunk(msg);
            break;
        case 'media_state': {
            const prev = media.peerState[from] || {};
            media.peerState[from] = { joined: true, mic: msg.mic, cam: msg.cam, filter: msg.filter, sharing: msg.sharing };
            if (!prev.joined) addChat({ type: 'system', text: nameOf(from) + ' joined the call', at: Date.now(), mid: uid() });
            renderPeople();
            refreshTiles();
            if (media.joined && state.self_npub > from) maybeOffer(from);
            break;
        }
        case 'media_leave':
            if (media.peerState[from]) {
                addChat({ type: 'system', text: nameOf(from) + ' left the call', at: Date.now(), mid: uid() });
            }
            delete media.peerState[from];
            teardownMediaPeer(from);
            renderPeople();
            refreshTiles();
            renderChat();
            break;
        case 'rtc_offer':
            await onRtcOffer(from, msg.sdp);
            break;
        case 'rtc_answer':
            await onRtcAnswer(from, msg.sdp);
            break;
        case 'rtc_candidate':
            await onRtcCandidate(from, msg.candidate);
            break;
    }

    if (!msg.to && RELAYABLE.has(msg.type)) {
        forward(msg, [npub], from);
    }
}

// ------------------------------------------------------------ peer set -----
function knownPeers() {
    return Array.from(state.p2p.peers).filter(p => p !== state.self_npub);
}
async function addKnownPeer(npub, persist = true) {
    if (!npub || npub === state.self_npub) return;
    if (!state.p2p.peers.has(npub)) {
        state.p2p.addPeer(npub);
        const peers = JSON.parse(localStorage.getItem('rookoo_peers') || '[]');
        if (!peers.includes(npub)) {
            peers.push(npub);
            localStorage.setItem('rookoo_peers', JSON.stringify(peers.slice(-200)));
        }
    }
    renderPeople();
    updatePeerStatus();
}
function rememberUser(npub, username) {
    if (!npub || npub === state.self_npub) return;
    const u = state.users[npub] || { npub };
    u.username = username || u.username;
    state.users[npub] = u;
}
function extractNpub(text) {
    if (!text) return null;
    const m = String(text).match(/npub1[02-9ac-hj-np-z]{20,}/i);
    return m ? m[0] : null;
}

// -------------------------------------------------------------- identity ---
function setupLogin() {
    const overlay = document.getElementById('login-overlay');
    const sk = localStorage.getItem('rookoo_sk');
    const username = localStorage.getItem('rookoo_username');
    if (sk && username) {
        overlay.remove();
        state.username = username;
        startApp(sk);
        return;
    }
    document.getElementById('login-card').addEventListener('submit', (e) => {
        e.preventDefault();
        const name = document.getElementById('login-username').value.trim();
        if (!name) return;
        let key = document.getElementById('login-key').value.trim();
        try {
            if (!key) key = bytesToHex(generateSecretKey());
            else if (key.startsWith('nsec1')) key = bytesToHex(nip19.decode(key).data);
            else if (!/^[0-9a-f]{64}$/i.test(key)) throw new Error('bad');
            getPublicKey(hexToBytes(key));
        } catch {
            document.getElementById('login-error').textContent = 'Key must be an nsec… or 64-char hex string.';
            return;
        }
        localStorage.setItem('rookoo_sk', key);
        localStorage.setItem('rookoo_username', name);
        state.username = name;
        overlay.remove();
        startApp(key);
    });
}

function startApp(sk) {
    state.p2p = new NostrP2P(sk, {
        maxConnections: 12,
        open: true,
        onConnect: async (npub) => {
            addKnownPeer(npub);
            sendDirect(npub, { type: 'hello', username: state.username, peers: knownPeers() });
            if (chatLog.length) sendDirect(npub, { type: 'chat_history', messages: chatLog.slice(-40) });
            if (media.joined) {
                sendDirect(npub, { type: 'media_state', mic: media.micOn, cam: media.camOn, filter: media.filter, sharing: !!media.screenStream });
                if (state.self_npub > npub) maybeOffer(npub);
            }
            renderPeople();
            updatePeerStatus();
        },
        onMessage: (npub, msg) => { handleMessage(npub, msg).catch(console.error); },
        onDisconnect: (npub) => {
            if (media.peerState[npub]) {
                delete media.peerState[npub];
                teardownMediaPeer(npub);
                refreshTiles();
            }
            renderPeople();
            updatePeerStatus();
        },
    });
    state.self_npub = state.p2p.npub;
    media.booted = true;

    for (const p of JSON.parse(localStorage.getItem('rookoo_peers') || '[]')) state.p2p.addPeer(p);

    // Invite links: .../#npub1… — auto-add the inviter.
    const invited = extractNpub(location.hash.slice(1));
    if (invited && invited !== state.self_npub) addKnownPeer(invited, true);

    document.getElementById('app').hidden = false;
    buildUI();
    refreshTiles();
    renderPeople();
    renderChat();

    setInterval(tick, 5000);
    const resume = () => { if (!media.booted) return; state.p2p.resume(); updatePeerStatus(); };
    document.addEventListener('visibilitychange', () => { if (!document.hidden) resume(); });
    window.addEventListener('pageshow', (e) => { if (e.persisted) resume(); });
    window.addEventListener('online', resume);
    window.addEventListener('beforeunload', () => {
        if (media.joined) {
            try { state.p2p.broadcast({ type: 'media_leave' }); } catch { /* best effort */ }
        }
    });
}

function tick() {
    updatePeerStatus();
    if (media.joined) {
        heartbeatTick++;
        if (heartbeatTick % 2 === 0) announceMedia();
        for (const npub of state.p2p.connections.keys()) {
            if (media.peerState[npub]?.joined && state.self_npub > npub && !media.peers[npub]) maybeOffer(npub);
        }
    }
}

function updatePeerStatus() {
    const el = document.getElementById('peer-status');
    if (!el || !state.p2p) return;
    const n = state.p2p.connections.size;
    const known = state.p2p.peers.size - 1;
    const inCall = Object.values(media.peerState).filter(p => p.joined).length;
    el.className = 'pill' + (n ? ' ok' : known ? ' warn' : '');
    el.textContent = n
        ? `${n} peer${n > 1 ? 's' : ''} connected · ${inCall} in call`
        : known
            ? `Reaching ${known} peer${known > 1 ? 's' : ''}…`
            : 'No peers yet';
}

// ------------------------------------------------------------- video UI ----
function ensureTile(npub) {
    if (tiles[npub]) return tiles[npub];
    const isSelf = npub === state.self_npub;
    const el = document.createElement('div');
    el.className = 'tile' + (isSelf ? ' self' : '');
    el.dataset.npub = npub;
    el.innerHTML =
        `<div class="avatar">${escapeHtml(initials(npub))}</div>` +
        `<video autoplay playsinline></video>` +
        `<div class="label"><span class="name"></span></div>` +
        `<div class="badges"></div>`;
    const vid = el.querySelector('video');
    if (isSelf) vid.muted = true;
    document.getElementById('video-grid').appendChild(el);
    tiles[npub] = el;
    return el;
}

function refreshTiles() {
    const grid = document.getElementById('video-grid');
    // Self always has a tile.
    ensureTile(state.self_npub);

    const present = new Set([state.self_npub]);
    for (const [npub, st] of Object.entries(media.peerState)) {
        if (st.joined) { ensureTile(npub); present.add(npub); }
    }
    for (const npub of Object.keys(tiles)) {
        if (!present.has(npub)) { tiles[npub].remove(); delete tiles[npub]; }
    }

    for (const npub of present) {
        const el = tiles[npub];
        const isSelf = npub === state.self_npub;
        const st = isSelf
            ? { joined: media.joined, mic: media.micOn, cam: media.camOn, sharing: !!media.screenStream }
            : (media.peerState[npub] || {});
        const vid = el.querySelector('video');
        el.querySelector('.name').textContent = nameOf(npub) + (isSelf ? ' (you)' : '');
        el.querySelector('.avatar').textContent = initials(npub);

        let hasVideo = false;
        if (isSelf) {
            const stream = media.screenStream || media.outStream;
            if (stream && st.joined) {
                if (vid.srcObject !== stream) vid.srcObject = stream;
                hasVideo = !!(media.screenStream || (media.outStream && media.outStream.getVideoTracks().length));
            } else if (vid.srcObject) {
                vid.srcObject = null;
            }
            el.classList.toggle('self', true);
        } else {
            hasVideo = !!vid.srcObject && !!vid.srcObject.getVideoTracks().length;
        }
        el.classList.toggle('has-video', hasVideo);
        el.classList.toggle('cam-off', st.joined && st.cam === false);
        el.classList.toggle('screen', !!st.sharing);
        el.classList.toggle('not-joined', !st.joined);

        const badges = [];
        if (st.joined && st.mic === false) badges.push('🔇');
        if (st.joined && st.cam === false) badges.push('📷');
        if (st.sharing) badges.push('🖥️');
        el.querySelector('.badges').innerHTML = badges.map(b => `<span>${b}</span>`).join('');
    }

    // Empty state / invite helper.
    const remote = present.size - 1;
    let empty = document.getElementById('empty-state');
    if (!remote && !media.joined) {
        if (!empty) {
            empty = document.createElement('div');
            empty.id = 'empty-state';
            grid.appendChild(empty);
        }
        const link = location.origin + location.pathname + '#' + state.self_npub;
        empty.innerHTML = `<div class="box">
            <h3>No one else is here yet</h3>
            <p class="muted">Click <strong>Join</strong> to turn on your camera, then send this invite link to a friend. They can also paste your peer ID with <strong>Add peer</strong>.</p>
            <code>${escapeHtml(link)}</code>
            <p class="muted small">Your peer ID: ${escapeHtml(state.self_npub)}</p>
        </div>`;
    } else if (empty) {
        empty.remove();
    }
}

function renderPeople() {
    const list = document.getElementById('people-list');
    if (!list) return;
    const conns = state.p2p ? state.p2p.connections : new Map();
    const rows = [];
    rows.push(`<div class="person">
        <div class="av" style="background:linear-gradient(135deg,#4f8cff,#8f5cff)">${escapeHtml(initials(state.self_npub))}</div>
        <div class="info"><div>${escapeHtml(nameOf(state.self_npub))} (you)</div>
        <div class="sub">${media.joined ? (media.micOn ? 'mic on' : 'muted') + ' · ' + (media.camOn ? 'camera on' : 'camera off') : 'not in call'}</div></div>
        <div class="dot ${conns.size ? 'on' : ''}"></div></div>`);

    const npubs = new Set([...conns.keys(), ...Object.keys(state.users)]);
    for (const npub of npubs) {
        if (npub === state.self_npub) continue;
        const st = media.peerState[npub];
        const sub = st?.joined
            ? (st.mic ? 'mic on' : 'muted') + ' · ' + (st.cam ? 'camera on' : 'camera off')
            : (conns.has(npub) ? 'connected' : 'offline');
        rows.push(`<div class="person">
            <div class="av">${escapeHtml(initials(npub))}</div>
            <div class="info"><div>${escapeHtml(nameOf(npub))}</div><div class="sub">${escapeHtml(sub)}</div></div>
            <div class="dot ${conns.has(npub) ? 'on' : ''}"></div></div>`);
    }
    list.innerHTML = rows.join('');
}

function renderChat() {
    const log = document.getElementById('chat-log');
    if (!log) return;
    log.innerHTML = '';
    for (const m of chatLog.slice(-300)) {
        const el = document.createElement('div');
        if (m.type === 'system') {
            el.className = 'msg system';
            el.textContent = m.text;
        } else if (m.type === 'file') {
            el.className = 'msg' + (m.sender === state.self_npub ? ' self' : '');
            const inc = incoming[m.file.hash];
            const pct = inc ? Math.round((inc.got / inc.total) * 100) : 0;
            el.innerHTML = `<div class="meta"><span>${escapeHtml(nameOf(m.sender))}</span><span>${new Date(m.at).toLocaleTimeString()}</span></div>
                <div class="body">📎 ${escapeHtml(m.file.name)} <span class="muted">(${fmtSize(m.file.size)})</span></div>
                <a class="dl" data-hash="${escapeHtml(m.file.hash)}">${inc ? 'Downloading… ' + pct + '%' : 'Download'}</a>
                ${inc ? `<div class="progress"><i style="width:${pct}%"></i></div>` : ''}`;
        } else {
            el.className = 'msg' + (m.sender === state.self_npub ? ' self' : '');
            el.innerHTML = `<div class="meta"><span>${escapeHtml(nameOf(m.sender))}</span><span>${new Date(m.at).toLocaleTimeString()}</span></div>
                <div class="body">${escapeHtml(m.text)}</div>`;
        }
        log.appendChild(el);
    }
    log.scrollTop = log.scrollHeight;
    renderFiles();
}

function addChat(m) {
    if (!m) return;
    if (m.mid && chatMids.has(m.mid)) return;
    if (m.mid) chatMids.add(m.mid);
    chatLog.push(m);
    if (chatLog.length > 1000) chatLog = chatLog.slice(-800);
}

function renderFiles() {
    const list = document.getElementById('files-list');
    if (!list) return;
    const entries = Object.values(fileMeta);
    if (!entries.length) {
        list.innerHTML = '<p class="muted small">No files shared yet.</p>';
        return;
    }
    list.innerHTML = entries.slice(-100).reverse().map(f => {
        const inc = incoming[f.hash];
        const pct = inc ? Math.round((inc.got / inc.total) * 100) : 0;
        return `<div class="msg file-item">
            <div class="icon">📄</div>
            <div class="info"><div class="n">${escapeHtml(f.name)}</div>
            <div class="sub muted">${fmtSize(f.size)}</div>
            ${inc ? `<div class="progress"><i style="width:${pct}%"></i></div>` : ''}</div>
            <a class="dl" data-hash="${escapeHtml(f.hash)}">Get</a></div>`;
    }).join('');
}

// ----------------------------------------------------------------- chat ----
function sendChat(text) {
    const t = text.trim();
    if (!t) return;
    const msg = { type: 'chat', mid: uid(), text: t, at: Date.now(), ttl: 8 };
    state.p2p.broadcast(msg); // broadcast signs and stamps msg.sender
    addChat(msg);
    renderChat();
}

// ---------------------------------------------------------------- files ----
function registerFile(file, from) {
    if (!file || !file.hash) return;
    if (!fileMeta[file.hash]) fileMeta[file.hash] = { hash: file.hash, name: file.name, size: file.size, from };
}

async function shareFile(file) {
    const buf = await file.arrayBuffer();
    const hash = await sha256hex(buf);
    await store.putFile({ hash, name: file.name, size: file.size, channel: null, blob: file });
    const meta = { hash, name: file.name, size: file.size };
    registerFile(meta, state.self_npub);
    const msg = { type: 'file_meta', mid: uid(), file: meta, at: Date.now(), ttl: 8 };
    state.p2p.broadcast(msg);
    addChat({ type: 'file', mid: msg.mid, sender: state.self_npub, file: meta, at: msg.at });
    renderChat();
    toast('Shared ' + file.name);
}

async function serveFile(hash, requester) {
    if (!hash || !requester) return;
    const f = await store.getFile(hash);
    if (!f || !f.blob) return;
    const buf = await f.blob.arrayBuffer();
    const total = Math.max(1, Math.ceil(buf.byteLength / FILE_CHUNK));
    for (let seq = 0; seq < total; seq++) {
        const slice = buf.slice(seq * FILE_CHUNK, (seq + 1) * FILE_CHUNK);
        sendDirect(requester, {
            type: 'file_chunk', hash, seq, total,
            name: f.name, size: f.size,
            data: b64FromBuf(slice),
        });
        if (seq % 20 === 19) await new Promise(r => setTimeout(r, 0));
    }
}

async function requestFile(hash, name, size) {
    if (!hash) return;
    const have = await store.getFile(hash);
    if (have && have.blob) { saveBlob(have.blob, have.name || name); return; }
    if (incoming[hash]) { toast('Already downloading…'); return; }
    const total = Math.max(1, Math.ceil((size || 0) / FILE_CHUNK));
    incoming[hash] = { chunks: new Array(total), got: 0, total, name, size, from: null, at: Date.now() };
    gossip({ type: 'file_req', hash, ttl: 8 });
    renderChat();
    toast('Requesting ' + (name || 'file') + '…');
}

async function receiveChunk(msg) {
    const inc = incoming[msg.hash];
    if (!inc) return;
    if (inc.from && inc.from !== msg.sender) return; // one provider at a time
    inc.from = msg.sender;
    if (msg.total && msg.total > inc.total) {
        inc.total = msg.total;
        inc.chunks.length = msg.total;
    }
    if (inc.chunks[msg.seq] === undefined) {
        inc.chunks[msg.seq] = msg.data;
        inc.got++;
    }
    renderChat();
    if (inc.got >= inc.total) {
        const parts = inc.chunks.map(d => bufFromB64(d || ''));
        const blob = new Blob(parts, { type: 'application/octet-stream' });
        await store.putFile({ hash: msg.hash, name: inc.name, size: blob.size, channel: null, blob });
        delete incoming[msg.hash];
        saveBlob(blob, inc.name);
        toast('Received ' + inc.name);
        renderChat();
    }
}

function saveBlob(blob, name) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = name || 'download';
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 30000);
}

// ---------------------------------------------------------------- media ----
function iceServers() {
    const ice = [{ urls: 'stun:stun.l.google.com:19302' }];
    try {
        const t = JSON.parse(localStorage.getItem('nostr_p2p_turn') || 'null');
        if (t && t.urls) ice.push(t);
    } catch { /* ignore */ }
    return ice;
}

async function startMedia() {
    try {
        media.rawStream = await navigator.mediaDevices.getUserMedia({
            video: { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30 } },
            audio: { echoCancellation: true, noiseSuppression: true },
        });
    } catch (e) {
        toast('Camera unavailable (' + e.name + ') — trying audio only');
        try {
            media.rawStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        } catch (e2) {
            media.rawStream = null;
        }
    }

    if (!media.rawStream) {
        media.micOn = false;
        media.camOn = false;
        toast('No camera or microphone — you can still watch, chat and share files');
        return;
    }

    const hasVideo = media.rawStream.getVideoTracks().length > 0;
    media.micOn = media.rawStream.getAudioTracks().length > 0;
    media.camOn = hasVideo;

    if (hasVideo) {
        // Canvas pipeline: source camera -> filtered canvas -> outgoing track.
        const v = document.createElement('video');
        v.muted = true;
        v.playsInline = true;
        v.srcObject = media.rawStream;
        await v.play().catch(() => {});
        media.sourceVideo = v;

        media.canvas = document.createElement('canvas');
        media.canvas.width = 1280;
        media.canvas.height = 720;
        media.ctx = media.canvas.getContext('2d', { alpha: false });
        media.pixelCanvas = document.createElement('canvas');
        media.pixelCanvas.width = 96;
        media.pixelCanvas.height = 54;
        media.pixelCtx = media.pixelCanvas.getContext('2d');

        const captured = media.canvas.captureStream(30);
        media.outVideoTrack = captured.getVideoTracks()[0];
        media.outStream = new MediaStream([media.outVideoTrack, ...media.rawStream.getAudioTracks()]);
        drawLoop();
    } else {
        media.outStream = new MediaStream([...media.rawStream.getAudioTracks()]);
    }
}

function drawLoop() {
    if (!media.ctx) return;
    const { ctx, sourceVideo, canvas } = media;
    if (sourceVideo.readyState >= 2) {
        const f = FILTERS[media.filter] || '';
        if (f === 'pixelate') {
            media.pixelCtx.drawImage(sourceVideo, 0, 0, media.pixelCanvas.width, media.pixelCanvas.height);
            ctx.imageSmoothingEnabled = false;
            ctx.filter = 'none';
            ctx.drawImage(media.pixelCanvas, 0, 0, canvas.width, canvas.height);
            ctx.imageSmoothingEnabled = true;
        } else {
            ctx.filter = f || 'none';
            ctx.drawImage(sourceVideo, 0, 0, canvas.width, canvas.height);
        }
    }
    rafId = requestAnimationFrame(drawLoop);
}

function announceMedia() {
    if (!state.p2p) return;
    gossip({
        type: 'media_state',
        mic: media.micOn,
        cam: media.camOn,
        filter: media.filter,
        sharing: !!media.screenStream,
        ttl: 8,
    });
}

async function joinMeeting() {
    if (media.joined) return;
    await startMedia();
    media.joined = true;
    document.getElementById('join-btn').hidden = true;
    document.getElementById('call-controls').hidden = false;
    announceMedia();
    for (const npub of state.p2p.connections.keys()) {
        if (state.self_npub > npub) maybeOffer(npub);
    }
    refreshTiles();
    renderPeople();
}

function leaveMeeting() {
    if (state.p2p) gossip({ type: 'media_leave', ttl: 8 });
    for (const npub of Object.keys(media.peers)) teardownMediaPeer(npub);
    stopScreen(true);
    if (rafId) { cancelAnimationFrame(rafId); rafId = 0; }
    if (media.rawStream) media.rawStream.getTracks().forEach(t => t.stop());
    media.rawStream = null;
    media.outStream = null;
    media.outVideoTrack = null;
    media.sourceVideo = null;
    media.canvas = null;
    media.ctx = null;
    media.joined = false;
    document.getElementById('join-btn').hidden = false;
    document.getElementById('call-controls').hidden = true;
    refreshTiles();
    renderPeople();
}

function maybeOffer(npub) {
    if (!media.joined || !npub || npub === state.self_npub) return;
    if (media.peers[npub]) return;
    if (state.self_npub > npub) createMediaPeer(npub, true);
}

async function createMediaPeer(npub, initiator) {
    if (media.peers[npub]) return;
    const pc = new RTCPeerConnection({ iceServers: iceServers() });
    media.peers[npub] = pc;

    const sendTracks = media.outStream ? media.outStream.getTracks() : [];
    for (const track of sendTracks) pc.addTrack(track, media.outStream || new MediaStream([track]));

    // If we are already screen sharing, use the display track for video.
    if (media.screenStream) {
        const sender = pc.getSenders().find(s => s.track && s.track.kind === 'video');
        if (sender) sender.replaceTrack(media.screenStream.getVideoTracks()[0]).catch(() => {});
    }

    pc.onicecandidate = (e) => {
        if (e.candidate) sendDirect(npub, { type: 'rtc_candidate', candidate: e.candidate.toJSON() });
    };
    pc.ontrack = (e) => {
        const stream = e.streams[0];
        if (!stream) return;
        const el = ensureTile(npub);
        const vid = el.querySelector('video');
        if (vid.srcObject !== stream) vid.srcObject = stream;
        el.classList.add('has-video');
        vid.play().catch(() => toast('Click anywhere to enable audio playback'));
        refreshTiles();
    };
    pc.onconnectionstatechange = () => {
        if (['failed', 'closed'].includes(pc.connectionState)) teardownMediaPeer(npub);
    };

    flushIce(npub);

    if (initiator) {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        sendDirect(npub, { type: 'rtc_offer', sdp: { type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
    }
}

function teardownMediaPeer(npub) {
    const pc = media.peers[npub];
    if (!pc) return;
    delete media.peers[npub];
    delete media.pendingIce[npub];
    try { pc.close(); } catch { /* ignore */ }
}

async function onRtcOffer(from, sdp) {
    if (!media.joined) {
        toast(nameOf(from) + ' started a call — joining…');
        try { await joinMeeting(); } catch { return; }
    }
    let pc = media.peers[from];
    if (pc) {
        // Glare: the larger npub keeps its offer, the smaller yields.
        if (state.self_npub > from) return;
        teardownMediaPeer(from);
    }
    pc = new RTCPeerConnection({ iceServers: iceServers() });
    media.peers[from] = pc;

    const sendTracks = media.outStream ? media.outStream.getTracks() : [];
    for (const track of sendTracks) pc.addTrack(track, media.outStream || new MediaStream([track]));
    if (media.screenStream) {
        const sender = pc.getSenders().find(s => s.track && s.track.kind === 'video');
        if (sender) sender.replaceTrack(media.screenStream.getVideoTracks()[0]).catch(() => {});
    }
    pc.onicecandidate = (e) => {
        if (e.candidate) sendDirect(from, { type: 'rtc_candidate', candidate: e.candidate.toJSON() });
    };
    pc.ontrack = (e) => {
        const stream = e.streams[0];
        if (!stream) return;
        const el = ensureTile(from);
        const vid = el.querySelector('video');
        if (vid.srcObject !== stream) vid.srcObject = stream;
        el.classList.add('has-video');
        vid.play().catch(() => {});
        refreshTiles();
    };
    pc.onconnectionstatechange = () => {
        if (['failed', 'closed'].includes(pc.connectionState)) teardownMediaPeer(from);
    };

    await pc.setRemoteDescription(sdp);
    flushIce(from);
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    sendDirect(from, { type: 'rtc_answer', sdp: { type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
}

async function onRtcAnswer(from, sdp) {
    const pc = media.peers[from];
    if (!pc || pc.signalingState !== 'have-local-offer') return;
    await pc.setRemoteDescription(sdp);
    flushIce(from);
}

async function onRtcCandidate(from, candidate) {
    const pc = media.peers[from];
    if (!pc || !pc.remoteDescription) {
        (media.pendingIce[from] = media.pendingIce[from] || []).push(candidate);
        return;
    }
    try { await pc.addIceCandidate(candidate); } catch { /* stale */ }
}

function flushIce(npub) {
    const pc = media.peers[npub];
    const buf = media.pendingIce[npub] || [];
    if (!pc || !pc.remoteDescription || !buf.length) return;
    media.pendingIce[npub] = [];
    for (const c of buf) pc.addIceCandidate(c).catch(() => {});
}

// ------------------------------------------------------------ controls -----
function setMic(on) {
    media.micOn = on;
    if (media.rawStream) media.rawStream.getAudioTracks().forEach(t => { t.enabled = on; });
    document.getElementById('mic-btn').classList.toggle('off', !on);
    document.getElementById('mic-btn').textContent = on ? '🎤' : '🔇';
    announceMedia();
    refreshTiles();
    renderPeople();
}

function setCam(on) {
    media.camOn = on;
    if (media.rawStream) media.rawStream.getVideoTracks().forEach(t => { t.enabled = on; });
    if (media.outVideoTrack) media.outVideoTrack.enabled = on;
    if (media.sourceVideo) { if (on) media.sourceVideo.play().catch(() => {}); }
    document.getElementById('cam-btn').classList.toggle('off', !on);
    document.getElementById('cam-btn').textContent = on ? '🎥' : '🚫';
    announceMedia();
    refreshTiles();
    renderPeople();
}

function setFilter(name) {
    if (!(name in FILTERS)) name = 'none';
    media.filter = name;
    announceMedia();
    toast('Filter: ' + document.querySelector(`#filter-select option[value="${name}"]`)?.textContent);
}

async function toggleScreen() {
    if (media.screenStream) { stopScreen(); return; }
    try {
        media.screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
    } catch { return; }
    const track = media.screenStream.getVideoTracks()[0];
    for (const npub of Object.keys(media.peers)) {
        const sender = media.peers[npub].getSenders().find(s => s.track && s.track.kind === 'video');
        if (sender) sender.replaceTrack(track).catch(() => {});
    }
    track.onended = () => stopScreen();
    document.getElementById('screen-btn').classList.add('off');
    announceMedia();
    refreshTiles();
    toast('Sharing your screen');
}

function stopScreen(silent = false) {
    if (!media.screenStream) return;
    media.screenStream.getTracks().forEach(t => t.stop());
    media.screenStream = null;
    for (const npub of Object.keys(media.peers)) {
        const sender = media.peers[npub].getSenders().find(s => s.track && s.track.kind === 'video');
        if (sender && media.outVideoTrack) sender.replaceTrack(media.outVideoTrack).catch(() => {});
    }
    document.getElementById('screen-btn')?.classList.remove('off');
    if (!silent) { announceMedia(); refreshTiles(); }
}

// The library must not signal with itself, so guard against two tabs sharing
// one identity (they would be indistinguishable to peers).
function checkMultiTab() {
    const tabId = uid();
    const started = Date.now();
    const claim = () => localStorage.setItem('rookoo_tab', JSON.stringify({ id: tabId, ts: Date.now() }));
    claim();
    setInterval(() => {
        let cur = null;
        try { cur = JSON.parse(localStorage.getItem('rookoo_tab') || 'null'); } catch { /* ignore */ }
        const other = cur && cur.id !== tabId && cur.ts >= started && Date.now() - cur.ts < 10000;
        if (other) {
            try { state.p2p?.close(); } catch { /* ignore */ }
            if (!document.getElementById('tab-warning')) {
                const w = document.createElement('div');
                w.id = 'tab-warning';
                w.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:2000;background:#e5484d;color:#fff;text-align:center;padding:8px;';
                w.textContent = 'RooKoo is open in another tab with the same identity — this tab is disconnected.';
                document.body.prepend(w);
            }
        } else {
            claim();
        }
    }, 4000);
}

// ------------------------------------------------------------------ UI -----
function buildUI() {
    document.getElementById('join-btn').addEventListener('click', () => joinMeeting().catch(e => toast('Could not join: ' + e.message)));
    document.getElementById('leave-btn').addEventListener('click', leaveMeeting);
    document.getElementById('mic-btn').addEventListener('click', () => setMic(!media.micOn));
    document.getElementById('cam-btn').addEventListener('click', () => setCam(!media.camOn));
    document.getElementById('screen-btn').addEventListener('click', toggleScreen);
    document.getElementById('filter-select').addEventListener('change', (e) => setFilter(e.target.value));
    document.getElementById('mirror-btn').addEventListener('click', () => {
        const self = tiles[state.self_npub];
        if (self) self.classList.toggle('mirror-off');
    });

    document.getElementById('chat-form').addEventListener('submit', (e) => {
        e.preventDefault();
        const inp = document.getElementById('chat-input');
        sendChat(inp.value);
        inp.value = '';
    });
    document.getElementById('file-input').addEventListener('change', async (e) => {
        const f = e.target.files[0];
        e.target.value = '';
        if (f) await shareFile(f);
    });

    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t === tab));
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.dataset.panel === tab.dataset.tab));
        });
    });
    document.getElementById('sidebar-toggle').addEventListener('click', () => {
        document.getElementById('sidebar').classList.toggle('open');
    });

    document.getElementById('copy-invite').addEventListener('click', async () => {
        const link = location.origin + location.pathname + '#' + state.self_npub;
        try { await navigator.clipboard.writeText(link); toast('Invite link copied'); }
        catch { toast(link); }
    });

    const dialog = document.getElementById('add-peer-dialog');
    document.getElementById('add-peer-open').addEventListener('click', () => dialog.showModal());
    document.getElementById('add-peer-form').addEventListener('submit', (e) => {
        e.preventDefault();
        const val = document.getElementById('add-peer-input').value;
        const npub = extractNpub(val);
        if (!npub) { toast('That does not look like an npub or invite link'); return; }
        if (npub === state.self_npub) { toast('That is your own peer ID'); return; }
        addKnownPeer(npub, true);
        dialog.close();
        document.getElementById('add-peer-input').value = '';
        toast('Connecting to ' + shortNpub(npub) + '…');
    });
    document.getElementById('add-peer-cancel')?.addEventListener('click', () => dialog.close());

    const settingsDialog = document.getElementById('settings-dialog');
    document.getElementById('settings-open').addEventListener('click', () => {
        const relays = JSON.parse(localStorage.getItem('nostr_p2p_relays') || 'null');
        document.getElementById('relays-input').value = Array.isArray(relays) ? relays.join('\n') : '';
        const turn = JSON.parse(localStorage.getItem('nostr_p2p_turn') || 'null');
        document.getElementById('turn-input').value = turn?.urls || '';
        settingsDialog.showModal();
    });
    document.getElementById('settings-cancel').addEventListener('click', () => settingsDialog.close());
    document.getElementById('settings-form').addEventListener('submit', (e) => {
        e.preventDefault();
        const urls = document.getElementById('relays-input').value.split(/[\n,]+/).map(s => s.trim()).filter(Boolean);
        if (urls.length) localStorage.setItem('nostr_p2p_relays', JSON.stringify(urls));
        else localStorage.removeItem('nostr_p2p_relays');
        const turn = document.getElementById('turn-input').value.trim();
        if (turn) localStorage.setItem('nostr_p2p_turn', JSON.stringify({ urls: turn }));
        else localStorage.removeItem('nostr_p2p_turn');
        location.reload();
    });

    // Delegated download clicks in chat / files list.
    document.addEventListener('click', (e) => {
        const a = e.target.closest('.dl');
        if (!a) return;
        const hash = a.dataset.hash;
        const f = fileMeta[hash];
        requestFile(hash, f?.name, f?.size).catch(console.error);
    });
}

document.addEventListener('DOMContentLoaded', () => { setupLogin(); checkMultiTab(); });
window.__state = state;
window.__media = media;
window.__store = store;
window.__fileMeta = fileMeta;
window.__incoming = incoming;
