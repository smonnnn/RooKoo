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
import { generateSecretKey, getPublicKey, bytesToHex, hexToBytes } from './nostr-deps.js';

const FILE_CHUNK = 48 * 1024;
const SEEN_MAX = 5000;

// Public relays that are open (no auth / proof-of-work) and accept ephemeral
// signaling events, probed from this machine. Overridable in Settings.
const DEFAULT_RELAYS = [
    'wss://relay.snort.social',
    'wss://relay.primal.net',
    'wss://nostr.mom',
    'wss://nostr-pub.wellorder.net',
];
function resolveRelays() {
    try {
        const stored = JSON.parse(localStorage.getItem('nostr_p2p_relays') || 'null');
        if (Array.isArray(stored) && stored.length) return stored;
    } catch { /* fall through */ }
    return DEFAULT_RELAYS;
}

const RELAYABLE = new Set([
    'hello', 'profile', 'peers', 'chat', 'chat_history', 'file_meta', 'file_req',
    'media_state', 'media_leave',
]);

// Content-aware background effects. These need per-pixel person/background
// segmentation (MediaPipe selfie segmentation), composited on the canvas so
// the effect is baked into the outgoing video — visible to everyone, not just
// a local preview. They are applied to your camera only; screen shares bypass
// the canvas entirely.
const FILTER_LABELS = {
    none: 'No effect',
    blur: 'Blur background',
    'blur-strong': 'Blur background (strong)',
    pixelate: 'Pixelate background',
    grayscale: 'Black & white background',
    remove: 'Remove background',
    'bg-blue': 'Blue background',
    'bg-green': 'Green background',
    'bg-white': 'White background',
    'bg-warm': 'Warm background',
    'bg-gradient': 'Gradient background',
};
const BACKGROUND_EFFECTS = new Set([
    'blur', 'blur-strong', 'pixelate', 'grayscale', 'remove',
    'bg-blue', 'bg-green', 'bg-white', 'bg-warm', 'bg-gradient',
]);
const SOLID_BG = {
    remove: '#20262B',
    'bg-blue': '#3B82F6',
    'bg-green': '#22C55E',
    'bg-white': '#FFFFFF',
    'bg-warm': '#EDA35A',
};
// Pinned MediaPipe Tasks Vision build (loaded from CDN on first use).
const MEDIAPIPE_VERSION = '0.10.14';
const MEDIAPIPE_MODULE = `https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@${MEDIAPIPE_VERSION}`;
const MEDIAPIPE_WASM = `${MEDIAPIPE_MODULE}/wasm`;
const MEDIAPIPE_MODEL = 'https://storage.googleapis.com/mediapipe-models/image_segmenter/selfie_segmenter/float16/1/selfie_segmenter.tflite';

// ---------------------------------------------------------------- state ----
const state = {
    p2p: null,
    self_npub: null,
    username: null,
    users: {},            // npub -> {npub, username}
    introducers: {},      // npub -> { via: npub|null, at }  (signed provenance)
    joinedVia: null,      // the npub whose invite we used to enter the room
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
    personCanvas: null,
    personCtx: null,
    maskCanvas: null,
    maskCtx: null,
    maskImageData: null,
    maskReady: false,
    lastMaskAt: 0,
    segmenter: null,
    segmenterLoading: false,
    outVideoTrack: null,
    micOn: true,
    camOn: true,
    mirror: true,         // mirrors the outgoing canvas (and thus the self-view)
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
const incoming = {};      // hash -> {chunks, got, total, name, size, from, silent?}
const objectURLs = {};    // hash -> blob: URL (avatars and shared files)
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

    if (msg.username) rememberUser(from, msg.username, msg.pfp);
    if (msg.peers) for (const p of msg.peers) addKnownPeer(p, false);
    if (msg.joinedVia !== undefined) recordIntroducer(from, msg.joinedVia);

    switch (msg.type) {
        case 'hello':
        case 'profile':
            addKnownPeer(from);
            renderPeople();
            refreshTiles();
            hydrateAvatars();
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
async function addKnownPeer(npub) {
    if (!npub || npub === state.self_npub) return;
    if (!state.p2p.peers.has(npub)) state.p2p.addPeer(npub);
    renderPeople();
    updatePeerStatus();
}
function rememberUser(npub, username, pfp) {
    if (!npub || npub === state.self_npub) {
        // Our own profile may arrive from another device via sync; adopt it.
        if (npub === state.self_npub && pfp) {
            state.users[npub] = { ...state.users[npub], npub, username: state.username, pfp };
            saveProfile();
        }
        return;
    }
    const u = state.users[npub] || { npub };
    u.username = username || u.username;
    if (pfp !== undefined) u.pfp = pfp || null;
    state.users[npub] = u;
}
// Signed join provenance: a peer's `hello`/`profile` is signature-verified by
// the library, so `from` is authentic; it also carries the npub whose invite
// that peer used. We can therefore draw the room's join tree from any member's
// npub: e.g. "Bob joined via Alice".
function recordIntroducer(npub, via) {
    if (!npub || npub === state.self_npub) return;
    const viaClean = (via && via !== npub) ? via : null;
    const prev = state.introducers[npub];
    if (!prev || (prev.via === null && viaClean)) {
        state.introducers[npub] = { via: viaClean, at: Date.now() };
    }
    renderPeople();
}
function introducerLabel(npub) {
    const via = state.introducers[npub]?.via;
    if (via === undefined) return '';
    if (via === null) return 'joined directly';
    return 'joined via ' + nameOf(via);
}
function setJoinedVia(npub) {
    if (!npub || npub === state.self_npub) return;
    state.joinedVia = npub;
    localStorage.setItem('rookoo_joined_via', npub);
}
// Join through an invite fragment (#npub1…). Called at startup and on
// hashchange, so pasting an invite link into an already-open app connects
// immediately (a fragment-only change does not reload the page).
function handleInviteFromHash() {
    const invited = extractNpub(location.hash.slice(1));
    if (!invited || invited === state.self_npub || !state.p2p) return false;
    setJoinedVia(invited);
    addKnownPeer(invited);
    try { history.replaceState(null, '', location.pathname + location.search); } catch { /* ignore */ }
    toast('Joining the room through ' + shortNpub(invited) + '…');
    return true;
}
function extractNpub(text) {
    if (!text) return null;
    const m = String(text).match(/npub1[02-9ac-hj-np-z]{20,}/i);
    return m ? m[0] : null;
}

// ------------------------------------------------------ profile pictures ---
// Pictures are content-addressed like any shared file: the blob is stored
// locally under its SHA-256 hash, the hash travels in hello/profile, and peers
// fetch the blob on demand over the existing chunked file transport.
function myPfp() {
    return state.users[state.self_npub]?.pfp || null;
}
function saveProfile() {
    localStorage.setItem('rookoo_profile', JSON.stringify({
        username: state.username,
        pfp: myPfp(),
    }));
}
async function fileURL(hash) {
    if (!hash) return null;
    if (objectURLs[hash]) return objectURLs[hash];
    const f = await store.getFile(hash);
    if (f && f.blob) {
        objectURLs[hash] = URL.createObjectURL(f.blob);
        return objectURLs[hash];
    }
    return null;
}
// Fetch a blob (e.g. someone's avatar) without triggering a download.
async function requestBlob(hash) {
    if (!hash || objectURLs[hash] || incoming[hash]) return;
    const f = await store.getFile(hash);
    if (f && f.blob) { hydrateAvatars(); return; }
    incoming[hash] = { chunks: [], got: 0, total: 0, name: 'avatar', size: 0, from: null, silent: true };
    gossip({ type: 'file_req', hash, ttl: 8 });
}
// Paint any avatar for which we have (or can fetch) a blob.
async function hydrateAvatars() {
    for (const el of document.querySelectorAll('[data-avatar]')) {
        const npub = el.dataset.avatar;
        const hash = npub === state.self_npub ? myPfp() : state.users[npub]?.pfp;
        if (!hash) { el.classList.remove('has-img'); continue; }
        const url = await fileURL(hash);
        if (url) {
            let img = el.querySelector('img');
            if (!img) { img = document.createElement('img'); img.alt = ''; el.prepend(img); }
            if (img.src !== url) img.src = url;
            el.classList.add('has-img');
        } else {
            el.classList.remove('has-img');
            requestBlob(hash);
        }
    }
}
// Downscale to a small square-ish JPEG so avatars stay cheap to transfer.
async function fileToAvatarBlob(file) {
    try {
        const bitmap = await createImageBitmap(file);
        const max = 256;
        const scale = Math.min(1, max / Math.max(bitmap.width, bitmap.height));
        const w = Math.max(1, Math.round(bitmap.width * scale));
        const h = Math.max(1, Math.round(bitmap.height * scale));
        const c = document.createElement('canvas');
        c.width = w; c.height = h;
        c.getContext('2d').drawImage(bitmap, 0, 0, w, h);
        bitmap.close?.();
        const blob = await new Promise(res => c.toBlob(res, 'image/jpeg', 0.85));
        if (blob) return blob;
    } catch { /* fall back to the original file */ }
    return file;
}
async function setProfilePicture(file) {
    if (!file || !file.type.startsWith('image/')) { toast('Pick an image file'); return; }
    if (file.size > 12 * 1024 * 1024) { toast('Image is too large (max 12 MB)'); return; }
    toast('Updating profile picture…');
    const blob = await fileToAvatarBlob(file);
    const buf = await blob.arrayBuffer();
    const hash = await sha256hex(buf);
    await store.putFile({ hash, name: 'avatar.jpg', size: blob.size, channel: null, blob });
    objectURLs[hash] = URL.createObjectURL(blob);
    state.users[state.self_npub] = { ...state.users[state.self_npub], npub: state.self_npub, username: state.username, pfp: hash };
    saveProfile();
    // Announce the new picture to the room (signed), then repaint.
    gossip({ type: 'profile', username: state.username, peers: knownPeers(), joinedVia: state.joinedVia, pfp: hash });
    renderPeople();
    refreshTiles();
    hydrateAvatars();
    toast('Profile picture updated');
}
function clearProfilePicture() {
    state.users[state.self_npub] = { ...state.users[state.self_npub], npub: state.self_npub, username: state.username, pfp: null };
    saveProfile();
    gossip({ type: 'profile', username: state.username, peers: knownPeers(), joinedVia: state.joinedVia, pfp: null });
    renderPeople();
    refreshTiles();
    hydrateAvatars();
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
        // Identities are always randomly generated — never user-supplied.
        const key = bytesToHex(generateSecretKey());
        getPublicKey(hexToBytes(key));
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
        relays: resolveRelays(),
        onConnect: async (npub) => {
            addKnownPeer(npub);
            // hello is signed and carries the introducer, so the receiver can
            // verify both who we are and which npub we joined through.
            sendDirect(npub, { type: 'hello', username: state.username, peers: knownPeers(), joinedVia: state.joinedVia, pfp: myPfp() });
            // Flood our profile (incl. provenance) so the whole room converges.
            gossip({ type: 'profile', username: state.username, peers: knownPeers(), joinedVia: state.joinedVia, pfp: myPfp() });
            if (chatLog.length) sendDirect(npub, { type: 'chat_history', messages: chatLog.slice(-40) });
            if (media.joined) {
                sendDirect(npub, { type: 'media_state', mic: media.micOn, cam: media.camOn, filter: media.filter, sharing: !!media.screenStream });
                if (state.self_npub > npub) maybeOffer(npub);
            }
            renderPeople();
            hydrateAvatars();
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

    // Restore our own profile (name + picture) from this browser.
    const savedProfile = JSON.parse(localStorage.getItem('rookoo_profile') || 'null');
    state.users[state.self_npub] = { npub: state.self_npub, username: state.username, pfp: savedProfile?.pfp || null };

    // The room is entered by knowing anyone's npub. We remember the npub whose
    // invite we used (our introducer) and reconnect to the room through them
    // on future loads — no peer list to manage.
    const savedVia = localStorage.getItem('rookoo_joined_via');
    if (savedVia && savedVia !== state.self_npub) state.joinedVia = savedVia;

    // Invite links: .../#npub1… — joining is immediate and needs no setup on
    // the other side, because the library accepts unknown peers (open: true).
    if (!handleInviteFromHash() && state.joinedVia) {
        addKnownPeer(state.joinedVia);
    }
    // Pasting an invite into the address bar of an already-open app only
    // changes the fragment; catch that here.
    window.addEventListener('hashchange', handleInviteFromHash);

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
    heartbeatTick++;
    // Periodic profile gossip keeps join-provenance and the peer set converged.
    if (heartbeatTick % 12 === 0 && state.p2p.connections.size) {
        gossip({ type: 'profile', username: state.username, peers: knownPeers(), joinedVia: state.joinedVia, pfp: myPfp() });
    }
    if (media.joined) {
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
        `<div class="avatar" data-avatar="${escapeHtml(npub)}"><img alt=""><span class="initials">${escapeHtml(initials(npub))}</span></div>` +
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

    // Only render remote tiles while we are in the call; after leaving, drop
    // them and release their video element so no freeze-frame lingers.
    const present = new Set([state.self_npub]);
    if (media.joined) {
        for (const [npub, st] of Object.entries(media.peerState)) {
            if (st.joined) { ensureTile(npub); present.add(npub); }
        }
    }
    for (const npub of Object.keys(tiles)) {
        if (!present.has(npub)) {
            const v = tiles[npub].querySelector('video');
            if (v) v.srcObject = null;
            tiles[npub].remove();
            delete tiles[npub];
        }
    }

    for (const npub of present) {
        const el = tiles[npub];
        const isSelf = npub === state.self_npub;
        const st = isSelf
            ? { joined: media.joined, mic: media.micOn, cam: media.camOn, sharing: !!media.screenStream }
            : (media.peerState[npub] || {});
        const vid = el.querySelector('video');
        el.querySelector('.name').textContent = nameOf(npub) + (isSelf ? ' (you)' : '');
        const av = el.querySelector('.avatar');
        av.dataset.avatar = npub;
        const init = av.querySelector('.initials');
        if (init) init.textContent = initials(npub);

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
            <p class="muted">Click <strong>Join</strong> to turn on your camera, then share this link or your npub with anyone. Anyone who has the npub of <em>anyone in the room</em> can join — no setup on your side.</p>
            <code>${escapeHtml(link)}</code>
            <p class="muted small">Your peer ID: ${escapeHtml(state.self_npub)}</p>
        </div>`;
    } else if (empty) {
        empty.remove();
    }

    hydrateAvatars();
}

function renderPeople() {
    const list = document.getElementById('people-list');
    if (!list) return;
    const conns = state.p2p ? state.p2p.connections : new Map();
    const rows = [];
    const selfIntro = state.joinedVia ? 'joined via ' + nameOf(state.joinedVia) : 'room seed';
    const selfSub = (media.joined ? (media.micOn ? 'mic on' : 'muted') + ' · ' + (media.camOn ? 'camera on' : 'camera off') : 'not in call') + ' · ' + selfIntro;
    rows.push(`<div class="person">
        <div class="av" data-avatar="${escapeHtml(state.self_npub)}" title="Change your picture" style="cursor:pointer"><img alt=""><span class="initials">${escapeHtml(initials(state.self_npub))}</span></div>
        <div class="info"><div>${escapeHtml(nameOf(state.self_npub))} (you)</div>
        <div class="sub">${escapeHtml(selfSub)}</div></div>
        <div class="dot ${conns.size ? 'on' : ''}"></div></div>`);

    const npubs = new Set([...conns.keys(), ...Object.keys(state.users)]);
    for (const npub of npubs) {
        if (npub === state.self_npub) continue;
        const st = media.peerState[npub];
        let sub = st?.joined
            ? (st.mic ? 'mic on' : 'muted') + ' · ' + (st.cam ? 'camera on' : 'camera off')
            : (conns.has(npub) ? 'connected' : 'offline');
        const intro = introducerLabel(npub);
        if (intro) sub += ' · ' + intro;
        rows.push(`<div class="person">
            <div class="av" data-avatar="${escapeHtml(npub)}"><img alt=""><span class="initials">${escapeHtml(initials(npub))}</span></div>
            <div class="info"><div>${escapeHtml(nameOf(npub))}</div><div class="sub">${escapeHtml(sub)}</div></div>
            <div class="dot ${conns.has(npub) ? 'on' : ''}"></div></div>`);
    }
    list.innerHTML = rows.join('');
    hydrateAvatars();
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
        if (inc.silent) {
            // Fetched for an avatar (or similar): no download prompt.
            hydrateAvatars();
        } else {
            saveBlob(blob, inc.name);
            toast('Received ' + inc.name);
        }
        renderChat();
        renderFiles();
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
        // Canvas pipeline: source camera -> effects canvas -> outgoing track.
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

        // Downscaled buffer used to pixelate the background.
        media.pixelCanvas = document.createElement('canvas');
        media.pixelCanvas.width = 80;
        media.pixelCanvas.height = 45;
        media.pixelCtx = media.pixelCanvas.getContext('2d');

        // Foreground layer: camera frame masked to the person silhouette.
        media.personCanvas = document.createElement('canvas');
        media.personCanvas.width = media.canvas.width;
        media.personCanvas.height = media.canvas.height;
        media.personCtx = media.personCanvas.getContext('2d');

        // Low-res segmentation mask (filled in as MediaPipe produces masks).
        media.maskCanvas = document.createElement('canvas');
        media.maskCanvas.width = 256;
        media.maskCanvas.height = 256;
        media.maskCtx = media.maskCanvas.getContext('2d');
        media.maskImageData = media.maskCtx.createImageData(256, 256);
        media.maskReady = false;
        media.lastMaskAt = 0;

        const captured = media.canvas.captureStream(30);
        media.outVideoTrack = captured.getVideoTracks()[0];
        media.outStream = new MediaStream([media.outVideoTrack, ...media.rawStream.getAudioTracks()]);
        drawLoop();
    } else {
        media.outStream = new MediaStream([...media.rawStream.getAudioTracks()]);
    }
}

// ---------------------------------------------------- background effects ---
// Loads the selfie segmentation model on first use. Kept out of the initial
// bundle: if it can't be fetched (offline), the caller falls back to no effect.
async function ensureSegmenter() {
    if (media.segmenter) return true;
    if (media.segmenterLoading) return false;
    media.segmenterLoading = true;
    toast('Loading background effects…', 6000);
    try {
        const vision = await import(/* @vite-ignore */ MEDIAPIPE_MODULE);
        const fileset = await vision.FilesetResolver.forVisionTasks(MEDIAPIPE_WASM);
        const options = (delegate) => ({
            baseOptions: { modelAssetPath: MEDIAPIPE_MODEL, delegate },
            runningMode: 'VIDEO',
            outputConfidenceMasks: true,
            outputCategoryMask: false,
        });
        try {
            media.segmenter = await vision.ImageSegmenter.createFromOptions(fileset, options('GPU'));
        } catch {
            media.segmenter = await vision.ImageSegmenter.createFromOptions(fileset, options('CPU'));
        }
        media.segmenterLoading = false;
        toast('Background effects ready');
        return true;
    } catch (e) {
        media.segmenterLoading = false;
        console.warn('Background effects unavailable:', e);
        toast('Background effects unavailable (offline?)');
        return false;
    }
}

// Ask MediaPipe for a fresh mask, throttled to ~15 fps to leave CPU for the
// call. The callback fills maskCanvas: opaque where the person is, so it can
// be used as an alpha matte via `destination-in`.
function updateMask(now) {
    if (!media.segmenter || !media.sourceVideo || media.sourceVideo.readyState < 2) return;
    if (now - media.lastMaskAt < 66) return;
    media.lastMaskAt = now;
    try {
        media.segmenter.segmentForVideo(media.sourceVideo, now, (result) => {
            const mask = result?.confidenceMasks?.[0];
            if (mask) {
                const data = mask.getAsFloat32Array();
                const mw = mask.width || media.maskCanvas.width;
                const mh = mask.height || media.maskCanvas.height;
                if (media.maskCanvas.width !== mw || media.maskCanvas.height !== mh) {
                    media.maskCanvas.width = mw;
                    media.maskCanvas.height = mh;
                    media.maskImageData = media.maskCtx.createImageData(mw, mh);
                }
                const px = media.maskImageData.data;
                for (let i = 0; i < mw * mh; i++) {
                    const o = i * 4;
                    px[o] = 255; px[o + 1] = 255; px[o + 2] = 255;
                    // Soft ramp between two confidence thresholds gives a
                    // feathered matte instead of a hard cut-out edge.
                    let a = (data[i] - 0.3) / 0.3;
                    a = a < 0 ? 0 : a > 1 ? 1 : a;
                    px[o + 3] = (a * 255) | 0;
                }
                media.maskCtx.putImageData(media.maskImageData, 0, 0);
                media.maskReady = true;
            }
            try { result?.close?.(); } catch { /* ignore */ }
        });
    } catch { /* transient MediaPipe error; keep last mask */ }
}

// Draws a source onto a canvas, applying the mirror transform when enabled.
// Mirroring happens here (in the outgoing pipeline) so remote peers see the
// same left/right orientation as the local self-view.
function drawSource(dst, source, w, h) {
    if (media.mirror) {
        dst.save();
        dst.translate(w, 0);
        dst.scale(-1, 1);
        dst.drawImage(source, 0, 0, w, h);
        dst.restore();
    } else {
        dst.drawImage(source, 0, 0, w, h);
    }
}

function drawPixelatedBackground(ctx, video, w, h) {
    const pw = media.pixelCanvas.width, ph = media.pixelCanvas.height;
    media.pixelCtx.clearRect(0, 0, pw, ph);
    drawSource(media.pixelCtx, video, pw, ph);
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(media.pixelCanvas, 0, 0, w, h);
    ctx.imageSmoothingEnabled = true;
}

function paintBackground(ctx, video, w, h) {
    const effect = media.filter;
    if (effect === 'blur' || effect === 'blur-strong') {
        ctx.filter = effect === 'blur' ? 'blur(10px)' : 'blur(22px)';
        drawSource(ctx, video, w, h);
        ctx.filter = 'none';
    } else if (effect === 'pixelate') {
        drawPixelatedBackground(ctx, video, w, h);
    } else if (effect === 'grayscale') {
        ctx.filter = 'grayscale(1)';
        drawSource(ctx, video, w, h);
        ctx.filter = 'none';
    } else if (effect === 'bg-gradient') {
        const g = ctx.createLinearGradient(0, 0, w, h);
        g.addColorStop(0, '#7B6B8D');
        g.addColorStop(1, '#EDA35A');
        ctx.fillStyle = g;
        ctx.fillRect(0, 0, w, h);
    } else {
        ctx.fillStyle = SOLID_BG[effect] || '#20262B';
        ctx.fillRect(0, 0, w, h);
    }
}

function drawWithMask(video, w, h) {
    const { ctx } = media;
    paintBackground(ctx, video, w, h);

    // Cut the person out of the camera frame using the mask as an alpha matte.
    const pctx = media.personCtx;
    pctx.globalCompositeOperation = 'source-over';
    pctx.filter = 'none';
    pctx.clearRect(0, 0, w, h);
    drawSource(pctx, video, w, h);
    pctx.globalCompositeOperation = 'destination-in';
    // A slight blur feathers the matte edge so the cut-out isn't jagged.
    pctx.filter = 'blur(2px)';
    pctx.imageSmoothingEnabled = true;
    drawSource(pctx, media.maskCanvas, w, h);
    pctx.filter = 'none';
    pctx.globalCompositeOperation = 'source-over';

    ctx.filter = 'none';
    ctx.drawImage(media.personCanvas, 0, 0);
}

function drawLoop() {
    if (!media.ctx) return;
    const { ctx, sourceVideo, canvas } = media;
    const w = canvas.width, h = canvas.height;
    // Screen share bypasses this canvas entirely; skip the work.
    if (!media.screenStream && sourceVideo.readyState >= 2) {
        const effect = media.filter;
        const isBg = BACKGROUND_EFFECTS.has(effect);
        if (isBg && media.segmenter && media.maskReady) {
            updateMask(performance.now());
            drawWithMask(sourceVideo, w, h);
        } else {
            if (isBg && media.segmenter) updateMask(performance.now());
            ctx.filter = 'none';
            drawSource(ctx, sourceVideo, w, h);
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
    media.personCanvas = null;
    media.personCtx = null;
    media.maskCanvas = null;
    media.maskCtx = null;
    media.maskImageData = null;
    media.maskReady = false;
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
    if (!(name in FILTER_LABELS)) name = 'none';
    media.filter = name;
    if (BACKGROUND_EFFECTS.has(name)) ensureSegmenter();
    announceMedia();
    toast(FILTER_LABELS[name]);
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
        media.mirror = !media.mirror;
        toast(media.mirror ? 'Mirrored (everyone sees it)' : 'Not mirrored');
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
    document.getElementById('avatar-input').addEventListener('change', async (e) => {
        const f = e.target.files[0];
        e.target.value = '';
        if (f) await setProfilePicture(f);
    });
    // Clicking your own avatar in the People list also opens the picker.
    document.getElementById('people-list').addEventListener('click', (e) => {
        const av = e.target.closest('.av[data-avatar]');
        if (av && av.dataset.avatar === state.self_npub) document.getElementById('avatar-input').click();
    });

    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t === tab));
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.dataset.panel === tab.dataset.tab));
        });
    });
    const sidebarEl = document.getElementById('sidebar');
    const mobileMq = window.matchMedia('(max-width: 860px)');
    // Desktop starts with the panel open; mobile starts with it hidden.
    const applySidebarDefault = () => sidebarEl.classList.toggle('collapsed', mobileMq.matches);
    applySidebarDefault();
    mobileMq.addEventListener?.('change', applySidebarDefault);
    document.getElementById('sidebar-toggle').addEventListener('click', () => {
        sidebarEl.classList.toggle('collapsed');
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
        if (npub === state.self_npub) { toast('That is your own npub'); return; }
        setJoinedVia(npub);
        addKnownPeer(npub);
        dialog.close();
        document.getElementById('add-peer-input').value = '';
        toast('Joining the room through ' + shortNpub(npub) + '…');
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
    document.getElementById('settings-identity').addEventListener('click', () => {
        // Always-random identities: mint a brand new one on demand.
        localStorage.setItem('rookoo_sk', bytesToHex(generateSecretKey()));
        location.reload();
    });
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
