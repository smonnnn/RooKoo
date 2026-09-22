// RooKoo Live — tree-based, signed, peer-to-peer live video.
//
// The streamer encodes the camera with WebCodecs (VideoEncoder) and signs every
// encoded chunk with their Nostr key. Viewers verify each chunk against the
// streamer's npub before decoding — so the picture is provably from the
// original author no matter how many hops it travelled. Each viewer can forward
// the SAME signed packet to a few downstream viewers, forming a tree: the
// streamer only uploads to a handful of nodes, yet the audience grows
// exponentially while each node keeps its own traffic small.
//
// NostrP2P (the same library the rooms use) carries peer discovery and the
// signaling for the streaming data channels; the media itself never touches it.

import { NostrP2P } from './nostr-p2p.js';
import { generateSecretKey, getPublicKey, nip19, bytesToHex, hexToBytes, schnorr, sha256 } from './nostr-deps.js';

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
    } catch { /* defaults */ }
    return DEFAULT_RELAYS;
}
function iceServers() {
    const ice = [{ urls: 'stun:stun.l.google.com:19302' }];
    try {
        const t = JSON.parse(localStorage.getItem('nostr_p2p_turn') || 'null');
        if (t && t.urls) ice.push(t);
    } catch { /* ignore */ }
    return ice;
}

// Branching factor: how many downstream copies each node sends. Overridable
// via ?children=N or localStorage. A tree with branching 2–3 grows
// exponentially while each node keeps its own upload tiny.
const MAX_CHILDREN = (() => {
    const q = new URLSearchParams(location.search).get('children');
    const v = parseInt(q || localStorage.getItem('rookoo_live_children') || '3', 10);
    return v >= 1 && v <= 8 ? v : 3;
})();
const KEYFRAME_INTERVAL = 60;      // frames (~2s at 30fps)
const BUFFER_LIMIT = 4 * 1024 * 1024;
const OFFER_TIMEOUT = 25000;

// ---------------------------------------------------------------- identity --
let skHex = localStorage.getItem('rookoo_sk');
if (!skHex) {
    skHex = bytesToHex(generateSecretKey());
    localStorage.setItem('rookoo_sk', skHex);
}
const username = localStorage.getItem('rookoo_username') || 'anon';

// ------------------------------------------------------------------- crypto --
function signData(bytes) { return schnorr.sign(sha256(bytes), hexToBytes(skHex)); }
function verifyData(pubBytes, sig, bytes) { return schnorr.verify(sig, sha256(bytes), pubBytes); }

// ------------------------------------------------------------ wire protocol --
function packSigned(type, meta) {
    const metaBytes = new TextEncoder().encode(JSON.stringify(meta));
    const sig = signData(metaBytes);
    const buf = new ArrayBuffer(5 + metaBytes.length + 64);
    const u8 = new Uint8Array(buf);
    u8[0] = type;
    new DataView(buf).setUint32(1, metaBytes.length, true);
    u8.set(metaBytes, 5);
    u8.set(sig, 5 + metaBytes.length);
    return buf;
}

const PROTO = {
    // 0x01 decoder config, 0x03 stream info — both signed over their metadata.
    packConfig(cfg) { return packSigned(0x01, cfg); },
    packInfo(info) { return packSigned(0x03, info); },
    packChunk(meta, data, isKey) {
        const metaBytes = new TextEncoder().encode(JSON.stringify(meta));
        const dataBytes = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
        const toSign = new Uint8Array(metaBytes.length + dataBytes.length);
        toSign.set(metaBytes, 0);
        toSign.set(dataBytes, metaBytes.length);
        const sig = signData(toSign);
        const buf = new ArrayBuffer(5 + metaBytes.length + 64 + dataBytes.length);
        const u8 = new Uint8Array(buf);
        // 0x02 = delta chunk, 0x12 = key chunk (so hops can protect keyframes
        // from being dropped under backpressure without parsing the metadata).
        u8[0] = isKey ? 0x12 : 0x02;
        new DataView(buf).setUint32(1, metaBytes.length, true);
        u8.set(metaBytes, 5);
        u8.set(sig, 5 + metaBytes.length);
        u8.set(dataBytes, 5 + metaBytes.length + 64);
        return buf;
    },
    unpack(buf) {
        const u8 = new Uint8Array(buf);
        const type = u8[0];
        const metaLen = new DataView(buf).getUint32(1, true);
        const meta = JSON.parse(new TextDecoder().decode(u8.slice(5, 5 + metaLen)));
        const sig = u8.slice(5 + metaLen, 5 + metaLen + 64);
        const data = u8.slice(5 + metaLen + 64);
        return { type, meta, sig, data };
    },
    packControl(cmd, payload) {
        const json = new TextEncoder().encode(JSON.stringify({ cmd, payload }));
        const buf = new ArrayBuffer(5 + json.length);
        const u8 = new Uint8Array(buf);
        u8[0] = 0x10;
        new DataView(buf).setUint32(1, json.length, true);
        u8.set(json, 5);
        return buf;
    },
    unpackControl(buf) {
        const len = new DataView(buf).getUint32(1, true);
        return JSON.parse(new TextDecoder().decode(new Uint8Array(buf).slice(5, 5 + len)));
    },
};

// ---------------------------------------------------------------- elements --
const $ = (id) => document.getElementById(id);
const localVideo = $('local-video');
const remoteCanvas = $('remote-canvas');
const stageVideo = $('stage-video');
const stageHint = $('stage-hint');
const ctx = remoteCanvas.getContext('2d');

// Phones (iOS) pause a <video> when leaving native fullscreen, which would
// freeze the camera preview — and the stream — so always nudge it back to play.
function resumeVideos() {
    for (const v of [localVideo, stageVideo]) {
        if (v.srcObject && v.paused) v.play().catch(() => {});
    }
}
for (const v of [localVideo, stageVideo]) {
    v.addEventListener('pause', () => { if (v.srcObject) v.play().catch(() => {}); });
    v.addEventListener('webkitendfullscreen', resumeVideos);
}
for (const evt of ['fullscreenchange', 'webkitfullscreenchange', 'webkitpresentationmodechanged']) {
    document.addEventListener(evt, resumeVideos);
}
document.addEventListener('visibilitychange', () => { if (!document.hidden) resumeVideos(); });

// ------------------------------------------------------------------- state --
const L = {
    npub: null,
    mode: 'idle',            // idle | streamer | viewer
    streamerNpub: null,
    streamerPub: null,
    relayEnabled: true,
    children: new Map(),     // npub -> { pc, channel, capacity, current }
    relays: new Map(),       // npub -> { capacity, current } (children that relay)
    upstream: null,          // { npub, pc, channel, role }
    admission: null,         // { sid, resolve, reject, timer }
    pendingOffer: null,      // { sid, pc }
    cachedConfig: null,
    decoder: null,
    decoderReady: false,
    hasKeyframe: false,
    forceKeyframe: false,
    frames: 0,
    connectWaiters: new Map(),
    rejoinTimer: null,
    rejoinAttempts: 0,
    info: null,              // { title, username, startedAt, viewers }
    infoPacket: null,        // last signed info packet (sent to new children)
    title: '',
    startedAt: 0,
    lastInfoAt: 0,
    tick: null,
    paused: false,
    deviceId: null,
    lastPacketAt: 0,
    lastKeyReq: 0,
    latency: 0,
};

function log(...a) { $('live-log').textContent = a.join(' '); console.log('[live]', ...a); }
function setStatus(text) { $('live-status').textContent = text; }
// Full-bleed the video area while a feed is active (see live.css).
function setActiveFeed(on) { $('live-app').classList.toggle('active', !!on); }
function updateStats() {
    $('st-role').textContent = L.mode;
    $('st-source').textContent = L.streamerNpub ? L.streamerNpub.slice(0, 14) + '…' : '—';
    $('st-upstream').textContent = L.upstream ? L.upstream.npub.slice(0, 14) + '…' : '—';
    $('st-down').textContent = String(L.children.size);
    $('st-lat').textContent = (L.mode === 'viewer' && L.latency) ? Math.round(L.latency) + ' ms' : '—';
    $('st-frames').textContent = String(L.frames);
}
function setSig(ok) {
    const el = $('st-sig');
    el.textContent = ok ? '✓ valid' : '✗ invalid';
    el.style.color = ok ? 'var(--ok)' : 'var(--danger)';
}
function rid() { return Math.random().toString(36).slice(2, 10); }

// --------------------------------------------------------------- transport --
const p2p = new NostrP2P(skHex, {
    open: true,
    maxConnections: 16,
    minConnectionAge: 24 * 60 * 60 * 1000,
    relays: resolveRelays(),
    onConnect: (npub) => {
        const ws = L.connectWaiters.get(npub);
        if (ws) { ws.forEach((r) => r()); L.connectWaiters.delete(npub); }
        log('connected', npub.slice(0, 14) + '…');
    },
    onMessage: (from, msg) => { handleSignal(from, msg).catch((e) => log('signal error', e.message)); },
    onDisconnect: (npub) => { if (L.upstream?.npub === npub) upstreamClosed(); },
});
L.npub = p2p.npub;
p2p.peers.add(L.npub);

function ensurePeer(npub) {
    if (npub === L.npub) return Promise.resolve();
    if (p2p.isConnected(npub)) return Promise.resolve();
    p2p.addPeer(npub);
    return new Promise((resolve, reject) => {
        const arr = L.connectWaiters.get(npub) || [];
        arr.push(resolve);
        L.connectWaiters.set(npub, arr);
        setTimeout(() => { if (!p2p.isConnected(npub)) reject(new Error('connect timeout')); }, 15000);
    });
}
function sendTo(npub, msg) {
    try { p2p.send(npub, msg); } catch (e) { log('send failed', e.message); }
}

// ------------------------------------------------------------ signal plane --
async function handleSignal(from, msg) {
    if (!msg || !msg.type) return;
    switch (msg.type) {
        case 'live_join': return handleJoin(from, msg);
        case 'live_offer': return acceptOffer(from, msg.sid, msg.sdp);
        case 'live_answer': return handleAnswer(from, msg);
        case 'live_approve': return resolveAdmission(from, msg, 'approve');
        case 'live_redirect': return resolveAdmission(from, msg, 'redirect');
        case 'live_reject': return resolveAdmission(from, msg, 'reject');
        case 'live_end': return endStream();
    }
}

function handleJoin(from, msg) {
    if (L.mode === 'idle') return;
    if (L.mode === 'viewer' && !L.relayEnabled) return;
    if (L.children.size < MAX_CHILDREN) {
        sendTo(from, { type: 'live_approve', sid: msg.sid });
    } else {
        const cand = [...L.relays.entries()].find(([, i]) => (i.current || 0) < (i.capacity || 0));
        if (cand) sendTo(from, { type: 'live_redirect', sid: msg.sid, npub: cand[0] });
        else sendTo(from, { type: 'live_reject', sid: msg.sid, reason: 'capacity' });
    }
}

function resolveAdmission(from, msg, kind) {
    const a = L.admission;
    if (!a || a.sid !== msg.sid) return;
    clearTimeout(a.timer);
    L.admission = null;
    if (kind === 'approve') a.resolve({ type: 'approve', target: from });
    else if (kind === 'redirect') a.resolve({ type: 'redirect', target: msg.npub });
    else a.reject(new Error(msg.reason || 'rejected'));
}

function requestAdmission(target, sid) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { if (L.admission?.sid === sid) { L.admission = null; reject(new Error('no response')); } }, OFFER_TIMEOUT);
        L.admission = { sid, resolve, reject, timer };
        sendTo(target, { type: 'live_join', sid });
    });
}

function handleAnswer(from, msg) {
    const p = L.pendingOffer;
    if (!p || p.sid !== msg.sid) return;
    const pc = p.pc;
    L.pendingOffer = null;
    pc.setRemoteDescription(new RTCSessionDescription(msg.sdp)).catch((e) => log('answer error', e.message));
}

// A node accepting a downstream viewer: create the pc, answer, and wire the
// incoming data channel to forward signed packets.
async function acceptOffer(from, sid, sdp) {
    if (L.mode === 'idle') return;
    if (L.mode === 'viewer' && !L.relayEnabled) return;
    if (L.children.size >= MAX_CHILDREN) return;
    const pc = new RTCPeerConnection({ iceServers: iceServers() });
    pc.ondatachannel = (e) => {
        const ch = e.channel;
        ch.binaryType = 'arraybuffer';
        ch.onopen = () => {
            L.children.set(from, { pc, channel: ch, capacity: 0, current: 0, subtree: 1 });
            updateStats();
            log('downstream', from.slice(0, 14) + '…');
            if (L.cachedConfig) try { ch.send(L.cachedConfig); } catch { /* ignore */ }
            if (L.infoPacket) try { ch.send(L.infoPacket); } catch { /* ignore */ }
            requestKeyframeUpstream();
            if (L.mode === 'viewer') {
                try { ch.send(PROTO.packControl('relay-capacity', { capacity: L.relayEnabled ? MAX_CHILDREN : 0, subtree: subtreeSize(), npub: L.npub })); } catch { /* ignore */ }
                notifyUpstreamCapacity();
            } else {
                sendInfo(true);
            }
        };
        ch.onclose = () => { L.children.delete(from); updateStats(); notifyUpstreamCapacity(); sendInfo(true); };
        ch.onmessage = (ev) => handleChildMessage(from, ev.data);
    };
    pc.onconnectionstatechange = () => {
        if (['failed', 'closed'].includes(pc.connectionState)) { L.children.delete(from); updateStats(); }
    };
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    await iceComplete(pc);
    sendTo(from, { type: 'live_answer', sid, sdp: { type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
}

function handleChildMessage(from, data) {
    const u8 = new Uint8Array(data);
    if (u8[0] !== 0x10) return;
    let ctl;
    try { ctl = PROTO.unpackControl(data); } catch { return; }
    const child = L.children.get(from);
    if (ctl.cmd === 'relay-capacity') {
        L.relays.set(from, { capacity: ctl.payload.capacity || 0, current: 0 });
        if (child) child.subtree = ctl.payload.subtree || 1;
        onSubtreeChange();
    } else if (ctl.cmd === 'relay-update') {
        const info = L.relays.get(from) || { capacity: 0, current: 0 };
        info.current = ctl.payload.current || 0;
        L.relays.set(from, info);
        if (child) child.subtree = ctl.payload.subtree || 1;
        onSubtreeChange();
    } else if (ctl.cmd === 'request-keyframe') {
        requestKeyframeUpstream();
    } else if (ctl.cmd === 'request-config') {
        if (L.cachedConfig && child?.channel?.readyState === 'open') {
            try { child.channel.send(L.cachedConfig); } catch { /* ignore */ }
        }
    } else if (ctl.cmd === 'request-info') {
        if (L.infoPacket && child?.channel?.readyState === 'open') {
            try { child.channel.send(L.infoPacket); } catch { /* ignore */ }
        }
    }
}

// A child's subtree size changed: propagate it toward the streamer.
function onSubtreeChange() {
    if (L.mode === 'viewer') notifyUpstreamCapacity();
    else if (L.mode === 'streamer') sendInfo(true);
    updateStreamInfoUI();
}

function requestKeyframeUpstream() {
    if (L.mode === 'streamer') { L.forceKeyframe = true; return; }
    sendControlUpstream('request-keyframe', {});
}
function sendControlUpstream(cmd, payload) {
    if (L.upstream?.channel?.readyState === 'open') {
        try { L.upstream.channel.send(PROTO.packControl(cmd, payload)); } catch { /* ignore */ }
    }
}
function notifyUpstreamCapacity() {
    if (L.mode !== 'viewer') return;
    sendControlUpstream('relay-update', { current: L.children.size, capacity: L.relayEnabled ? MAX_CHILDREN : 0, subtree: subtreeSize(), npub: L.npub });
}
function forwardToChildren(buf) {
    // Config (0x01) and keyframes (0x12) are always forwarded so a congested
    // child can resync; delta chunks are dropped while a child is backed up.
    const important = [0x01, 0x12].includes(new Uint8Array(buf)[0]);
    for (const [, c] of L.children) {
        const ch = c.channel;
        if (!ch || ch.readyState !== 'open') continue;
        if (!important && ch.bufferedAmount > BUFFER_LIMIT) continue;
        try { ch.send(buf); } catch { /* ignore */ }
    }
}

// ---- stream metadata: title, streamer name, watchers, uptime ----
function childSubtree(c) { return c.subtree || 1; }
function subtreeSize() { let n = 1; for (const [, c] of L.children) n += childSubtree(c); return n; }
function totalWatchers() { let n = 0; for (const [, c] of L.children) n += childSubtree(c); return n; }
function fmtDuration(ms) {
    const s = Math.max(0, Math.floor(ms / 1000));
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
    const p = (x) => String(x).padStart(2, '0');
    return h ? `${h}:${p(m)}:${p(sec)}` : `${p(m)}:${p(sec)}`;
}
function updateStreamInfoUI() {
    const info = L.info || {};
    const active = L.mode === 'streamer' || (L.mode === 'viewer' && (L.frames > 0 || L.upstream));
    $('si-title').textContent = active ? (info.title || L.title || 'Live') : 'No stream';
    $('si-user').textContent = active
        ? (info.username ? '@' + info.username : (L.streamerNpub ? '@' + L.streamerNpub.slice(0, 10) + '…' : ''))
        : '';
    const viewers = L.mode === 'streamer' ? totalWatchers() : (info.viewers || 0);
    $('si-watchers').textContent = viewers + ' watching';
    const started = info.startedAt || L.startedAt || 0;
    $('si-uptime').textContent = active && started ? fmtDuration(Date.now() - started) : '';
}
function startTick() {
    if (L.tick) return;
    let n = 0;
    L.tick = setInterval(() => {
        updateStreamInfoUI();
        n++;
        if (L.mode === 'streamer' && n % 3 === 0) sendInfo();
        // Viewer watchdog: nudge a stalled upstream, then reconnect if dead.
        if (L.mode === 'viewer' && L.upstream) {
            const idle = Date.now() - (L.lastPacketAt || Date.now());
            if (idle > 12000) {
                log('stream stalled — reconnecting');
                upstreamClosed();
            } else if (idle > 5000 && Date.now() - L.lastKeyReq > 3000) {
                L.lastKeyReq = Date.now();
                requestConfigUpstream();
                requestKeyframeUpstream();
            }
        }
    }, 1000);
}
// Streamer: (re)build the signed info packet and push it to the tree.
function sendInfo(force = false) {
    if (L.mode !== 'streamer') return;
    const now = Date.now();
    if (!force && now - L.lastInfoAt < 1000) return;
    L.lastInfoAt = now;
    L.info = { title: L.title, username, startedAt: L.info?.startedAt || L.startedAt || now, viewers: totalWatchers() };
    L.infoPacket = PROTO.packInfo(L.info);
    forwardToChildren(L.infoPacket);
    updateStreamInfoUI();
}
function iceComplete(pc, maxMs = 6000) {
    return new Promise((resolve) => {
        if (pc.iceGatheringState === 'complete') return resolve();
        const t = setTimeout(resolve, maxMs);
        pc.addEventListener('icegatheringstatechange', () => {
            if (pc.iceGatheringState === 'complete') { clearTimeout(t); resolve(); }
        });
    });
}

// ---------------------------------------------------------------- streamer --
let encoder = null, localStream = null, localScreen = null, encodeCfg = null, cameraCfg = null, frameCount = 0;

// Pick an even width/height preserving the source aspect, long side <= cap.
function fitDims(vw, vh, cap) {
    const long = Math.max(vw || cap, vh || Math.round(cap * 9 / 16));
    const scale = Math.min(1, cap / long);
    return {
        w: Math.max(2, Math.round((vw * scale) / 2) * 2),
        h: Math.max(2, Math.round((vh * scale) / 2) * 2),
    };
}

async function pickCodec(width, height) {
    // H.264 first for the widest support (Safari encodes H.264 only), then VP9/VP8.
    const candidates = [
        'avc1.640028', 'avc1.4d0028', 'avc1.42E01E',
        'vp09.00.10.08', 'vp8',
    ];
    const bitrate = Math.min(2_500_000, Math.max(600_000, Math.round(width * height * 2.2)));
    for (const codec of candidates) {
        const cfg = { codec, width, height, bitrate, framerate: 30, latencyMode: 'realtime' };
        let t;
        try { t = new VideoEncoder({ output: () => {}, error: () => {} }); await t.configure(cfg); t.close(); return cfg; }
        catch { try { t?.close(); } catch { /* ignore */ } }
    }
    return null;
}

// Source dimensions (camera/screen) from the live video element.
function sourceDims() {
    return { w: localVideo.videoWidth || 1280, h: localVideo.videoHeight || 720 };
}

// Video input constraints for the selected source (or the default camera).
function videoConstraints() {
    const base = { width: { ideal: 640 }, height: { ideal: 360 }, frameRate: { ideal: 30 } };
    return L.deviceId ? { deviceId: { exact: L.deviceId }, ...base } : base;
}

// List video inputs — including "OBS Virtual Camera" once OBS is running it.
async function listCameras() {
    const sel = $('camera-select');
    if (!sel || !navigator.mediaDevices?.enumerateDevices) return;
    let devices = [];
    try { devices = await navigator.mediaDevices.enumerateDevices(); } catch { return; }
    const cams = devices.filter((d) => d.kind === 'videoinput');
    const current = L.deviceId || sel.value || '';
    sel.innerHTML = '<option value="">Default camera</option>';
    cams.forEach((d, i) => {
        const o = document.createElement('option');
        o.value = d.deviceId;
        o.textContent = d.label || ('Camera ' + (i + 1));
        sel.appendChild(o);
    });
    if (current && [...sel.options].some((o) => o.value === current)) sel.value = current;
    else sel.value = '';
}

// Swap the camera while live (keeps the encoder and viewers).
async function switchCamera(deviceId) {
    if (L.mode !== 'streamer' || localScreen) return;
    try {
        const s = await navigator.mediaDevices.getUserMedia({ video: videoConstraints(), audio: false });
        if (localStream) localStream.getTracks().forEach((t) => t.stop());
        localStream = s;
        localVideo.srcObject = s;
        await localVideo.play().catch(() => {});
        if (localVideo.videoWidth) {
            const d = fitDims(localVideo.videoWidth, localVideo.videoHeight, 720);
            const cfg = await pickCodec(d.w, d.h);
            if (cfg && encoder) { encodeCfg = cfg; cameraCfg = cfg; await encoder.configure(cfg).catch(() => {}); }
        }
        startReader();
        L.forceKeyframe = true;
        log('video source switched');
    } catch (e) { log('source switch failed: ' + e.message); }
}

async function goLive() {
    if (typeof VideoEncoder === 'undefined' || typeof VideoFrame === 'undefined') {
        log('This browser can’t encode video (WebCodecs needed: Chrome/Edge/Android).');
        return;
    }
    try {
        localStream = await navigator.mediaDevices.getUserMedia({ video: videoConstraints(), audio: false });
    } catch (e) { log('camera error: ' + e.message); return; }

    listCameras();
    localVideo.srcObject = localStream;
    localVideo.hidden = false;
    // Show our own camera in the main stage so the broadcaster can see it too.
    stageVideo.srcObject = localStream;
    stageVideo.hidden = false;
    stageHint.style.display = 'none';
    await localVideo.play().catch(() => {});
    if (!localVideo.videoWidth) await new Promise((r) => localVideo.addEventListener('loadedmetadata', r, { once: true }));
    const d = fitDims(localVideo.videoWidth || 1280, localVideo.videoHeight || 720, 720);
    const cfg = await pickCodec(d.w, d.h);
    if (!cfg) { log('no supported video encoder (VP8/VP9/H.264)'); return; }
    log('encoding ' + cfg.codec + ' ' + cfg.width + '×' + cfg.height);

    L.mode = 'streamer';
    L.streamerNpub = L.npub;
    L.streamerPub = hexToBytes(getPublicKey(hexToBytes(skHex)));
    L.title = ($('stream-title').value || '').trim() || 'Live';
    L.startedAt = Date.now();
    L.info = { title: L.title, username, startedAt: L.startedAt, viewers: 0 };
    L.infoPacket = PROTO.packInfo(L.info);
    startTick();
    updateStreamInfoUI();
    setLiveBadge(true);
    setActiveFeed(true);
    setControlsEnabled(true);
    vcPlay.hidden = true; // pausing your own outgoing stream isn't meaningful
    setStatus('live');
    $('go-live').disabled = true;
    $('stop-live').disabled = false;
    if (navigator.mediaDevices?.getDisplayMedia) $('screen-live').disabled = false;
    showShareLink();
    updateStats();

    encoder = new VideoEncoder({
        output: (chunk, metadata) => {
            if (metadata?.decoderConfig) {
                const c = { ...metadata.decoderConfig };
                // description may be a Uint8Array, ArrayBuffer or view — make it
                // a plain array so it survives JSON (H.264 needs it).
                if (c.description) {
                    if (c.description instanceof ArrayBuffer) c.description = Array.from(new Uint8Array(c.description));
                    else if (ArrayBuffer.isView(c.description)) c.description = Array.from(new Uint8Array(c.description.buffer, c.description.byteOffset, c.description.byteLength));
                }
                L.cachedConfig = PROTO.packConfig(c);
                forwardToChildren(L.cachedConfig);
            }
            if (!L.children.size) return;
            const meta = { type: chunk.type, timestamp: chunk.timestamp, duration: chunk.duration || 0, wall: Date.now() };
            const data = new Uint8Array(chunk.byteLength);
            chunk.copyTo(data);
            const isKey = chunk.type === 'key';
            const packet = PROTO.packChunk(meta, data, isKey);
            for (const [, child] of L.children) {
                if (child.channel?.readyState !== 'open') continue;
                if (!isKey && child.channel.bufferedAmount > BUFFER_LIMIT) continue;
                try { child.channel.send(packet); } catch { /* ignore */ }
            }
        },
        error: (e) => log('encoder error: ' + e.message),
    });
    encodeCfg = cfg;
    cameraCfg = cfg;
    await encoder.configure(cfg);
    startReader();
}

// Feed the encoder from the current source. Prefer MediaStreamTrackProcessor
// (zero-copy, lowest latency) where it exists (Chrome/Edge); otherwise fall
// back to a canvas + VideoFrame loop (Safari), which is a bit heavier but the
// only option there.
let readerGen = 0;
function startReader() {
    if (typeof MediaStreamTrackProcessor !== 'undefined') startProcessorReader();
    else startCanvasReader();
}

function encodeFrameOrDrop(frame, n) {
    if (encoder?.state !== 'configured') { try { frame.close(); } catch { /* ignore */ } return; }
    const key = L.forceKeyframe || n % KEYFRAME_INTERVAL === 0;
    if (key) L.forceKeyframe = false;
    if (key || encoder.encodeQueueSize <= 6) {
        try { encoder.encode(frame, key ? { keyFrame: true } : undefined); } catch { /* ignore */ }
    }
    try { frame.close(); } catch { /* ignore */ }
}

function startProcessorReader() {
    const gen = ++readerGen;
    const stream = localVideo.srcObject;
    const track = stream && stream.getVideoTracks ? stream.getVideoTracks()[0] : null;
    if (!track) return;
    const processor = new MediaStreamTrackProcessor({ track });
    const reader = processor.readable.getReader();
    let n = frameCount;
    (async () => {
        try {
            while (gen === readerGen) {
                const { done, value: frame } = await reader.read();
                if (done) break;
                n++; frameCount = n;
                encodeFrameOrDrop(frame, n);
            }
        } catch (e) { if (e.name !== 'AbortError') console.warn('[live] reader', e); }
    })();
}

function startCanvasReader() {
    const gen = ++readerGen;
    const src = localVideo;
    const cvs = document.createElement('canvas');
    const c = cvs.getContext('2d', { alpha: false });
    let n = frameCount;
    const schedule = () => {
        if (gen !== readerGen) return;
        if (typeof src.requestVideoFrameCallback === 'function') src.requestVideoFrameCallback(step);
        else setTimeout(step, 1000 / 30);
    };
    const step = () => {
        if (gen !== readerGen) return;
        if (encoder?.state === 'configured' && encodeCfg && src.videoWidth) {
            if (cvs.width !== encodeCfg.width || cvs.height !== encodeCfg.height) {
                cvs.width = encodeCfg.width;
                cvs.height = encodeCfg.height;
            }
            try { c.drawImage(src, 0, 0, cvs.width, cvs.height); } catch { /* not ready */ }
            n++; frameCount = n;
            try { encodeFrameOrDrop(new VideoFrame(cvs, { timestamp: Math.round(performance.now() * 1000) }), n); }
            catch { /* ignore */ }
        }
        schedule();
    };
    schedule();
}

// Broadcast the screen instead of the camera. The signed-chunk protocol is
// unchanged, so provenance and forwarding keep working; viewers just get a new
// decoder config when the resolution changes.
async function shareScreen() {
    if (L.mode !== 'streamer' || localScreen) return;
    if (!navigator.mediaDevices?.getDisplayMedia) { log('screen sharing not supported here'); return; }
    try {
        localScreen = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
    } catch { return; }
    const track = localScreen.getVideoTracks()[0];
    track.onended = () => stopScreenShare();
    localVideo.srcObject = localScreen;
    stageVideo.srcObject = localScreen;
    await localVideo.play().catch(() => {});
    if (!localVideo.videoWidth) await new Promise((r) => localVideo.addEventListener('loadedmetadata', r, { once: true }));
    const sd = localScreen.getVideoTracks()[0].getSettings?.() || {};
    const d = fitDims(sd.width || localVideo.videoWidth || 1280, sd.height || localVideo.videoHeight || 720, 1280);
    const cfg = { ...encodeCfg, width: d.w, height: d.h };
    try { await encoder.configure(cfg); encodeCfg = cfg; } catch { /* keep current */ }
    startReader();
    L.forceKeyframe = true;
    $('screen-live').textContent = 'Stop screen';
    $('screen-live').classList.add('off');
    log('sharing screen');
}

async function stopScreenShare() {
    const screen = localScreen;
    localScreen = null;
    if (screen) {
        // Detach onended first so stopping doesn't re-enter this function.
        screen.getTracks().forEach((t) => { t.onended = null; t.stop(); });
    }
    if (L.mode === 'streamer' && encoder && localStream) {
        localVideo.srcObject = localStream;
        stageVideo.srcObject = localStream;
        const cfg = cameraCfg || encodeCfg;
        try { await encoder.configure(cfg); encodeCfg = cfg; } catch { /* ignore */ }
        startReader();
        L.forceKeyframe = true;
    }
    const btn = $('screen-live');
    if (btn) { btn.textContent = 'Share screen'; btn.classList.remove('off'); }
    log('screen stopped');
}

function stopLive() {
    readerGen++;
    if (localScreen) { localScreen.getTracks().forEach((t) => t.stop()); localScreen = null; }
    if (encoder) { try { encoder.close(); } catch { /* ignore */ } encoder = null; }
    if (localStream) { localStream.getTracks().forEach((t) => t.stop()); localStream = null; }
    localVideo.srcObject = null;
    localVideo.hidden = true;
    stageVideo.srcObject = null;
    stageVideo.hidden = true;
    stageHint.style.display = '';
    stageHint.textContent = 'Nothing playing yet';
    $('screen-live').textContent = 'Share screen';
    $('screen-live').classList.remove('off');
    $('screen-live').disabled = true;
    // Tell viewers the stream is over so they clear the picture (and relay it
    // on to their own downstreams) instead of showing a frozen frame.
    for (const [npub] of L.children) { try { p2p.send(npub, { type: 'live_end' }); } catch { /* ignore */ } }
    for (const [, c] of L.children) { try { c.pc.close(); } catch { /* ignore */ } }
    L.children.clear();
    L.relays.clear();
    L.cachedConfig = null;
    L.mode = 'idle';
    L.streamerNpub = null;
    L.info = null;
    L.infoPacket = null;
    L.title = '';
    L.startedAt = 0;
    $('go-live').disabled = false;
    $('stop-live').disabled = true;
    $('share-row').hidden = true;
    setStatus('idle');
    updateStats();
    updateStreamInfoUI();
    setLiveBadge(false);
    setControlsEnabled(false);
    setActiveFeed(false);
    vcPlay.hidden = false;
}

function showShareLink() {
    const link = location.href.split('#')[0] + '#' + L.npub;
    $('share-link').textContent = link;
    $('share-row').hidden = false;
}

// ----------------------------------------------------------- viewer / relay --
async function watch(streamerNpub) {
    if (!/^npub1/.test(streamerNpub)) { log('not a valid streamer ID'); return; }
    L.relayEnabled = $('relay-check').checked;
    L.streamerNpub = streamerNpub;
    try { L.streamerPub = hexToBytes(nip19.decode(streamerNpub).data); } catch { log('bad npub'); return; }
    L.mode = 'viewer';
    L.frames = 0;
    L.rejoinAttempts = 0;
    resetDecoder();
    setStatus('joining…');
    $('watch-btn').disabled = true;
    $('leave-btn').disabled = false;
    stageHint.textContent = 'Connecting…';
    updateStats();
    attemptJoin(streamerNpub);
}

async function attemptJoin(streamerNpub) {
    if (L.mode !== 'viewer') return;
    let target = streamerNpub;
    for (let hop = 0; hop < 4; hop++) {
        try {
            await ensurePeer(target);
            const res = await requestAdmission(target, rid());
            if (res.type === 'approve') { await connectUpstream(target, hop === 0 ? 'streamer' : 'relay'); return; }
            if (res.type === 'redirect') { target = res.target; continue; }
        } catch (e) {
            log('join failed: ' + e.message);
            scheduleRejoin(streamerNpub);
            return;
        }
    }
    log('too many redirects');
    scheduleRejoin(streamerNpub);
}

function scheduleRejoin(streamerNpub) {
    if (L.mode !== 'viewer') return;
    L.rejoinAttempts = (L.rejoinAttempts || 0) + 1;
    if (L.rejoinAttempts > 6) { log('giving up'); leaveWatch(); return; }
    const delay = Math.min(1500 * L.rejoinAttempts, 8000);
    log('retrying in ' + Math.round(delay / 1000) + 's');
    setStatus('reconnecting…');
    clearTimeout(L.rejoinTimer);
    L.rejoinTimer = setTimeout(() => { if (L.mode === 'viewer' && !L.upstream) attemptJoin(streamerNpub); }, delay);
}

async function connectUpstream(target, role) {
    const pc = new RTCPeerConnection({ iceServers: iceServers() });
    const ch = pc.createDataChannel('stream', { ordered: true });
    ch.binaryType = 'arraybuffer';
    ch.onopen = () => {
        L.upstream = { npub: target, pc, channel: ch, role };
        L.lastPacketAt = Date.now();
        setStatus('live');
        stageHint.textContent = role === 'streamer' ? 'Direct from streamer' : 'Relayed';
        updateStats();
        if (L.relayEnabled) {
            try { ch.send(PROTO.packControl('relay-capacity', { capacity: MAX_CHILDREN, subtree: subtreeSize(), npub: L.npub })); } catch { /* ignore */ }
        }
        try { ch.send(PROTO.packControl('request-config', {})); } catch { /* ignore */ }
        try { ch.send(PROTO.packControl('request-info', {})); } catch { /* ignore */ }
    };
    ch.onmessage = (ev) => onUpstreamData(ev.data);
    ch.onclose = () => upstreamClosed();
    pc.onconnectionstatechange = () => { if (['failed', 'closed'].includes(pc.connectionState)) upstreamClosed(); };

    const sid = rid();
    L.pendingOffer = { sid, pc };
    await pc.setLocalDescription(await pc.createOffer());
    await iceComplete(pc);
    sendTo(target, { type: 'live_offer', sid, sdp: { type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
}

function upstreamClosed() {
    if (L.mode !== 'viewer') return;
    if (L.upstream) { try { L.upstream.pc.close(); } catch { /* ignore */ } L.upstream = null; }
    resetDecoder();
    clearVideo('Reconnecting…');
    setStatus('reconnecting…');
    updateStats();
    if (L.streamerNpub) scheduleRejoin(L.streamerNpub);
}

// Clear the picture and disable the controls (used when the stream ends or
// while reconnecting, so a stale frame never lingers).
function clearVideo(hint) {
    remoteCanvas.hidden = true;
    ctx.clearRect(0, 0, remoteCanvas.width, remoteCanvas.height);
    stageHint.style.display = '';
    stageHint.textContent = hint || 'Nothing playing yet';
    L.paused = false;
    setControlsEnabled(false);
    setLiveBadge(false);
    vcPlay.hidden = false;
    vcPlay.textContent = '⏸';
    vcPlay.title = 'Pause (keeps relaying)';
    if (document.pictureInPictureElement) document.exitPictureInPicture().catch(() => {});
}

// The streamer stopped (explicit end message): clear and don't keep retrying.
function endStream() {
    clearTimeout(L.rejoinTimer);
    if (L.upstream) { try { L.upstream.pc.close(); } catch { /* ignore */ } L.upstream = null; }
    for (const [npub] of L.children) { try { p2p.send(npub, { type: 'live_end' }); } catch { /* ignore */ } }
    for (const [, c] of L.children) { try { c.pc.close(); } catch { /* ignore */ } }
    L.children.clear();
    L.relays.clear();
    resetDecoder();
    clearVideo('Stream ended');
    setActiveFeed(false);
    L.mode = 'idle';
    L.frames = 0;
    L.info = null;
    L.infoPacket = null;
    setStatus('ended');
    $('watch-btn').disabled = false;
    $('leave-btn').disabled = true;
    setSig(null);
    $('st-sig').textContent = '—';
    updateStats();
    updateStreamInfoUI();
}

function onUpstreamData(data) {
    L.lastPacketAt = Date.now();
    const u8 = new Uint8Array(data);
    if (u8[0] === 0x10) {
        let ctl;
        try { ctl = PROTO.unpackControl(data); } catch { return; }
        if (ctl.cmd === 'request-config') {
            if (L.cachedConfig && L.upstream?.channel?.readyState === 'open') {
                try { L.upstream.channel.send(L.cachedConfig); } catch { /* ignore */ }
            }
        } else if (ctl.cmd === 'request-keyframe') {
            requestKeyframeUpstream();
        }
        return;
    }
    let packet;
    try { packet = PROTO.unpack(data); } catch { return; }
    const metaBytes = new TextEncoder().encode(JSON.stringify(packet.meta));
    const toVerify = new Uint8Array(metaBytes.length + packet.data.length);
    toVerify.set(metaBytes, 0);
    toVerify.set(packet.data, metaBytes.length);
    const valid = verifyData(L.streamerPub, packet.sig, toVerify);
    setSig(valid);
    if (!valid) { console.warn('[live] bad signature, dropping packet'); return; }

    if (packet.type === 0x01) {
        L.cachedConfig = data;
        configureDecoder(packet.meta);
    } else if (packet.type === 0x03) {
        L.info = packet.meta;
        L.infoPacket = data;
        updateStreamInfoUI();
    } else if (packet.type === 0x02 || packet.type === 0x12) {
        if (packet.meta.wall) L.latency = Math.max(0, Date.now() - packet.meta.wall);
        if (packet.meta.type === 'key') L.hasKeyframe = true;
        // While paused we skip decoding (saving CPU) but keep relaying the raw
        // signed packets, so downstream still gets the latest stream. The
        // canvas keeps the last frame; resuming asks for a fresh keyframe.
        if (L.paused) {
            // relay only
        } else if (!L.hasKeyframe || !L.decoder) {
            // waiting for a keyframe to (re)start decoding
        } else if (L.decoder.decodeQueueSize > 12) {
            // Fell too far behind: reset and resync from a fresh keyframe.
            console.warn('[live] decoder behind — resyncing');
            decoderReset();
            requestConfigUpstream();
            requestKeyframeUpstream();
        } else {
            try {
                L.decoder.decode(new EncodedVideoChunk({
                    type: packet.meta.type,
                    timestamp: packet.meta.timestamp,
                    duration: packet.meta.duration || 0,
                    data: packet.data,
                }));
            } catch (e) {
                console.warn('[live] decode error', e);
                decoderReset();
                requestConfigUpstream();
                requestKeyframeUpstream();
            }
        }
    }
    // Forward the identical signed packet — downstream verifies the same author.
    forwardToChildren(data);
}

function configureDecoder(cfg) {
    if (cfg.description && Array.isArray(cfg.description)) cfg.description = new Uint8Array(cfg.description);
    if (L.decoder && (L.decoder._w !== cfg.codedWidth || L.decoder._h !== cfg.codedHeight)) {
        try { L.decoder.close(); } catch { /* ignore */ }
        L.decoder = null;
        L.hasKeyframe = false;
    }
    if (!L.decoder) {
        const dec = new VideoDecoder({
            output: (frame) => {
                remoteCanvas.hidden = false;
                stageHint.style.display = 'none';
                if (remoteCanvas.width !== frame.displayWidth) remoteCanvas.width = frame.displayWidth;
                if (remoteCanvas.height !== frame.displayHeight) remoteCanvas.height = frame.displayHeight;
                ctx.drawImage(frame, 0, 0);
                frame.close();
                L.frames++;
                setControlsEnabled(true);
                setLiveBadge(true);
                setActiveFeed(true);
                if (L.frames % 15 === 0) updateStats();
            },
            error: (e) => {
                console.warn('[live] decoder error', e);
                if (L.decoder === dec) { decoderReset(); requestConfigUpstream(); requestKeyframeUpstream(); }
            },
        });
        dec._w = cfg.codedWidth;
        dec._h = cfg.codedHeight;
        L.decoder = dec;
    }
    try { L.decoder.configure(cfg); L.decoderReady = true; }
    catch (e) { console.warn('[live] decoder configure failed', e); L.decoderReady = false; }
}

// Drop the decoder without clearing cachedConfig (relays still serve config).
function decoderReset() {
    if (L.decoder) { try { L.decoder.close(); } catch { /* ignore */ } L.decoder = null; }
    L.hasKeyframe = false;
    L.decoderReady = false;
}
function requestConfigUpstream() { sendControlUpstream('request-config', {}); }

function resetDecoder() {
    L.hasKeyframe = false;
    L.decoderReady = false;
    L.cachedConfig = null;
    if (L.decoder) { try { L.decoder.close(); } catch { /* ignore */ } L.decoder = null; }
}

function leaveWatch() {
    clearTimeout(L.rejoinTimer);
    if (L.upstream) { try { L.upstream.pc.close(); } catch { /* ignore */ } L.upstream = null; }
    for (const [, c] of L.children) { try { c.pc.close(); } catch { /* ignore */ } }
    L.children.clear();
    L.relays.clear();
    resetDecoder();
    clearVideo('Nothing playing yet');
    setActiveFeed(false);
    L.mode = 'idle';
    L.streamerNpub = null;
    L.info = null;
    L.infoPacket = null;
    L.frames = 0;
    setStatus('idle');
    $('watch-btn').disabled = false;
    $('leave-btn').disabled = true;
    setSig(null);
    $('st-sig').textContent = '—';
    updateStreamInfoUI();
    updateStats();
}

// -------------------------------------------------------------------- UI ----
const notice = $('privacy-notice');
if (localStorage.getItem('rookoo_privacy_ok') !== '1') notice.hidden = false;
$('privacy-close').addEventListener('click', () => {
    notice.hidden = true;
    localStorage.setItem('rookoo_privacy_ok', '1');
});

// Network settings (relays + TURN, including credentials).
const settingsDialog = $('settings-dialog');
$('settings-open').addEventListener('click', () => {
    const relays = JSON.parse(localStorage.getItem('nostr_p2p_relays') || 'null');
    $('relays-input').value = Array.isArray(relays) ? relays.join('\n') : '';
    const turn = JSON.parse(localStorage.getItem('nostr_p2p_turn') || 'null') || {};
    $('turn-input').value = turn.urls || '';
    $('turn-username').value = turn.username || '';
    $('turn-credential').value = turn.credential || '';
    settingsDialog.showModal();
});
$('settings-cancel').addEventListener('click', () => settingsDialog.close());
$('settings-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const urls = $('relays-input').value.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
    if (urls.length) localStorage.setItem('nostr_p2p_relays', JSON.stringify(urls));
    else localStorage.removeItem('nostr_p2p_relays');
    const turnUrl = $('turn-input').value.trim();
    const username = $('turn-username').value.trim();
    const credential = $('turn-credential').value;
    if (turnUrl) {
        const turn = { urls: turnUrl };
        if (username) turn.username = username;
        if (credential) turn.credential = credential;
        localStorage.setItem('nostr_p2p_turn', JSON.stringify(turn));
    } else {
        localStorage.removeItem('nostr_p2p_turn');
    }
    location.reload();
});

// Video controls. Pausing only freezes the picture; packets keep flowing to
// downstream viewers. Resuming requests a fresh keyframe.
const vcPlay = $('vc-play'), vcFs = $('vc-fs'), vcPip = $('vc-pip'), vcLive = $('vc-live');
const pipSupported = ('pictureInPictureEnabled' in document && document.pictureInPictureEnabled)
    || ('webkitSetPresentationMode' in HTMLVideoElement.prototype);
if (pipSupported) vcPip.hidden = false;
function setControlsEnabled(on) {
    vcPlay.disabled = !on;
    vcFs.disabled = !on;
    if (pipSupported) vcPip.disabled = !on;
}
function setLiveBadge(on) {
    vcLive.textContent = on ? 'LIVE' : 'OFFLINE';
    vcLive.classList.toggle('off', !on);
}
vcPlay.addEventListener('click', () => {
    L.paused = !L.paused;
    vcPlay.textContent = L.paused ? '▶' : '⏸';
    vcPlay.title = L.paused ? 'Resume' : 'Pause (keeps relaying)';
    if (!L.paused) { L.hasKeyframe = false; requestKeyframeUpstream(); }
});
function activeVideoEl() { return stageVideo.hidden ? remoteCanvas : stageVideo; }
vcFs.addEventListener('click', () => {
    const fsEl = document.fullscreenElement || document.webkitFullscreenElement;
    if (fsEl) {
        (document.exitFullscreen || document.webkitExitFullscreen).call(document);
        return;
    }
    const stage = $('live-stage');
    const req = stage.requestFullscreen || stage.webkitRequestFullscreen;
    if (req) { req.call(stage); return; }
    // iOS has no element fullscreen — use the native video player instead.
    const v = activeVideoEl();
    if (typeof v.webkitEnterFullscreen === 'function') { try { v.webkitEnterFullscreen(); } catch { /* ignore */ } }
});
vcPip.addEventListener('click', () => {
    const el = activeVideoEl();
    if (document.pictureInPictureElement) { document.exitPictureInPicture?.().catch(() => {}); return; }
    if (typeof el.requestPictureInPicture === 'function') el.requestPictureInPicture().catch(() => {});
    else if (typeof el.webkitSetPresentationMode === 'function') {
        el.webkitSetPresentationMode(el.webkitPresentationMode === 'picture-in-picture' ? 'inline' : 'picture-in-picture');
    }
});

$('go-live').addEventListener('click', goLive);
$('screen-live').addEventListener('click', () => { if (localScreen) stopScreenShare(); else shareScreen(); });
$('camera-select').addEventListener('change', (e) => {
    L.deviceId = e.target.value || null;
    if (L.mode === 'streamer' && !localScreen) switchCamera(L.deviceId);
});
navigator.mediaDevices?.addEventListener?.('devicechange', listCameras);
listCameras();

// WebCodecs capability check (Chrome/Edge and Android Chrome; iOS Safari lacks
// it). Disable the parts that can't work so phones still make sense.
const canEncode = typeof VideoEncoder !== 'undefined' && typeof VideoFrame !== 'undefined';
const canDecode = typeof VideoDecoder !== 'undefined';
if (!canEncode) $('go-live').disabled = true;
if (!canDecode) {
    $('watch-btn').disabled = true;
    stageHint.textContent = 'This browser can’t play the stream — WebCodecs is needed (Chrome/Edge/Android).';
}
// Replace the parts this browser can't do with an explanation (no popups).
if (!canEncode) {
    $('broadcast-card').innerHTML = '<h3>Broadcast</h3>'
        + '<p class="muted small">Broadcasting isn’t supported in this browser: it needs WebCodecs video encoding, which is available in <b>Chrome, Edge and Android Chrome</b> but not in iOS Safari. You can still watch streams and relay them.</p>';
}
if (!canDecode) {
    $('watch-card').innerHTML = '<h3>Watch</h3>'
        + '<p class="muted small">Watching isn’t supported in this browser: it needs WebCodecs video decoding (Chrome, Edge or Android Chrome).</p>';
}
$('stop-live').addEventListener('click', stopLive);
$('watch-btn').addEventListener('click', () => {
    const raw = $('watch-input').value.trim();
    const m = raw.match(/npub1[02-9ac-hj-np-z]{20,}/i);
    if (!m) { log('paste a live link or streamer ID'); return; }
    watch(m[0]);
});
$('leave-btn').addEventListener('click', leaveWatch);
$('copy-link').addEventListener('click', async () => {
    try { await navigator.clipboard.writeText($('share-link').textContent); log('link copied'); }
    catch { log($('share-link').textContent); }
});

// Auto-join if the link carries a streamer npub.
const invited = (location.hash.match(/npub1[02-9ac-hj-np-z]{20,}/i) || [])[0];
if (invited && canDecode) { $('watch-input').value = invited; watch(invited); }
else if (invited) { log('This browser can’t decode the stream (needs WebCodecs: Chrome/Edge/Android).'); }
startTick();
updateStats();
updateStreamInfoUI();
window.__live = L;
