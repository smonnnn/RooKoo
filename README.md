# RooKoo

A simple, **serverless peer-to-peer video meeting app** — a tiny Zoom
alternative. Video, voice, video filters, text chat and file sharing all run
directly between browsers. There is no backend: the whole app is a static page
and every participant is an equal peer.

Built on the [NostrP2P library](https://github.com/HommelWater/Bombus/tree/p2p)
from the Bombus `p2p` branch.

**Live at <https://smonnnn.github.io/RooKoo/>**

## Features

- **Video chat** — full-mesh WebRTC, one connection per participant.
- **Voice chat** — with per-participant mute and camera-off indicators.
- **Content-aware background effects** — real-time person/background
  segmentation (MediaPipe selfie segmentation) composited on a canvas before
  encoding, so everyone sees it: blur, strong blur, pixelate, black & white,
  background removal, and blue/green/white/warm/gradient backgrounds.
- **Screen sharing** — `getDisplayMedia`, swapped in with `RTCRtpSender.replaceTrack`.
- **Text chat** — gossiped across the mesh, with history sent to newcomers.
- **File sharing** — content-addressed (SHA-256), transferred in chunks and
  cached in IndexedDB so any peer who has a copy can re-serve it.
- **Join by any member's npub** — no peer list, no setup on the host side.
  Share anyone's invite link and the room opens up; the mesh introduces the
  newcomer to everybody automatically.
- **Signed join provenance** — because every message is signed, you can see who
  arrived *through whom* (e.g. "Bob · joined via Alice").
- **Responsive flat UI** in the Pidge "pigeon" palette (JetBrains Mono, square
  edges) with a mobile-friendly collapsible sidebar.

## Identity & rooms

- There are **no accounts**. On first visit a random Nostr keypair (`npub`) is
  generated in the browser and kept in `localStorage`; it is always random and
  never user-supplied. Use **⚙ → New random identity** to mint another.
- There is **no room to create and no peer to add**. A room is simply the
  connected mesh, and access is granted by knowing the unguessable random npub
  of **anyone already in it**. That makes sharing an npub equivalent to sharing
  a meeting link.
- Open an invite link (`…#<npub>`) or paste any member's npub under **Join
  room** and you are connected. The member you used acts as your introducer and
  relays the rest of the room's peer list to you.
- When two peers connect, a deterministic rule (the lexicographically larger
  `npub` offers) ensures exactly one offer per pair.

## How it works

| Concern | Approach |
| --- | --- |
| Identity | A random Nostr keypair generated once per browser and persisted locally |
| Peer discovery | Nostr relays (ephemeral, NIP-44-encrypted kind-25000 events) via `NostrP2P` |
| Room access | Knowledge of any member's random npub; relays only introduce peers |
| Join provenance | `hello`/`profile` messages carry `joinedVia` and are signature-verified |
| Chat / presence / files | WebRTC data-channel mesh, flooded with a seen-set, re-broadcast unchanged so message signatures stay valid |
| Audio / video | A separate WebRTC mesh whose SDP/ICE is tunnelled through the data channels |
| Background effects | MediaPipe selfie segmentation produces a per-frame person mask; the camera frame is composited over the chosen background on a canvas |
| Signaling relay | Directed messages are forwarded hop-by-hop when two peers are not directly connected |
| Files | SHA-256 addressed blobs; metadata is broadcast, chunks are sent on request |

No server ever sees media or messages. Relays only introduce peers.

## Running it

**Hosted version:** <https://smonnnn.github.io/RooKoo/>

Or run it locally — any static file server works (there is no build step):

```bash
cd RooKoo
python3 -m http.server 8080
```

Then open <http://localhost:8080>. Camera/microphone access requires
`localhost` or HTTPS — to use it over the internet, serve the folder over HTTPS
(e.g. behind a reverse proxy) and share the link.

## Using it

1. Enter a display name and click **Enter meeting**. A random identity is
   created for you automatically.
2. Click **Join** to turn on your camera and microphone.
3. Click **Copy invite** and send the link, or send your npub, to anyone.
   Anyone who has the npub of *any* member can join — or paste that npub under
   **Join room**.
4. Use the bottom bar to mute, stop video, share your screen, pick a background
   effect, or toggle mirroring (which flips what *everyone* sees, not just your
   self-view). Chat and files live in the right-hand sidebar. The **People**
   panel shows each member and the npub they joined through.

## Relays and NAT traversal

Relays are only used for the WebRTC handshake (SDP/ICE). Public relays are
frequently rate-limited, proof-of-work-gated, or simply offline, so the app
ships with several that are open and accept ephemeral signaling events:
`relay.snort.social`, `relay.primal.net`, `nostr.mom`,
`nostr-pub.wellorder.net`.

For the most reliable connections, run your own relay and set it under
**⚙ Network settings** (stored in `localStorage.nostr_p2p_relays`, one URL per
line). A relay you control makes joins near-instant.

For peers behind strict/symmetric NATs, add a **TURN** server in the same
dialog (stored in `localStorage.nostr_p2p_turn`). STUN is always included; TURN
is what makes hard NATs work.

### Testing with two windows on one machine

Each browser profile has **one** identity. Opening the same site in two tabs of
the same profile means both tabs share that identity, and they cannot see each
other (peers can't tell them apart). For a second participant on one machine,
use a **private/incognito window** (or a different browser/profile) — it gets
its own random identity.

## Limitations

- Fully mesh-based: fine for small meetings (the client targets ~12 direct
  peers), not for large webinars.
- Chat history is in-memory; files are persisted locally in IndexedDB.
- Background effects load the segmentation model from a CDN on first use
  (~1 MB). If the network is unavailable the effect is simply skipped; the
  rest of the app keeps working.
- There is no room password beyond the unguessable random npub: anyone who
  learns a member's npub can join. Don't share it publicly.

## Mobile & PWA

RooKoo is installable (web app manifest + service worker): on Android Chrome
use "Install app" / "Add to Home screen", on iOS Safari use Share → Add to Home
Screen. It then launches fullscreen and the shell loads even while offline.

On phones the sidebar slides in below the header with a tap-to-close backdrop,
the status indicator collapses to a coloured dot, controls wrap to fit, and
layout respects notches / the home indicator via safe-area insets. The viewport
uses `dvh` and `interactive-widget=resizes-content`, so the on-screen keyboard
resizes the chat instead of causing layout jank.

Video is always letterboxed (`object-fit: contain`), never stretched or cropped,
so a non-16:9 camera shows black bars rather than a distorted picture. Screen
sharing uses `getDisplayMedia` and works in the installed Android PWA; the
button is hidden on platforms that don't support it (e.g. iOS).

## Files

| File | Purpose |
| --- | --- |
| `index.html`, `style.css`, `app.js` | The application |
| `manifest.webmanifest`, `sw.js` | PWA manifest and offline service worker |
| `icon.png`, `icon-192.png`, `icon-512.png` | App icon / favicon (from the Pidge project) |
| `nostr-p2p.js` | The vendored NostrP2P library (signaling + data-channel mesh) |
| `nostr-deps.js` | Vendored `@noble`/nostr-tools dependencies |
| `store.js` | IndexedDB replica store for shared files |

## Credits

The P2P transport and signaling design come from
[HommelWater/Bombus](https://github.com/HommelWater/Bombus/tree/p2p) (`p2p`
branch). RooKoo is a media-first application built on that library.
