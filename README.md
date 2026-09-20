# RooKoo

A simple, **serverless peer-to-peer video meeting app** — a tiny Zoom
alternative. Video, voice, video filters, text chat and file sharing all run
directly between browsers. There is no backend: the whole app is a static page
and every participant is an equal peer.

Built on the [NostrP2P library](https://github.com/HommelWater/Bombus/tree/p2p)
from the Bombus `p2p` branch.

## Features

- **Video chat** — full-mesh WebRTC, one connection per participant.
- **Voice chat** — with per-participant mute and camera-off indicators.
- **Video filters** — applied on a canvas *before* encoding, so everyone sees
  the effect: black & white, sepia, invert, warm, cool, vivid, bright, dark,
  soft focus and pixelate. Single tap to switch.
- **Screen sharing** — `getDisplayMedia`, swapped in with `RTCRtpSender.replaceTrack`.
- **Text chat** — gossiped across the mesh, with history sent to newcomers.
- **File sharing** — content-addressed (SHA-256), transferred in chunks and
  cached in IndexedDB so any peer who has a copy can re-serve it.
- **Invite links / Add peer** — share `…#<npub>`; only one side needs to add.
- **Responsive, dark UI** with a mobile-friendly collapsible sidebar.

## How it works

| Concern | Approach |
| --- | --- |
| Identity | A Nostr keypair (`nsec`), generated in the browser and kept in `localStorage` |
| Peer discovery | Nostr relays (ephemeral, NIP-44-encrypted kind-25000 events) via `NostrP2P` |
| Chat / presence / files | WebRTC data-channel mesh, flooded with a seen-set, re-broadcast unchanged so message signatures stay valid |
| Audio / video | A separate WebRTC mesh whose SDP/ICE is tunnelled through the data channels |
| Signaling relay | Directed messages are forwarded hop-by-hop when two peers are not directly connected |
| Files | SHA-256 addressed blobs; metadata is broadcast, chunks are sent on request |

No server ever sees media or messages. Relays only introduce peers.

## Running it

Any static file server works (there is no build step):

```bash
cd RooKoo
python3 -m http.server 8080
```

Then open <http://localhost:8080> in two browsers (or two devices on your
network). Camera/microphone access requires `localhost` or HTTPS — to use it
over the internet, serve the folder over HTTPS (e.g. behind a reverse proxy)
and share the link.

## Using it

1. Enter a display name (leave the key field empty to generate a fresh
   identity, or paste an existing `nsec…`/hex key to keep one).
2. Click **Join** to turn on your camera and microphone.
3. Click **Copy invite** and send the link to a friend, or have them paste
   your `npub` under **Add peer**. Only one side needs to add the other —
   discovery is bidirectional.
4. Use the bottom bar to mute, stop video, share your screen, pick a filter or
   mirror your self-view. Chat and files live in the right-hand sidebar.

Incoming calls auto-answer: if a peer is already in a call when you join, the
media mesh is negotiated automatically using a deterministic rule (the
lexicographically larger `npub` offers), so exactly one offer is made per pair.

## Relays and NAT traversal

By default the library uses public relays (`relay.damus.io`, `nos.lol`,
`relay.nostr.band`). Public relays are frequently rate-limited or require
proof-of-work, which can stop peers from finding each other. For reliable
connections, **run your own relay** and set it under the ⚙ **Network
settings** dialog (stored in `localStorage.nostr_p2p_relays`).

For peers behind strict/symmetric NATs, add a **TURN** server in the same
dialog (stored in `localStorage.nostr_p2p_turn`). STUN is always included; TURN
is what makes hard NATs work.

## Limitations

- Fully mesh-based: fine for small meetings (the client targets ~12 direct
  peers), not for large webinars.
- Chat history is in-memory; files are persisted locally in IndexedDB.
- Screen share and camera are browser-permission dependent; Safari support for
  `canvas.captureStream` filters is good on recent versions but not older iOS.

## Files

| File | Purpose |
| --- | --- |
| `index.html`, `style.css`, `app.js` | The application |
| `nostr-p2p.js` | The vendored NostrP2P library (signaling + data-channel mesh) |
| `nostr-deps.js` | Vendored `@noble`/nostr-tools dependencies |
| `store.js` | IndexedDB replica store for shared files |

## Credits

The P2P transport and signaling design come from
[HommelWater/Bombus](https://github.com/HommelWater/Bombus/tree/p2p) (`p2p`
branch). RooKoo is a media-first application built on that library.
