# TLS

**Status: not implemented.** `wss://` is refused by name at dial time; see
[Current behaviour](#current-behaviour). This note records what the work is,
what it is not, and the three decisions that should be made before starting.

## Why this note exists

TLS looks like a large project and is usually assumed to be one. In this
repository it is not, for a specific reason: we would not be implementing TLS,
we would be *enabling* it. Two facts decide the size of the job:

- **rustls is already in the dependency tree.** It arrives transitively
  through `webrtc`'s DTLS stack (`rustls` 0.23), so it is already compiled
  into every native build. Using it adds no new heavy dependency and no new
  audit surface.
- **`tokio-tungstenite` already has the plumbing.** Its `__rustls-tls`
  feature wires rustls into both connect and accept. We currently take the
  crate with default features (`connect`, `handshake`), which is why there is
  no TLS today.

So the work is feature flags, a connector, an acceptor, and certificate
configuration — roughly a day or two for client and server `wss`. What
follows is the part that actually needs thought.

## Current behaviour

`wss://` is **refused, not downgraded**. `Endpoint` records whether TLS was
asked for (`Endpoint::secure`) and `dial()` returns a typed
`TransportError::SchemeUnavailable { scheme: "wss", .. }` naming TLS as the
reason.

This matters more than it might look. An earlier version of `Endpoint` parsed
`wss://` as an alias for `wssc` and then rebuilt the URL as `ws://` — so a
caller asking for an encrypted connection silently got a plaintext one. A
scheme that promises encryption must never quietly hand back a connection
without it; that is the single worst failure mode a transport can have, and
it is exactly what this crate's "named, typed refusal, never a stub that
half-works" rule exists to prevent. The refusal is covered by
`a_secure_endpoint_is_refused_rather_than_downgraded` in
`tests/test_endpoint_framing_flow.rs`.

`ws://` is unaffected and carries no promise to break.

## What the work is

### 1. Client `wss` (smallest, useful on its own)

Enable `tokio-tungstenite`'s rustls feature, build a `Connector` from the
chosen root store, and pass it through `WebSocketNative::connect`. Then
`Endpoint::dial` drops the refusal above and constructs `wss://` for secure
endpoints — the code already branches on `Endpoint::secure`.

### 2. Server `wss`

A `tokio_rustls::TlsAcceptor` in front of the accept path, plus certificate
and private key loading. `AutoDetectListener` needs care: it sniffs the first
bytes of a connection to tell TCP from a WebSocket upgrade, and under TLS
those bytes are a ClientHello. Detection has to happen *after* the TLS
handshake, not before, or the sniff will see ciphertext.

### 3. TLS peer identity

`PeerIdentity` already carries proven identities for SSH (host key and client
key). A TLS peer certificate is the same kind of fact, and populating it is
what turns `wss` from "encrypted" into "authenticated" — the thing that makes
it comparable to the `ssh` scheme rather than merely private. This is design
work, not plumbing, and is worth treating as its own decision.

## Decisions to make first

These are the reasons to agree a direction before writing code, and none of
them is about effort.

**Root store.** Three real options, and the right one is not obvious:

| Option | For | Against |
|---|---|---|
| `webpki-roots` (bundled Mozilla set) | Reproducible, no host dependency, works identically everywhere | Updating roots needs a release of this crate |
| Native platform roots | Honours enterprise and corporate CAs, follows OS policy | Varies per host; the same binary behaves differently on different machines |
| Pin a private CA | Nodes in one deployment only ever talk to each other, so a public CA proves nothing useful | Requires issuing and rotating certificates |

For a swarm whose members authenticate each other, the third is arguably the
best fit and the first two are the wrong question. This should be settled
deliberately — it is the decision most likely to be regretted later.

**Certificate verification policy.** Whether a caller may opt out (the TLS
analogue of `HostKeyVerification::AcceptAny` in the `ssh` scheme), and if so
how loudly. The `ssh` precedent is that accepting an unknown key is an
explicit, named opt-in and never the default; TLS should follow it rather
than invent a second convention.

**Whether `wss` reports peer identity.** See §3 above. If it does, decide
what a certificate maps to in `PeerIdentity` before shipping the encryption,
so the identity surface does not have to change shape afterwards.

## Platform reach

| Target | TLS story |
|---|---|
| Native | rustls, as described above |
| Browser | Nothing to do — the browser's own WebSocket API does TLS for `wss://`, and certificate policy belongs to the browser |
| wasm32-wasip2 | The awkward one. `wasi-tls` is not broadly available, so `wss` most likely gets a typed refusal here while native and browser work — the same shape as the existing `ssh` and `webrtc` gaps in the scheme table |

## Relationship to gRPC

TLS is a prerequisite for running gRPC over this crate (`tonic` needs TLS and
ALPN for h2). It is worth doing on its own merits, but choosing the backend
here also sets the direction for that. The other prerequisite is unrelated to
TLS: `Transport` takes `&mut self` in both directions, so an `AsyncRead` and
`AsyncWrite` half cannot coexist without a lock — that needs either a `split()`
into halves or interior mutability, and it is a breaking change across every
implementor.

## Suggested sequence

1. Native client `wss` — smallest, immediately useful, exercises the root
   store decision.
2. Server-side accept, including the `AutoDetectListener` ordering problem.
3. TLS peer identity, as a separate decision once the first two are in.

Not recommended: doing all three at once, or deferring the root store choice
by defaulting to `webpki-roots` because it is the easiest to wire up.
