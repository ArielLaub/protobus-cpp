# Cross-language suite

protobus-cpp against the TypeScript, Python and Go ports' real libraries,
over a real broker, in both directions:

| Test | What runs |
|---|---|
| `Servers/CppClient.AgainstEveryServer/{cpp,ts,py,go}` | the C++ client scenario against each language's server |
| `Clients/PeerClients.AgainstACppServer/{ts,py,go}` | each language's client scenario against the C++ server |
| `CrossLang.MixedReplicasShareOneRetryLadder` | `interop.Flaky` served in all four languages at once, sharing one queue and one retry ladder |

Every client runs the same fifteen checks (unary calls, priority, streams in
order, empty streams, mid-stream errors, cancellation reaching the producer,
custom types with maps and defaults, an echo round trip, handled, unhandled and
protocol errors, call metadata, instance routing, and events both ways). The
mixed-replica test requires every message to be attempted exactly four times,
in more than one language, and to reach `interop.Flaky.DLQ` exactly once with
the shared metadata headers.

## Layout

- `proto/interop.proto`: the contract, identical to protobus-go's.
- `cpppeer/`: the C++ participant (`cpppeer server` / `cpppeer client`).
- `peers/ts/peer.js`, `peers/py/peer.py`: the TypeScript and Python
  participants, the same files protobus-go's suite runs.
- `peers/go/`: the Go participant. Its server is protobus-go's own `gopeer`
  package; its client mirrors protobus-go's client scenario.
- `crosslang_test.cc`: the harness.

## Running it

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
(cd ../protobus && npm ci && npm run build-ts)   # the TypeScript peer runs the built library
cmake --build build && ./build/crosslang/protobus_crosslang_tests
```

The peers are found in sibling checkouts: `PROTOBUS_TS` (default
`../protobus`), `PROTOBUS_PY` (default `../protobus-py`, with a `venv/`) and
`PROTOBUS_GO` (default `../protobus-go`, with Go on the PATH). A missing peer
skips its tests, unless its variable is set explicitly, as CI does: then its
absence is a failure, so a misconfigured run cannot pass by skipping.

Any peer can be run by hand against any server:

```bash
PROTOBUS_TEST_AMQP=$PROTOBUS_TEST_AMQP_URL PEER_TARGET=cpp node peers/ts/peer.js client
```
