# Streaming

A method declared `returns (stream T)` is a server stream: one request, many
replies.

```protobuf
service Assistant {
  rpc generate (GenerateRequest) returns (stream Token);
}
```

## The service

The handler is a C++20 coroutine returning `protobus::Generator<T>`. Each
`co_yield` is one chunk; returning ends the stream:

<!-- doc-check: compile proto=examples/tokenstream/proto -->
```cpp
#include <chrono>

#include "chat.protobus.h"

class Assistant : public Chat::AssistantBase {
 public:
  using AssistantBase::AssistantBase;

  protobus::Generator<Chat::Token> generate(const Chat::GenerateRequest& request,
                                            protobus::CallContext& context) override {
    for (int i = 0; i < 100; ++i) {
      // waitFor returns true as soon as the caller cancels: stop producing.
      if (context.signal.waitFor(std::chrono::milliseconds(50))) co_return;
      Chat::Token token;
      token.set_index(i);
      token.set_text("word ");
      co_yield token;
    }
  }
};
```

The body does not start until the framework pulls the first chunk, and the
framework pulls one chunk at a time, publishing each and waiting for the
broker's confirm before asking for the next, so a producer is paced by the
broker.

A chunk is sent once the next one exists, so that the last can carry the
final flag: a producer that pauses between chunks delivers each one when it
produces the following one. A stream that ends without a chunk sends one empty
final message.

### Errors mid-stream

An exception thrown from the body ends the stream: the chunks already yielded
reach the caller, then the error, as a `RemoteError`. A `HandledError` crosses
with its code and message; anything else crosses as `exposeInternalErrors`
allows (see [Errors](errors.md)). An error the handler raises is not retried:
it is the stream's answer. (A chunk that cannot be published at all, its
channel gone, sends the request through the retry ladder like any failed
settlement; the replay's chunks repeat sequence numbers the caller has seen,
and it drops them.)

An exception thrown before the generator exists (a malformed request) is
answered as a unary call's would be.

### Cancellation

The handler's `signal` fires when the caller cancels or stops listening. The
framework then stops pulling, publishes nothing more the generator yields, and
destroys it: the coroutine frame unwinds and the destructors of its locals
run, which is how a producer releases what it holds. A producer that ignores
its signal is still stopped at its next `co_yield`.

Cancellation travels as a message on `proto.bus.cancel`, a fanout every
replica hears. It is best effort: a lost notice means the producer runs to
completion, the same outcome as never cancelling.

## The caller

The proxy method returns a `protobus::Stream<T>`, consumed with a range-for
loop or with `next()`:

<!-- doc-check: compile proto=examples/tokenstream/proto -->
```cpp
#include <iostream>
#include <thread>

#include "chat.protobus.h"

void read(Chat::AssistantProxy& assistant) {
  Chat::GenerateRequest request;
  request.set_prompt("why streams?");

  // Range-for; leaving the loop early cancels the stream.
  for (const auto& token : assistant.generate(request)) {
    std::cout << token.text();
    if (token.index() == 10) break;
  }

  // next(): nullopt at the end.
  auto stream = assistant.generate(request);
  while (auto token = stream.next()) std::cout << token->text();

  // Cancel from elsewhere: a Stop button, a closed HTTP request.
  protobus::AbortController stop;
  protobus::StreamOptions options;
  options.signal = stop.signal();
  std::thread button([&] { stop.abort(); });
  for (const auto& token : assistant.generate(request, options)) std::cout << token.text();
  button.join();
}
```

A stream is cancelled when the loop is left early, when the `Stream` is
destroyed unfinished, when `cancel()` is called, or when its `signal` fires. A
cancelled stream ends, it does not throw.

`protobus::StreamOptions`:

| Field | Default | |
|---|---|---|
| `actor` | empty | As for a unary call |
| `idleTimeoutMs` | `STREAM_IDLE_TIMEOUT_MS` (60 s) | The longest gap tolerated between chunks |
| `signal` | none | Cancels the stream when it fires |

The request is published before the method returns; a failure to publish
surfaces from the first `next()`.

### What a stream can throw

| Exception | Meaning |
|---|---|
| `RemoteError` | The service's error, after the chunks it sent |
| `StreamTimeoutError` | No chunk within the idle timeout. The producer is told to stop |
| `StreamBackpressureError` | The caller fell behind: more than `STREAM_MAX_BUFFERED_CHUNKS` chunks or `STREAM_MAX_BUFFERED_BYTES` bytes waiting for this call, or `STREAM_MAX_TOTAL_BUFFERED_BYTES` across all of the process's calls |
| `StreamSequenceError` | A chunk was lost (the sequence numbers have a gap). The stream fails rather than look complete |
| `DisconnectedError` | The connection dropped mid-stream |

## On the wire

Every chunk is a `ResponseContainer` published to the caller's reply queue
under the request's correlation id, with two headers: `x-protobus-seq`
(0, 1, 2, ...) and `x-protobus-final` (true on the last). The caller drops a
chunk it has already seen and fails on a gap. A failure is the final chunk,
carrying a `ResponseError`. This is the same protocol every port speaks; see
[Compatibility](compatibility.md).
