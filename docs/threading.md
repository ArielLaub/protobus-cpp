# Threading and lifetimes

protobus-cpp is thread-safe: a `Context`, its proxies and its services may be
used from any number of threads at once, and the library's own state is
locked and checked under ThreadSanitizer in CI. What it cannot lock for you is
your own code. This page is the short list of rules that keeps that code
safe, and what protobus does when one is broken.

## The rules

1. **Your handlers run in parallel**, on the connection's worker threads,
   unless you turn on `serializeHandlers`. With `maxConcurrent` above 1,
   several requests run at once; even at the default of 1, a service's event
   handlers run alongside its request handlers. State they share needs a
   mutex or atomics, or `serializeHandlers`.
2. **Some callbacks run on threads that must not block**: listed below. Do
   not make a protobus call there (an RPC, a publish, a stream's `next()`):
   hand the work to a thread of your own.
3. **The `Context` outlives everything built on it**: every service, proxy
   and stream, and every handler still running. Stop the services and drain
   before destroying it, or let `RunnableService` shut down.
4. **A custom `ILogger` must be thread-safe**: it is called from every thread
   at once.

## Serialized handlers

`MessageServiceOptions::serializeHandlers` (off by default) runs a service's
handlers one at a time, as the TypeScript port's event loop would: requests,
each step of a stream (the code between two `co_yield`s) and events never
overlap, so the service's own members need no locking.

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include "Calculator.protobus.h"

class CountingCalculator : public Calculator::ServiceBase {
 public:
  using ServiceBase::ServiceBase;

  Calculator::AddResponse add(const Calculator::AddRequest& r, protobus::CallContext&) override {
    ++calls_;  // no lock: no other handler of this service runs meanwhile
    Calculator::AddResponse out;
    out.set_result(r.a() + r.b());
    return out;
  }

 private:
  int calls_ = 0;
};

std::shared_ptr<CountingCalculator> start(protobus::Context& context) {
  protobus::MessageServiceOptions options;
  options.serializeHandlers = true;
  auto service = std::make_shared<CountingCalculator>(context, options);
  service->init();
  return service;
}
```

What it costs:

- One handler at a time per service, in this process. Other replicas, and
  other services in the process, still run in parallel.
- A request waiting for its turn is already counting down its
  `processingTimeoutMs`.
- A handler must not call its own service and wait for the answer: that
  request could only run after the handler finishes. protobus refuses the
  call with `std::logic_error` rather than let it deadlock. Calling another
  service is fine, and so is publishing an event, even to yourself. A cycle
  across two serialized services (A waits on B, which waits on A) is not
  detected; it ends at the callers' timeouts.

## Which thread runs what

| Code | Thread | May block? |
|---|---|---|
| Service handlers, stream generators, event handlers | a worker | yes, including calls to other services |
| Abort listeners (`signal.addListener`) | the thread that aborts: a worker for a processing timeout or a stream cancellation, yours for your own `AbortController` | keep them short |
| `onDisconnected`, `onReconnecting`, `onReconnected`, `onError` | the transport's I/O thread or a worker (`onDisconnected` also on the caller of `disconnect()`) | **no** |
| `Connection::publishAsync`'s completion | the transport's I/O thread | **no** |
| Your `ILogger` | any | briefly |
| Proxy calls, `publishEvent`, a stream's `next()` | yours | yes: they wait for the broker |

The two kinds of thread that must not block, the transport's I/O threads and
the timer thread, are marked. A blocking protobus call made on one throws
`std::logic_error` naming the call and the thread, instead of hanging: the
call would wait for a reply or a confirm that only that same thread can
deliver. The in-memory broker's callback thread is marked the same way, so a
test finds the mistake before production does.

## Sharing objects between threads

| Object | From several threads at once |
|---|---|
| `Context`, `Connection` | yes |
| A generated proxy, `ServiceProxy` | yes |
| A service | yes: protobus calls its handlers; your code may call its public methods |
| `AbortController`, `AbortSignal` | yes |
| A stream (`Stream<T>`, `ChunkStream`) | iterate it from one thread at a time; `cancel()` from any |

## Lifetimes

- A service is owned by a `std::shared_ptr` (`init()` refuses one that is
  not). Each running handler holds a reference, and so does a stream's
  generator and any abort listener its processing timeout started, so
  releasing a service mid-request destroys it only once that work is done.
- A stream's request and `CallContext` live as long as its generator: a
  handler's `const Request&` and `CallContext&` stay valid across its
  `co_yield`s. Anything else a generator refers to by reference must outlive
  it too.
- Destroying a `Context` closes it and waits up to
  `SHUTDOWN_DRAIN_TIMEOUT_MS` for running handlers. If some still run after
  that, they would use it after it is gone: protobus logs an error saying so,
  and a debug build (`NDEBUG` unset) aborts there, where the cause is
  visible, rather than at the memory corruption that would follow.
- `RunnableService` never runs `cleanup()` beneath a running handler; see
  [Services](services.md#ownership-and-lifecycle).
