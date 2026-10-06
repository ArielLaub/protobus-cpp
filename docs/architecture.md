# Architecture

protobus-cpp follows the TypeScript reference class for class:

| Class | Role |
|---|---|
| `Context` | One process's place on the bus: owns the connection, the message factory and the two dispatchers |
| `Connection` | The AMQP connection: reconnection, restoration, confirmed publishing, and the consume loop that runs a handler and settles its delivery |
| `MessageFactory` | The schemas the process knows, and the envelopes |
| `MessageDispatcher` | The client side of RPC: publishes requests, matches replies to waiting calls, buffers streams |
| `EventDispatcher` | Publishes events |
| `BaseListener` | A channel, an exchange, a queue and a consumer, restored across reconnections |
| `MessageListener`, `CallbackListener`, `EventListener` | A service's request queue with its retry ladder; the process's reply queue; an events queue |
| `CancelListener` | Hears stream cancellations |
| `MessageService`, `RunnableService`, `ProxiedService` | Services, their lifecycle, and a service with a proxy to itself |
| `ServiceProxy` | Calls a service by name |

Where TypeScript has a promise, C++ blocks: a call returns once its reply
arrives. Where TypeScript has an async generator, C++ has a coroutine
(`Generator<T>`). Where TypeScript has an event loop, C++ has threads.

## Threads

| Thread | Runs |
|---|---|
| The transport's I/O thread, one per AMQP connection | Everything rabbitmq-c does: reading frames, assembling deliveries, matching confirms to publishes, running commands submitted by other threads. Its callbacks never block |
| The connection's worker pool | Handlers, settlements (replies, acks, retries, dead letters) and reconnection attempts. Elastic: a task runs on an idle worker or a new one, and idle workers exit |
| The connection's timer thread | Timeouts: processing, RPC, confirm, stream idle, reconnect backoff. Its callbacks are short and hand real work to the pool, including a timed-out handler's abort listeners, which are application code |
| The caller's thread | Blocking calls: `init()`, proxy calls, `publishEvent`, `next()` on a stream |

rabbitmq-c is not thread-safe, so one thread owns each connection; other
threads submit commands to it through a queue and a wake pipe. A command that
waits for a broker reply (declare, bind, consume) blocks its submitting
thread, not the I/O thread's frame handling for long: while rabbitmq-c waits
for an RPC reply it queues every other frame, and the loop processes them as
soon as the command returns.

The pool is elastic because handlers block. A handler that calls another
service waits for a reply delivered through the same pool, and a fixed pool
saturated by such handlers would deadlock. Its size is bounded in practice by
the consumers' prefetch.

Replies are handled one at a time in arrival order, so a stream's chunks are
seen in sequence; requests are handled in parallel up to the service's
`maxConcurrent`.

## Ownership

- A `Context` outlives every service and proxy built on it. Closing it fails
  the calls and streams still waiting on it, disconnects, then waits up to
  `SHUTDOWN_DRAIN_TIMEOUT_MS` for handlers still running.
- A service's `cleanup()` (under `RunnableService`) runs only once no handler
  is running on its connection; see [Services](services.md).
- A service is owned by a `std::shared_ptr`. Each delivery takes a reference
  for as long as its handler runs (and a stream, for as long as its
  generator), so releasing a service mid-request destroys it when that request
  is done, never under a running handler.
- Components register with the connection through weak references, so a
  component destroyed between a reconnection and its restoration is simply
  skipped.
- A `Stream` holds what its call needs; it stays safe to use, or destroy,
  after its proxy is gone.

## Settling a delivery

A late-ack consumer, the default for services and event listeners, settles a
delivery after its handler:

1. The handler runs, raced against the processing timeout.
2. A reply (or every chunk of a stream) is published and confirmed.
3. The delivery is acknowledged.

The reply goes before the acknowledgement, so the worst case is a redelivered
request (at-least-once, which the retry ladder assumes anyway) rather than an
acknowledged request whose reply was never sent. On a failure, the request is
republished to the retry exchange and acknowledged, or published to the DLQ,
the caller answered, and acknowledged. If settling itself fails (the publish
cannot be confirmed), the delivery is requeued after a second.

## Reconnection

The connection has a generation, bumped by every teardown. A reconnection
attempt connects, then runs every registered restorer in registration order:
each component reopens its channel, redeclares its exchange, queue and
bindings, and resumes consuming. Only when all have finished does the
connection report itself ready and release the publishes waiting for it. A
restorer that throws, or a teardown during restoration (detected by the
generation), discards the attempt, and the backoff tries again. Restorers run
one after another because one component's exchange is what another binds to.
