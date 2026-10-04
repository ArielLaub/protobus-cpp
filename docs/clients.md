# Clients

A client is a `<Service>Proxy`, generated beside each `<Service>Base`, with a
method per rpc. It is built on a `Context` and initialised once:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include "Calculator.protobus.h"

int call(protobus::Context& context) {
  Calculator::ServiceProxy calculator(context);  // or (context, "Calculator.Service.eu1")
  calculator.init();                             // resolves the contract
  Calculator::AddRequest request;
  request.set_a(1);
  request.set_b(2);
  return calculator.add(request).result();
}
```

A proxy and its `Context` are safe for concurrent use. Calls block the calling
thread until the reply arrives, the call fails, or its timeout passes.

## Call options

`protobus::CallOptions`, the last argument of every unary method:

| Field | Default | |
|---|---|---|
| `actor` | empty | Free-text caller identity, for tracing; the service sees it as `CallContext::actor`. Not authenticated |
| `rpc` | true | `false` publishes without waiting for a reply: the call returns once the broker has the request |
| `timeoutMs` | `RPC_CALL_TIMEOUT_MS` | How long to wait for the reply. The deadline starts before the request is published, so it bounds the broker confirm too |
| `priority` | unset | 0-255; reorders the request on a priority queue, ignored elsewhere |
| `messageId` | a UUID | The request's identity (see below) |

Unset options leave the message without the property, byte for byte as an
older publisher would send it.

## What a call can throw

| Exception | Meaning |
|---|---|
| `protobus::RemoteError` | The service answered with an error: `what()` is its message, `code()` its code, `method()` the method. See [Errors](errors.md) |
| `protobus::RpcTimeoutError` | No reply within the timeout. The request may still be processed |
| `protobus::UnroutableError` | No queue is bound to the routing key: no service by that name is running. Definite: nothing was delivered |
| `protobus::PublishNackedError` | The broker refused the request. Definite |
| `protobus::PublishConfirmTimeoutError` | No broker confirm in time. **Ambiguous**: the broker may have stored the request |
| `protobus::ChannelClosedError` | The channel closed before the confirm. **Ambiguous** |
| `protobus::DisconnectedError` | The connection dropped while the call awaited its reply. The request may or may not have been processed |
| `protobus::NotReadyError` | The connection was closed, gave up reconnecting, or stayed down past `CONNECTION_READY_TIMEOUT_MS`. Nothing was published |
| `protobus::NotConnectedError` | The context was never connected |
| `protobus::InvalidRequestError` | The request could not be encoded (a `bigint` wider than 32 bytes, a message of the wrong type) |
| `protobus::InvalidPriorityError`, `protobus::InvalidMessageIdError` | An option out of range. Nothing was published |

When several outcomes are available, a failed publish wins: "the request never
left" is the more specific answer.

All protobus exceptions derive from `protobus::Error`, which carries `name()`
and `code()`. `PublishError::ambiguous()` tells the two kinds of publish
failure apart.

## Retrying safely

An ambiguous failure means the request may exist twice if you send it again.
Give the call a stable `messageId`, derived from the work (an order id), never
from a clock, and send the retry with the same id:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include "Calculator.protobus.h"

Calculator::AddResponse addOnce(Calculator::ServiceProxy& calculator, const Calculator::AddRequest& request,
                                const std::string& orderId) {
  protobus::CallOptions options;
  options.messageId = "add-" + orderId;
  for (int attempt = 0;; ++attempt) {
    try {
      return calculator.add(request, options);
    } catch (const protobus::PublishError& e) {
      if (!e.ambiguous() || attempt == 2) throw;  // definite: report; ambiguous: try again, same id
    }
  }
}
```

The service sees the id as `CallContext::messageId` on every attempt, every
redelivery and every retry hop, which is what lets it deduplicate. A blank id,
or one over 255 bytes (AMQP's limit), is refused.

## Waiting through a reconnection

A call made while the connection is being restored waits for it, up to
`CONNECTION_READY_TIMEOUT_MS`, rather than failing: the channel is being
replaced and will be there shortly. A call already awaiting its reply when the
connection drops fails with `DisconnectedError`, because its request may or
may not have been processed. See [Configuration](configuration.md#reconnection).

## The dynamic proxy

`protobus::ServiceProxy` calls methods by name with dynamic messages, for
gateways and tools that load schemas at runtime:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/protobus.h>

int addDynamically(protobus::Context& context) {
  protobus::ServiceProxy calculator(context, "Calculator.Service");
  calculator.init();
  auto request = context.factory().newMessage("Calculator.AddRequest");
  const auto* d = request->GetDescriptor();
  request->GetReflection()->SetInt32(request.get(), d->FindFieldByName("a"), 2);
  request->GetReflection()->SetInt32(request.get(), d->FindFieldByName("b"), 3);
  auto response = calculator.call("add", *request);
  return response->GetReflection()->GetInt32(*response, response->GetDescriptor()->FindFieldByName("result"));
}
```

`callStream` does the same for streaming methods. The factory knows every type
compiled into the program and every schema loaded with
`Context::init(url, {"./proto"})` or `MessageFactory::parse`. A request of the
wrong type is refused with `InvalidRequestError` before anything is sent.

## Below the proxy

`Context::publishMessage(content, routingKey, options)` publishes an encoded
`RequestContainer` and returns the raw reply; `publishStreamingMessage`
returns a `ChunkStream` of raw replies. `MessageFactory` builds and decodes the
envelopes. The proxies are built on these.
