# Security

protobus trusts the broker and the processes holding its credentials: a
caller is another of your services. What it adds on top of RabbitMQ's own
access control is that a message cannot make a service do something its
routing key did not ask for, and that error text and logs disclose no more
than they must.

## Dispatch checks

A request's envelope names the method to run, and the envelope is the
publisher's to write. Before anything is decoded, a service checks, in order:

1. the routing key the broker delivered on starts with
   `REQUEST.<ServiceName>.`: the delivery belongs to this service;
2. the method in the envelope is the method the routing key names: a client
   allowed to publish to one routing key cannot run another method;
3. the envelope names a method of this service's contract, spelled in full:
   not another loaded service's, and not with extra segments;
4. the contract declares the method, and the service implements it.

A request failing any of them is answered `PROTOCOL_ERROR`, reported against
the method the routing key names, and never retried. Because of the second
check, RabbitMQ topic permissions on `proto.bus` (which routing keys a user
may publish) mean what they say.

The payload is decoded only after the checks, as the contract's request type,
and its custom-type values are checked against their wire formats. A payload
that does not decode is answered `PROTOCOL_ERROR`.

Event handlers are selected by the routing key the broker delivered on, not
by the topic the publisher wrote into the event.

## What callers see

A `HandledError`'s message always reaches the caller: raising one is a
decision to tell the caller something. An unhandled exception's message
reaches the caller only while `PROTOBUS_EXPOSE_INTERNAL_ERRORS` is true (the
default, as in the other ports: a caller is one of your own services). Set it
to `false` for a service whose callers relay errors to untrusted clients;
callers then see `internal service error (correlationId <id>)` with code
`INTERNAL_ERROR`, and the real error stays in the service's log.

The `x-last-error` header on retry and dead-letter copies never carries an
unhandled error's message, whatever the setting: queues and dashboards keep it
longer, and more widely, than a caller would.

## What is logged

- A broker URL's password is replaced with `***`.
- Request, reply and event payloads are never logged: a failure to decode
  logs the type and size. Payload material reaches a log only through a
  diagnostics serializer the application installs, which decides what is safe
  (see [Configuration](configuration.md#logging)).
- Structured records carry only framework-generated text and low-cardinality
  identifiers, with control characters collapsed, so a value cannot forge a
  log line.

## Identity

`CallContext::actor` is whatever the caller wrote into the envelope. Use it for
tracing, never for authorisation. `messageId` is likewise caller-supplied; it
identifies a message for deduplication, not a sender.

## Transport

Use `amqps://` for TLS. The broker's certificate is verified against the
system's certificate authorities, or the bundle named by `cacertfile`; a
client certificate is `certfile` and `keyfile`. `verify=verify_none` turns
verification off and is for tests only.
