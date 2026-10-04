# protobus-cpp documentation

| Start | |
|---|---|
| [Getting Started](getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](clients.md) | Calling services: options, timeouts, errors, the dynamic proxy |
| [Streaming](streaming.md) | Server streaming and cancellation |
| [Events](events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](errors.md) | The error model, retries and dead letters |

| Reference | |
|---|---|
| [Configuration](configuration.md) | Environment variables and reconnection |
| [Code generation](codegen.md) | The CLI, the protoc plugin, `protobus_generate()`, custom types |
| [Testing](testing.md) | The in-memory broker and the suites |
| [Compatibility](compatibility.md) | The wire contract and how the ports differ |
| [Architecture](architecture.md) | Threads, ownership and reconnection |
| [Security](security.md) | Dispatch checks, error exposure, logging |

The C++ snippets marked for it in these pages are compiled by the test suite
(`scripts/check-doc-snippets.py`), so they track the API.
