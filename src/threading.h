// Guards that turn threading mistakes into immediate, explained errors.
//
// Some protobus threads run callbacks that must never block: the transport's
// I/O thread (deliveries, confirms, onDisconnected) and the timer thread. A
// blocking protobus call made there waits for work only that same thread can
// do, and hangs. Those threads are marked, and every blocking entry point
// checks the mark and throws std::logic_error instead.
//
// A service with serializeHandlers runs its handlers one at a time: each
// holds the service's turn. A handler that calls its own service and waits
// for the answer would wait for its own turn forever, so that is refused too.
#pragma once

#include <mutex>
#include <string>

namespace protobus::detail {

// Mark the calling thread as one that must not block, for the rest of its
// life. `what` names it in the error ("the protobus timer thread").
void markNonBlockingThread(const char* what);

// Throws std::logic_error when called on a marked thread. `operation` names
// the call being refused ("Connection::publish").
void requireMayBlock(const char* operation);

// One service's turn: held by whichever of its handlers is running.
struct HandlerTurn {
  explicit HandlerTurn(std::string serviceName) : service(std::move(serviceName)) {}
  std::mutex mutex;
  const std::string service;
};

// Holds a turn for its scope, and records it on this thread. A null turn is
// a no-op: the service runs its handlers in parallel.
class TurnScope {
 public:
  explicit TurnScope(HandlerTurn* turn);
  ~TurnScope();
  TurnScope(const TurnScope&) = delete;
  TurnScope& operator=(const TurnScope&) = delete;

 private:
  HandlerTurn* turn_;
  HandlerTurn* previous_;
};

// Throws std::logic_error when this thread holds the turn of the service
// that `routingKey` (REQUEST.<service>.<method>) addresses: waiting for that
// reply would wait for this very handler to finish.
void requireNotOwnTurn(const std::string& routingKey, const char* operation);

}  // namespace protobus::detail
