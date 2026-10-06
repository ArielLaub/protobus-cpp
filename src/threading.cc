#include "threading.h"

#include <stdexcept>

namespace protobus::detail {

namespace {
thread_local const char* nonBlockingThread = nullptr;
thread_local HandlerTurn* heldTurn = nullptr;
}  // namespace

void markNonBlockingThread(const char* what) { nonBlockingThread = what; }

void requireMayBlock(const char* operation) {
  if (nonBlockingThread == nullptr) return;
  throw std::logic_error(std::string("protobus: ") + operation + " blocks, and was called on " + nonBlockingThread +
                         ", which runs callbacks that must not block (onDisconnected, onReconnecting, "
                         "publishAsync's completion, timers). It would wait for work only that thread can do, "
                         "and hang. Hand the work to a thread of your own instead.");
}

TurnScope::TurnScope(HandlerTurn* turn) : turn_(turn), previous_(heldTurn) {
  if (turn_ == nullptr) return;
  turn_->mutex.lock();
  heldTurn = turn_;
}

TurnScope::~TurnScope() {
  if (turn_ == nullptr) return;
  heldTurn = previous_;
  turn_->mutex.unlock();
}

void requireNotOwnTurn(const std::string& routingKey, const char* operation) {
  if (heldTurn == nullptr) return;
  const std::string prefix = "REQUEST." + heldTurn->service + ".";
  // Its own methods are one word past the prefix; more words name another
  // service, such as an instance-named one ("<service>.<instance>").
  if (routingKey.compare(0, prefix.size(), prefix) != 0) return;
  if (routingKey.find('.', prefix.size()) != std::string::npos) return;
  throw std::logic_error("protobus: " + std::string(operation) + " to " + routingKey + " from inside a handler of " +
                         heldTurn->service +
                         ", which has serializeHandlers on: the request could only run once this handler "
                         "finishes, so waiting for its reply would deadlock. Call the method directly, or "
                         "turn serializeHandlers off.");
}

}  // namespace protobus::detail
