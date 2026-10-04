// CallbackListener: the process's reply queue, an anonymous queue on the
// callbacks exchange bound to its own name. Replies are handled one at a
// time, in arrival order, so a stream's chunks are seen in sequence.
#pragma once

#include "protobus/base_listener.h"

namespace protobus {

class CallbackListener : public BaseListener {
 public:
  explicit CallbackListener(std::shared_ptr<Connection> connection);

  // The queue replies are addressed to.
  std::string callbackQueue() const { return queueName(); }

 protected:
  const char* listenerName() const override { return "CallbackListener"; }
};

}  // namespace protobus
