#include "protobus/callback_listener.h"

#include "protobus/config.h"

namespace protobus {

CallbackListener::CallbackListener(std::shared_ptr<Connection> connection) : BaseListener(std::move(connection)) {
  exchangeName_ = Config::callbacksExchangeName();
  exchangeType_ = "direct";
  orderedDelivery_ = true;
}

}  // namespace protobus
