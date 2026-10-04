// Broker URL handling: the RabbitMQ URI spec, amqp:// and amqps://.
#pragma once

#include <map>
#include <optional>
#include <string>

namespace protobus::amqp {

struct BrokerUrl {
  bool tls = false;
  std::string user = "guest";
  std::string password = "guest";
  std::string host = "localhost";
  int port = 5672;
  std::string vhost = "/";
  std::map<std::string, std::string> query;
};

// Parse a broker URL. Percent-escapes are decoded in the userinfo and vhost
// (a vhost is routinely written /%2f). An empty path is the default vhost
// "/". Throws AmqpError for something that is not an AMQP URL.
BrokerUrl parseBrokerUrl(const std::string& url);

}  // namespace protobus::amqp
