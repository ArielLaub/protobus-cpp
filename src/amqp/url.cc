#include "amqp/url.h"

#include <cctype>

#include "protobus/transport.h"

namespace protobus::amqp {

namespace {

std::string percentDecode(const std::string& s, const std::string& url) {
  std::string out;
  out.reserve(s.size());
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%') {
      if (i + 2 >= s.size() || !std::isxdigit(static_cast<unsigned char>(s[i + 1])) ||
          !std::isxdigit(static_cast<unsigned char>(s[i + 2]))) {
        throw AmqpError("broker URL has a malformed percent-escape", 0, false);
      }
      out.push_back(static_cast<char>(std::stoi(s.substr(i + 1, 2), nullptr, 16)));
      i += 2;
    } else {
      out.push_back(s[i]);
    }
  }
  (void)url;
  return out;
}

}  // namespace

BrokerUrl parseBrokerUrl(const std::string& raw) {
  BrokerUrl u;
  std::string rest;
  if (raw.rfind("amqp://", 0) == 0) {
    rest = raw.substr(7);
  } else if (raw.rfind("amqps://", 0) == 0) {
    u.tls = true;
    u.port = 5671;
    rest = raw.substr(8);
  } else {
    throw AmqpError("broker URL must start with amqp:// or amqps://", 0, false);
  }

  const auto queryStart = rest.find('?');
  std::string query;
  if (queryStart != std::string::npos) {
    query = rest.substr(queryStart + 1);
    rest = rest.substr(0, queryStart);
  }
  const auto pathStart = rest.find('/');
  std::string authority = pathStart == std::string::npos ? rest : rest.substr(0, pathStart);
  if (pathStart != std::string::npos) {
    const std::string path = rest.substr(pathStart + 1);
    // amqp://host/ names the vhost "" in the strict spec, but every client
    // in practice (amqplib included) treats an empty path as "/".
    u.vhost = path.empty() ? "/" : percentDecode(path, raw);
  }

  const auto at = authority.rfind('@');
  if (at != std::string::npos) {
    const std::string userinfo = authority.substr(0, at);
    authority = authority.substr(at + 1);
    const auto colon = userinfo.find(':');
    if (colon == std::string::npos) {
      u.user = percentDecode(userinfo, raw);
      u.password = "";
    } else {
      u.user = percentDecode(userinfo.substr(0, colon), raw);
      u.password = percentDecode(userinfo.substr(colon + 1), raw);
    }
  }

  std::string host = authority;
  if (!host.empty() && host.front() == '[') {
    const auto close = host.find(']');
    if (close == std::string::npos) throw AmqpError("broker URL has an unterminated IPv6 address", 0, false);
    const std::string after = host.substr(close + 1);
    u.host = host.substr(1, close - 1);
    if (!after.empty()) {
      if (after.front() != ':') throw AmqpError("broker URL has a malformed port", 0, false);
      host = "[]" + after;
    } else {
      host.clear();
    }
  }
  const auto colon = host.rfind(':');
  if (colon != std::string::npos) {
    const std::string port = host.substr(colon + 1);
    if (port.empty() || port.size() > 5 ||
        port.find_first_not_of("0123456789") != std::string::npos || std::stoi(port) > 65535) {
      throw AmqpError("broker URL has a malformed port", 0, false);
    }
    u.port = std::stoi(port);
    if (host.front() != '[') u.host = host.substr(0, colon);
  } else if (!host.empty() && host.front() != '[') {
    u.host = host;
  }
  if (u.host.empty()) u.host = "localhost";

  size_t start = 0;
  while (start < query.size()) {
    auto amp = query.find('&', start);
    if (amp == std::string::npos) amp = query.size();
    const std::string pair = query.substr(start, amp - start);
    if (!pair.empty()) {
      const auto eq = pair.find('=');
      const std::string key = percentDecode(pair.substr(0, eq), raw);
      const std::string value = eq == std::string::npos ? "" : percentDecode(pair.substr(eq + 1), raw);
      u.query[key] = value;
    }
    start = amp + 1;
  }
  return u;
}

}  // namespace protobus::amqp
