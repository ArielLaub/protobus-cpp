// An isolated RabbitMQ for tests: a fresh virtual host per test on the broker
// named by PROTOBUS_TEST_AMQP_URL, created and deleted through the management
// API at PROTOBUS_TEST_MGMT_URL.
//
// There is deliberately no default broker URL: localhost:5672 is routinely a
// port-forward to a shared cluster, and a suite that declares and deletes
// queues must never reach one by accident. Tests skip when the variables are
// unset, and fail when they are set but the broker is unusable.
#pragma once

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <random>
#include <regex>
#include <string>
#include <thread>
#include <vector>

namespace integration {

struct HttpResponse {
  int status = 0;
  std::string body;
};

inline std::string base64(const std::string& in) {
  static const char* t = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  int val = 0;
  int bits = -6;
  for (unsigned char c : in) {
    val = (val << 8) + c;
    bits += 8;
    while (bits >= 0) {
      out.push_back(t[(val >> bits) & 0x3f]);
      bits -= 6;
    }
  }
  if (bits > -6) out.push_back(t[((val << 8) >> (bits + 8)) & 0x3f]);
  while (out.size() % 4) out.push_back('=');
  return out;
}

inline std::string urlEncode(const std::string& s) {
  static const char* hex = "0123456789ABCDEF";
  std::string out;
  for (unsigned char c : s) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 15]);
    }
  }
  return out;
}

// The management API, over a minimal HTTP/1.1 client.
class Management {
 public:
  explicit Management(const std::string& url) {
    // http://user:pass@host:port
    static const std::regex re(R"(^http://([^:@/]+):([^@/]*)@([^:/]+):(\d+)/?$)");
    std::smatch m;
    if (!std::regex_match(url, m, re)) throw std::runtime_error("PROTOBUS_TEST_MGMT_URL must be http://user:pass@host:port");
    auth_ = base64(m[1].str() + ":" + m[2].str());
    host_ = m[3];
    port_ = m[4];
  }

  HttpResponse request(const std::string& method, const std::string& path, const std::string& body = "") const {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host_.c_str(), port_.c_str(), &hints, &res) != 0) throw std::runtime_error("cannot resolve " + host_);
    int fd = -1;
    for (auto* p = res; p; p = p->ai_next) {
      fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
      ::close(fd);
      fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) throw std::runtime_error("management API unreachable at " + host_ + ":" + port_);
    std::string req = method + " " + path + " HTTP/1.1\r\nHost: " + host_ + "\r\nAuthorization: Basic " + auth_ +
                      "\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: " +
                      std::to_string(body.size()) + "\r\n\r\n" + body;
    ::send(fd, req.data(), req.size(), 0);
    std::string raw;
    char buf[8192];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof buf, 0)) > 0) raw.append(buf, static_cast<size_t>(n));
    ::close(fd);
    HttpResponse r;
    if (raw.size() > 12) r.status = std::stoi(raw.substr(9, 3));
    auto split = raw.find("\r\n\r\n");
    if (split != std::string::npos) {
      std::string head = raw.substr(0, split);
      r.body = raw.substr(split + 4);
      if (head.find("Transfer-Encoding: chunked") != std::string::npos ||
          head.find("transfer-encoding: chunked") != std::string::npos) {
        std::string decoded;
        size_t pos = 0;
        while (pos < r.body.size()) {
          auto eol = r.body.find("\r\n", pos);
          if (eol == std::string::npos) break;
          const size_t len = std::stoul(r.body.substr(pos, eol - pos), nullptr, 16);
          if (len == 0) break;
          decoded += r.body.substr(eol + 2, len);
          pos = eol + 2 + len + 2;
        }
        r.body = decoded;
      }
    }
    return r;
  }

 private:
  std::string auth_;
  std::string host_;
  std::string port_;
};

class VHost {
 public:
  VHost(const Management& mgmt, std::string amqpUrlBase, std::string name, std::string user)
      : mgmt_(mgmt), amqpBase_(std::move(amqpUrlBase)), name_(std::move(name)), user_(std::move(user)) {}
  ~VHost() {
    mgmt_.request("DELETE", "/api/vhosts/" + urlEncode(name_));
    for (const auto& u : users_) mgmt_.request("DELETE", "/api/users/" + urlEncode(u));
  }

  std::string url() const { return amqpBase_ + urlEncode(name_); }
  const std::string& name() const { return name_; }

  // Close every client connection to this vhost from the broker's side, as a
  // network failure would. Polls, because the management API lists a new
  // connection only after its statistics interval.
  int closeConnections(std::chrono::milliseconds wait = std::chrono::seconds(10)) const {
    static const std::regex nameRe("\"name\":\"([^\"]+)\"");
    const auto deadline = std::chrono::steady_clock::now() + wait;
    for (;;) {
      auto r = mgmt_.request("GET", "/api/vhosts/" + urlEncode(name_) + "/connections?columns=name");
      std::vector<std::string> names;
      for (std::sregex_iterator it(r.body.begin(), r.body.end(), nameRe), end; it != end; ++it) {
        names.push_back((*it)[1]);
      }
      if (!names.empty()) {
        for (const auto& n : names) mgmt_.request("DELETE", "/api/connections/" + urlEncode(n));
        return static_cast<int>(names.size());
      }
      if (std::chrono::steady_clock::now() > deadline) return 0;
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }

  // The number of messages in a queue, polling until it equals `want` or the
  // wait ends. Returns the last count seen.
  int waitQueueDepth(const std::string& queue, int want, std::chrono::milliseconds wait = std::chrono::seconds(15)) const {
    static const std::regex re("\"messages\":(\\d+)");
    const auto deadline = std::chrono::steady_clock::now() + wait;
    int seen = -1;
    for (;;) {
      auto r = mgmt_.request("GET", "/api/queues/" + urlEncode(name_) + "/" + urlEncode(queue) + "?columns=messages");
      std::smatch m;
      if (std::regex_search(r.body, m, re)) seen = std::stoi(m[1]);
      if (seen == want || std::chrono::steady_clock::now() > deadline) return seen;
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }

  // Delete a queue from the broker's side, as an operator would.
  bool deleteQueue(const std::string& queue) const {
    return mgmt_.request("DELETE", "/api/queues/" + urlEncode(name_) + "/" + urlEncode(queue)).status < 300;
  }

  // A user of this vhost with the given permissions (regular expressions),
  // and optionally a topic permission restricting what it may publish to
  // `topicExchange`. Returns the AMQP URL that logs in as it. The user is
  // deleted with the vhost's other test state by the caller.
  std::string createUser(const std::string& user, const std::string& password, const std::string& configure,
                         const std::string& write, const std::string& read, const std::string& topicExchange = "",
                         const std::string& topicWrite = "") const {
    auto r = mgmt_.request("PUT", "/api/users/" + urlEncode(user),
                           "{\"password\":\"" + password + "\",\"tags\":\"\"}");
    if (r.status >= 300) throw std::runtime_error("cannot create user: HTTP " + std::to_string(r.status));
    r = mgmt_.request("PUT", "/api/permissions/" + urlEncode(name_) + "/" + urlEncode(user),
                      "{\"configure\":\"" + configure + "\",\"write\":\"" + write + "\",\"read\":\"" + read +
                          "\"}");
    if (r.status >= 300) throw std::runtime_error("cannot grant permissions: HTTP " + std::to_string(r.status));
    if (!topicExchange.empty()) {
      r = mgmt_.request("PUT", "/api/topic-permissions/" + urlEncode(name_) + "/" + urlEncode(user),
                        "{\"exchange\":\"" + topicExchange + "\",\"write\":\"" + topicWrite +
                            "\",\"read\":\".*\"}");
      if (r.status >= 300) throw std::runtime_error("cannot set topic permissions: HTTP " + std::to_string(r.status));
    }
    users_.push_back(user);
    // amqp://guest:guest@host:port/ -> amqp://user:password@host:port/<vhost>
    static const std::regex re(R"(^(amqps?://)[^@/]+@(.*)$)");
    std::smatch m;
    const std::string base = amqpBase_;
    if (!std::regex_match(base, m, re)) throw std::runtime_error("unexpected AMQP URL");
    return m[1].str() + user + ":" + password + "@" + m[2].str() + urlEncode(name_);
  }

  bool queueExists(const std::string& queue) const {
    return mgmt_.request("GET", "/api/queues/" + urlEncode(name_) + "/" + urlEncode(queue)).status == 200;
  }

 private:
  const Management& mgmt_;
  std::string amqpBase_;
  std::string name_;
  std::string user_;
  mutable std::vector<std::string> users_;
};

class Broker {
 public:
  // Skips when the broker variables are unset; fails when they are set but
  // the management API cannot be reached.
  static Broker* require() {
    const char* amqp = std::getenv("PROTOBUS_TEST_AMQP_URL");
    const char* mgmt = std::getenv("PROTOBUS_TEST_MGMT_URL");
    if (!amqp || !mgmt || !*amqp || !*mgmt) return nullptr;
    static Broker* broker = new Broker(amqp, mgmt);
    return broker;
  }

  std::unique_ptr<VHost> newVHost() {
    static std::mt19937 gen{std::random_device{}()};
    const std::string name = "protobus-cpp-test-" + std::to_string(gen());
    auto r = mgmt_.request("PUT", "/api/vhosts/" + urlEncode(name), "{}");
    if (r.status >= 300) throw std::runtime_error("cannot create vhost: HTTP " + std::to_string(r.status) + " " + r.body);
    r = mgmt_.request("PUT", "/api/permissions/" + urlEncode(name) + "/" + urlEncode(user_),
                      R"({"configure":".*","write":".*","read":".*"})");
    if (r.status >= 300) throw std::runtime_error("cannot grant permissions: HTTP " + std::to_string(r.status));
    return std::make_unique<VHost>(mgmt_, amqpBase_, name, user_);
  }

 private:
  Broker(const std::string& amqp, const std::string& mgmt) : mgmt_(mgmt) {
    static const std::regex re(R"(^(amqps?://([^:@/]+)(:[^@/]*)?@[^/]+)/?.*$)");
    std::smatch m;
    if (!std::regex_match(amqp, m, re)) throw std::runtime_error("PROTOBUS_TEST_AMQP_URL must carry a user");
    amqpBase_ = m[1].str() + "/";
    user_ = m[2];
    auto r = mgmt_.request("GET", "/api/overview");
    if (r.status != 200) throw std::runtime_error("management API unreachable: HTTP " + std::to_string(r.status));
  }

  Management mgmt_;
  std::string amqpBase_;
  std::string user_;
};

}  // namespace integration

#define REQUIRE_BROKER(var)                                                                       \
  auto* var = ::integration::Broker::require();                                                   \
  if (var == nullptr) GTEST_SKIP() << "PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set"
