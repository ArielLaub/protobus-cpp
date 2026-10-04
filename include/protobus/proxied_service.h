// ProxiedService: a service holding a typed proxy to itself, for a service
// that publishes requests to its own other instances.
//
//   class Player : public protobus::ProxiedService<Combat::PlayerProxy, Combat::PlayerBase> { ... };
//
// TProxy is a generated proxy class (constructible from a Context and a
// service name, with init()); Base is the service's base class.
#pragma once

#include <memory>

#include "protobus/message_service.h"

namespace protobus {

template <typename TProxy, typename Base = MessageService>
class ProxiedService : public Base {
 public:
  using Base::Base;

  void init() override {
    Base::init();
    auto proxy = std::make_unique<TProxy>(this->context(), this->ServiceName());
    proxy->init();
    proxy_ = std::move(proxy);
  }

  TProxy& proxy() { return *proxy_; }

 private:
  std::unique_ptr<TProxy> proxy_;
};

}  // namespace protobus
