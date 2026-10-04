// RunnableService: a MessageService with process lifecycle.
//
//   int main() {
//     protobus::Context ctx;
//     ctx.init(std::getenv("AMQP_URL"));
//     auto service = protobus::RunnableService::start<CalculatorService>(ctx);
//     return protobus::RunnableService::wait();
//   }
//
// start() initialises the service and installs SIGINT and SIGTERM handlers.
// On a signal (or requestShutdown()) every started service is shut down
// gracefully: it stops taking new work, in-flight work drains (up to
// SHUTDOWN_DRAIN_TIMEOUT_MS), each service's cleanup() runs, and the
// connections close. wait() then returns the exit code. If nothing returns
// from main within SHUTDOWN_EXIT_GRACE_MS of the shutdown, the process is
// made to exit, so a stray thread cannot hold it open.
#pragma once

#include <functional>
#include <memory>
#include <string>

#include "protobus/logger.h"
#include "protobus/message_service.h"

namespace protobus {

class RunnableService : public MessageService {
 public:
  using MessageService::MessageService;

  // By convention, "Calculator.Service" reads Calculator.proto. Override for
  // another layout.
  std::string ProtoFileName() const override;

  // Release the service's own resources at shutdown, after in-flight work
  // has drained. Default: nothing.
  virtual void cleanup() {}

  // Construct T(context, options), initialise it, run postInit, and register
  // it for graceful shutdown. On a startup failure the service is stopped and
  // the connection closed, and the error rethrown.
  template <typename T>
  static std::shared_ptr<T> start(Context& context, MessageServiceOptions options = {},
                                  std::function<void(T&)> postInit = {}) {
    installSignalHandlers();
    std::shared_ptr<T> service;
    try {
      service = std::make_shared<T>(context, options);
      Logger::info("Starting service: " + service->ServiceName());
      service->init();
      if (postInit) postInit(*service);
      registerForShutdown(context, service);
      Logger::info("Service ready: " + service->ServiceName());
      return service;
    } catch (const std::exception& e) {
      Logger::error(std::string("Service startup failed: ") + e.what());
      abandonStart(context, service);
      throw;
    }
  }

  // Block until a shutdown has completed; returns the process exit code.
  static int wait();
  // Shut every started service down, as a signal would. Returns at once.
  static void requestShutdown(const std::string& reason = "requested");
  // Whether a shutdown ends the process after the grace period (default on).
  // Tests that start services in-process turn it off.
  static void setExitAfterShutdown(bool exit);

 private:
  static void installSignalHandlers();
  static void registerForShutdown(Context& context, std::shared_ptr<RunnableService> service);
  static void abandonStart(Context& context, std::shared_ptr<RunnableService> service);
};

}  // namespace protobus
