// The protobus getting-started example: a service, a client, and an event, in
// one program.
//
//   docker compose up -d --wait                         # RabbitMQ on 127.0.0.1:25672
//   export AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
//   ./build/examples/calculator server                   # in one terminal
//   ./build/examples/calculator client                   # in another
//   ./build/examples/calculator                          # or both at once
//
// The schema (proto/Calculator.proto) is an ordinary protobus schema, shared
// as-is with TypeScript, Python and Go services. The build generates its C++
// code with protobus_generate().
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <thread>

#include "Calculator.protobus.h"

// Implements Calculator.Service: override the rpcs of the generated
// ServiceBase. An rpc left unimplemented answers PROTOCOL_ERROR.
class CalculatorService : public Calculator::ServiceBase {
 public:
  using ServiceBase::ServiceBase;

  Calculator::AddResponse add(const Calculator::AddRequest& request, protobus::CallContext&) override {
    announce("add");
    Calculator::AddResponse response;
    response.set_result(request.a() + request.b());
    return response;
  }

  Calculator::DivideResponse divide(const Calculator::DivideRequest& request, protobus::CallContext&) override {
    if (request.divisor() == 0) {
      // A HandledError is an answer, not a failure: the caller gets it at
      // once and it is never retried. Anything else thrown is retried.
      throw protobus::HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
    }
    announce("divide");
    Calculator::DivideResponse response;
    response.set_quotient(request.dividend() / request.divisor());
    return response;
  }

 private:
  void announce(const std::string& operation) {
    // Events fan out: every subscribing service gets one.
    Calculator::Calculated event;
    event.set_operation(operation);
    *event.mutable_at() = protobus::makeTimestamp(std::chrono::system_clock::now());
    publishEvent(event);
  }
};

void call(protobus::Context& context) {
  Calculator::ServiceProxy calculator(context);
  calculator.init();

  Calculator::AddRequest add;
  add.set_a(20);
  add.set_b(22);
  const auto sum = calculator.add(add);
  std::cout << "20 + 22 = " << sum.result() << std::endl;

  Calculator::DivideRequest divide;
  divide.set_dividend(1);
  divide.set_divisor(4);
  protobus::CallOptions options;
  options.actor = "example-client";
  const auto quotient = calculator.divide(divide, options);
  std::cout << "1 / 4 = " << quotient.quotient() << std::endl;

  divide.set_divisor(0);
  try {
    calculator.divide(divide);
  } catch (const protobus::RemoteError& e) {
    std::cout << "1 / 0 -> " << e.what() << " (" << e.code() << ")" << std::endl;
  }
}

int main(int argc, char** argv) {
  const std::string mode = argc > 1 ? argv[1] : "both";
  const char* url = std::getenv("AMQP_URL");
  protobus::Context context;
  context.init(url ? url : "amqp://guest:guest@127.0.0.1:25672/");

  if (mode == "client") {
    call(context);
    return 0;
  }

  protobus::MessageServiceOptions options;
  options.maxConcurrent = 8;
  auto service = protobus::RunnableService::start<CalculatorService>(context, options);
  // Subscribers receive typed events, on the default topic
  // EVENT.Calculator.Calculated.
  service->subscribeEvent<Calculator::Calculated>(
      [](const Calculator::Calculated& event, const std::string&, const std::string&) {
        const auto at = std::chrono::system_clock::to_time_t(protobus::toTimePoint(event.at()));
        std::cout << "event: " << event.operation() << " at " << std::put_time(std::gmtime(&at), "%FT%TZ")
                  << std::endl;
      });

  if (mode == "both") {
    call(context);
    // Let the events arrive before shutting down.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    protobus::RunnableService::requestShutdown("done");
  } else {
    std::cout << "calculator service running; Ctrl-C to stop" << std::endl;
  }
  // Returns once SIGINT/SIGTERM (or requestShutdown) has shut the service
  // down gracefully.
  return protobus::RunnableService::wait();
}
