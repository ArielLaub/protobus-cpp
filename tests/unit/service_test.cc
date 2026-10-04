#include <gtest/gtest.h>

#include "helpers.h"
#include "wire/envelope.h"

namespace {

using pbtesting::CalcService;
using pbtesting::eventually;
using pbtesting::header;
using pbtesting::MemoryBus;

using ServiceTest = MemoryBus;

pbtest::AddRequest add(int a, int b) {
  pbtest::AddRequest r;
  r.set_a(a);
  r.set_b(b);
  return r;
}

TEST_F(ServiceTest, AnswersAUnaryCall) {
  serve();
  auto calc = proxy();
  EXPECT_EQ(calc.add(add(20, 22)).result(), 42);
}

TEST_F(ServiceTest, DeclaresTheTopologyEveryPortDeclares) {
  serve();
  EXPECT_TRUE(broker->queueExists("pbtest.Calc"));
  EXPECT_TRUE(broker->queueExists("pbtest.Calc.Events"));
  EXPECT_TRUE(broker->queueExists("pbtest.Calc.Retry"));
  EXPECT_TRUE(broker->queueExists("pbtest.Calc.DLQ"));
  EXPECT_TRUE(broker->exchangeExists("pbtest.Calc.Retry.Exchange"));
  EXPECT_EQ(broker->bindings("pbtest.Calc", "proto.bus"), std::vector<std::string>{"REQUEST.pbtest.Calc.*"});
  EXPECT_EQ(broker->bindings("pbtest.Calc.Retry", "pbtest.Calc.Retry.Exchange"), std::vector<std::string>{"#"});
  auto retryArgs = broker->queueArguments("pbtest.Calc.Retry");
  ASSERT_TRUE(retryArgs);
  EXPECT_EQ(retryArgs->at("x-message-ttl").asInt(), 5000);
  EXPECT_EQ(retryArgs->at("x-dead-letter-exchange").asString(), "proto.bus");
  // A service that configures nothing declares its queue with no arguments,
  // so it redeclares cleanly against one another port created.
  EXPECT_TRUE(broker->queueArguments("pbtest.Calc")->empty());
}

TEST_F(ServiceTest, HandledErrorIsAnAnswerNotARetry) {
  auto svc = serve();
  auto calc = proxy();
  pbtest::DivideRequest r;
  r.set_dividend(1);
  try {
    calc.divide(r);
    FAIL() << "expected a RemoteError";
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "DIVISION_BY_ZERO");
    EXPECT_STREQ(e.what(), "cannot divide by zero");
    EXPECT_EQ(e.method(), "pbtest.Calc.divide");
  }
  pbtest::FailRequest f;
  f.set_handled(true);
  f.set_id("h");
  EXPECT_THROW(calc.fail(f), protobus::RemoteError);
  EXPECT_EQ(svc->failAttempts.load(), 1);
  EXPECT_EQ(broker->queueDepth("pbtest.Calc.DLQ"), 0u);
}

TEST_F(ServiceTest, UnhandledErrorClimbsTheRetryLadderIntoTheDlq) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 2;
  o.retry.retryDelayMs = 20;
  auto svc = serve(o);
  auto calc = proxy();
  pbtest::FailRequest f;
  f.set_id("x1");
  protobus::CallOptions call;
  call.messageId = "order-17";
  try {
    calc.fail(f, call);
    FAIL() << "expected a RemoteError";
  } catch (const protobus::RemoteError& e) {
    // The caller is answered once retries are spent, with the final failure.
    EXPECT_STREQ(e.what(), "boom x1");
  }
  EXPECT_EQ(svc->failAttempts.load(), 3);
  // The message keeps its identity across every hop.
  for (const auto& id : svc->failMessageIds) EXPECT_EQ(id, "order-17");

  ASSERT_TRUE(eventually([&] { return broker->queueDepth("pbtest.Calc.DLQ") == 1; }));
  auto dead = broker->peek("pbtest.Calc.DLQ").at(0);
  EXPECT_EQ(header(dead, "x-retry-count"), "2");
  EXPECT_EQ(header(dead, "x-original-queue"), "pbtest.Calc");
  EXPECT_EQ(header(dead, "x-original-routing-key"), "REQUEST.pbtest.Calc.fail");
  // The class and code, never an unhandled error's message.
  EXPECT_EQ(header(dead, "x-last-error"), "std::runtime_error");
  EXPECT_FALSE(header(dead, "x-first-failure-time").empty());
  EXPECT_FALSE(header(dead, "x-dlq-time").empty());
  EXPECT_EQ(dead.properties.messageId, "order-17");
  EXPECT_EQ(dead.properties.contentType, "application/octet-stream");
  EXPECT_EQ(dead.properties.deliveryMode, 2);
  // A dead letter has no caller left to answer.
  EXPECT_FALSE(dead.properties.replyTo);
}

TEST_F(ServiceTest, WithoutRetriesTheCallerIsAnsweredAndTheMessageRejected) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 0;
  auto svc = serve(o);
  auto calc = proxy();
  pbtest::FailRequest f;
  f.set_id("once");
  EXPECT_THROW(calc.fail(f), protobus::RemoteError);
  EXPECT_EQ(svc->failAttempts.load(), 1);
  EXPECT_FALSE(broker->queueExists("pbtest.Calc.Retry"));
  EXPECT_EQ(broker->queueDepth("pbtest.Calc"), 0u);
}

TEST_F(ServiceTest, InternalErrorsCanBeHiddenFromCallers) {
  env.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false");
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 0;
  serve(o);
  auto calc = proxy();
  pbtest::FailRequest f;
  f.set_id("secret");
  try {
    calc.fail(f);
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "INTERNAL_ERROR");
    EXPECT_NE(std::string(e.what()).find("internal service error (correlationId "), std::string::npos);
    EXPECT_EQ(std::string(e.what()).find("secret"), std::string::npos);
  }
}

TEST_F(ServiceTest, ProcessingTimeoutAnswersTheCaller) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 0;
  o.processingTimeoutMs = 50;
  auto svc = serve(o);
  auto calc = proxy();
  pbtest::SlowRequest r;
  r.set_ms(2000);
  try {
    calc.slow(r);
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROCESSING_TIMEOUT");
    EXPECT_NE(std::string(e.what()).find("exceeded the 50ms processing timeout"), std::string::npos);
  }
  // The handler's signal fired, so a cooperative handler stops.
  EXPECT_TRUE(eventually([&] { return svc->slowAborted.load() == 1; }));
}

TEST_F(ServiceTest, UnimplementedRpcAnswersProtocolError) {
  serve();
  auto calc = proxy();
  try {
    calc.unimplemented({});
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROTOCOL_ERROR");
  }
}

TEST_F(ServiceTest, CallMetadataReachesTheHandler) {
  serve();
  auto calc = proxy();
  protobus::CallOptions o;
  o.actor = "client-cpp";
  o.messageId = "mid-1";
  auto who = calc.whoami({}, o);
  EXPECT_EQ(who.actor(), "client-cpp");
  EXPECT_EQ(who.message_id(), "mid-1");
  EXPECT_EQ(who.routing_key(), "REQUEST.pbtest.Calc.whoami");
  EXPECT_FALSE(who.redelivered());
  // Without one, every publish still carries a messageId.
  EXPECT_FALSE(calc.whoami({}).message_id().empty());
}

TEST_F(ServiceTest, InstanceNamedServicesShareOneContract) {
  auto a = std::make_shared<pbtesting::InstanceCalc>(*ctx, "pbtest.Calc.inst1");
  a->init();
  services.push_back(a);
  EXPECT_EQ(a->contractServiceName(), "pbtest.Calc");
  auto inst = proxy("pbtest.Calc.inst1");
  EXPECT_EQ(inst.raw().contractServiceName(), "pbtest.Calc");
  EXPECT_EQ(inst.whoami({}).routing_key(), "REQUEST.pbtest.Calc.inst1.whoami");
  EXPECT_EQ(inst.add(add(1, 2)).result(), 3);
}

TEST_F(ServiceTest, NothingBoundFailsFastAsUnroutable) {
  auto calc = proxy();
  EXPECT_THROW(calc.add(add(1, 1)), protobus::UnroutableError);
}

TEST_F(ServiceTest, NoReplyInTimeIsAnRpcTimeout) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 0;
  serve(o);
  auto calc = proxy();
  pbtest::SlowRequest r;
  r.set_ms(1000);
  protobus::CallOptions call;
  call.timeoutMs = 50;
  EXPECT_THROW(calc.slow(r, call), protobus::RpcTimeoutError);
}

TEST_F(ServiceTest, FireAndForgetReturnsOnceConfirmed) {
  auto svc = serve();
  auto calc = proxy();
  protobus::CallOptions o;
  o.rpc = false;
  pbtest::FailRequest f;
  f.set_handled(true);
  calc.fail(f, o);
  EXPECT_TRUE(eventually([&] { return svc->failAttempts.load() == 1; }));
}

TEST_F(ServiceTest, CallOptionsAreValidatedBeforeAnythingIsSent) {
  serve();
  auto calc = proxy();
  protobus::CallOptions blank;
  blank.messageId = "   ";
  EXPECT_THROW(calc.add(add(1, 1), blank), protobus::InvalidMessageIdError);
  protobus::CallOptions longId;
  longId.messageId = std::string(256, 'x');
  EXPECT_THROW(calc.add(add(1, 1), longId), protobus::InvalidMessageIdError);
  protobus::CallOptions badPriority;
  badPriority.priority = 256;
  EXPECT_THROW(calc.add(add(1, 1), badPriority), protobus::InvalidPriorityError);
  protobus::CallOptions priority;
  priority.priority = protobus::Config::PRIORITY_CONTROL;
  // On a plain queue a priority is simply ignored.
  EXPECT_EQ(calc.add(add(1, 1), priority).result(), 2);
}

TEST_F(ServiceTest, PriorityQueuesAreOptIn) {
  protobus::MessageServiceOptions o;
  o.maxPriority = protobus::Config::RECOMMENDED_MAX_PRIORITY;
  serve(o);
  EXPECT_EQ(broker->queueArguments("pbtest.Calc")->at("x-max-priority").asInt(), 2);

  protobus::MessageServiceOptions early;
  early.maxPriority = 2;
  early.lateAck = false;
  EXPECT_THROW(CalcService(*ctx, early), protobus::InvalidPriorityError);
  protobus::MessageServiceOptions zero;
  zero.maxPriority = 0;
  EXPECT_THROW(CalcService(*ctx, zero), protobus::InvalidPriorityError);
}

TEST_F(ServiceTest, ChangedRetryDelayIsReportedAsAMismatch) {
  serve();
  auto other = newContext();
  protobus::MessageServiceOptions o;
  o.retry.retryDelayMs = 1234;
  auto second = std::make_shared<CalcService>(*other, o);
  EXPECT_THROW(second->init(), protobus::RetryQueueMismatchError);
}

TEST_F(ServiceTest, CustomTypesRoundTrip) {
  serve();
  auto calc = proxy();
  pbtest::Wallet w;
  *w.mutable_amount() = protobus::makeBigint("1000000000000000000000000000000");
  *w.mutable_at() = protobus::makeTimestampMs(1577836800000);
  (*w.mutable_balances())["k"] = protobus::makeBigint(protobus::Uint256::parse("1606938044258990275541962092341162602522202993782792835301376"));
  *w.add_parts() = protobus::makeBigint(protobus::Uint256(1));
  w.set_kind(pbtest::KIND_SPOT);
  w.set_big(9007199254740993);
  auto back = calc.echo(w);
  EXPECT_EQ(protobus::toUint256(back.amount()).toString(), "1000000000000000000000000000000");
  EXPECT_EQ(back.at().value(), 1577836800000);
  EXPECT_EQ(protobus::toUint256(back.balances().at("k")).toString(),
            "1606938044258990275541962092341162602522202993782792835301376");
  EXPECT_EQ(back.kind(), pbtest::KIND_SPOT);
  EXPECT_EQ(back.big(), 9007199254740993);
}

TEST_F(ServiceTest, AnOversizedBigintIsRefusedBeforeItIsSent) {
  serve();
  auto calc = proxy();
  pbtest::Wallet w;
  w.mutable_amount()->set_value(std::string(33, '\x01'));
  EXPECT_THROW(calc.echo(w), protobus::InvalidRequestError);
}

// ---- dispatch security ---------------------------------------------------------

class DispatchTest : public MemoryBus {
 protected:
  // Publish a hand-built request and decode the reply.
  protobus::DecodedResponse raw(const std::string& routingKey, const std::string& body) {
    return protobus::MessageFactory::decodeResponse(ctx->publishMessage(body, routingKey));
  }
  static std::string request(const std::string& method, const google::protobuf::Message& m) {
    return protobus::wire::encodeRequest({method, "", m.SerializeAsString()});
  }
};

TEST_F(DispatchTest, TheBodyCannotPickAMethodOtherThanTheRoutingKey) {
  serve();
  auto r = raw("REQUEST.pbtest.Calc.add", request("pbtest.Calc.divide", pbtest::DivideRequest()));
  ASSERT_TRUE(r.isError());
  EXPECT_EQ(r.errorCode, "PROTOCOL_ERROR");
  EXPECT_NE(r.errorMessage->find("contradicts routing key"), std::string::npos);
  // Reported against the method the routing key names.
  EXPECT_EQ(r.errorMethod, "pbtest.Calc.add");
}

TEST_F(DispatchTest, TheBodyMustNameThisContract) {
  serve();
  auto r = raw("REQUEST.pbtest.Calc.add", request("deep.pkg.Store.add", pbtest::AddRequest()));
  ASSERT_TRUE(r.isError());
  EXPECT_EQ(r.errorCode, "PROTOCOL_ERROR");
  EXPECT_NE(r.errorMessage->find("is not a method of pbtest.Calc"), std::string::npos);
}

TEST_F(DispatchTest, AnUndeclaredMethodIsRejected) {
  serve();
  auto r = raw("REQUEST.pbtest.Calc.nope", request("pbtest.Calc.nope", pbtest::AddRequest()));
  ASSERT_TRUE(r.isError());
  EXPECT_NE(r.errorMessage->find("declares no method nope"), std::string::npos);
}

TEST_F(DispatchTest, MalformedBytesAreAnsweredNotRetried) {
  auto svc = serve();
  auto envelope = raw("REQUEST.pbtest.Calc.add", std::string("\x0a\xff", 2));
  ASSERT_TRUE(envelope.isError());
  EXPECT_EQ(envelope.errorCode, "PROTOCOL_ERROR");
  EXPECT_EQ(envelope.errorMessage, "request envelope did not decode");

  auto payload = raw("REQUEST.pbtest.Calc.add",
                     protobus::wire::encodeRequest({"pbtest.Calc.add", "", std::string("\x08\x80", 2)}));
  ASSERT_TRUE(payload.isError());
  EXPECT_EQ(payload.errorCode, "PROTOCOL_ERROR");
  EXPECT_NE(payload.errorMessage->find("payload did not decode"), std::string::npos);
  EXPECT_EQ(broker->queueDepth("pbtest.Calc.Retry"), 0u);
  EXPECT_EQ(broker->queueDepth("pbtest.Calc.DLQ"), 0u);
}

// ---- the dynamic API -------------------------------------------------------------

TEST_F(ServiceTest, DynamicProxyCallsATypedService) {
  serve();
  protobus::ServiceProxy calc(*ctx, "pbtest.Calc");
  calc.init();
  EXPECT_TRUE(calc.isStreaming("ticks"));
  auto req = ctx->factory().newMessage("pbtest.AddRequest");
  req->GetReflection()->SetInt32(req.get(), req->GetDescriptor()->FindFieldByName("a"), 4);
  req->GetReflection()->SetInt32(req.get(), req->GetDescriptor()->FindFieldByName("b"), 5);
  auto res = calc.call("add", *req);
  EXPECT_EQ(res->GetReflection()->GetInt32(*res, res->GetDescriptor()->FindFieldByName("result")), 9);
  EXPECT_THROW(calc.call("nope", *req), protobus::UnknownMethodError);
  EXPECT_THROW(calc.call("ticks", *req), protobus::InvalidRequestError);
  // A request of the wrong type is refused rather than sent.
  EXPECT_THROW(calc.call("add", pbtest::Nothing()), protobus::InvalidRequestError);
}

class DynamicCalc : public protobus::MessageService {
 public:
  explicit DynamicCalc(protobus::Context& ctx, protobus::MessageServiceOptions o = {}) : MessageService(ctx, o) {
    registerMethod("add", [this](const google::protobuf::Message& req, protobus::CallContext&) {
      const auto* d = req.GetDescriptor();
      const int a = req.GetReflection()->GetInt32(req, d->FindFieldByName("a"));
      const int b = req.GetReflection()->GetInt32(req, d->FindFieldByName("b"));
      auto res = context().factory().newMessage("pbtest.AddResponse");
      res->GetReflection()->SetInt32(res.get(), res->GetDescriptor()->FindFieldByName("result"), a * b);
      return res;
    });
  }
  std::string ServiceName() const override { return "pbtest.Calc"; }
};

TEST_F(ServiceTest, DynamicServiceAnswersATypedProxy) {
  serve<DynamicCalc>();
  auto calc = proxy();
  EXPECT_EQ(calc.add(add(6, 7)).result(), 42);
  // Registered rpcs only: the rest answer PROTOCOL_ERROR.
  try {
    calc.whoami({});
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROTOCOL_ERROR");
    EXPECT_STREQ(e.what(), "invalid service method whoami");
  }
}

TEST_F(ServiceTest, AServiceMustBeSharedOwned) {
  CalcService onTheStack(*ctx);
  EXPECT_THROW(onTheStack.init(), std::logic_error);
}

TEST_F(ServiceTest, AServiceWithoutASchemaFailsToStart) {
  class Missing : public protobus::MessageService {
   public:
    using MessageService::MessageService;
    std::string ServiceName() const override { return "nowhere.Service"; }
  };
  Missing m(*ctx);
  EXPECT_THROW(m.init(), protobus::MissingProto);
}

}  // namespace
