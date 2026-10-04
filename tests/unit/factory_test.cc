#include <gtest/gtest.h>

#include <google/protobuf/descriptor.h>

#include <filesystem>
#include <fstream>

#include "helpers.h"

namespace {

namespace fs = std::filesystem;

class FactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    protobus::setLogLevel(protobus::LogLevel::Silent);
    dir = fs::temp_directory_path() / ("pbcpp-factory-" + std::to_string(::getpid()) + "-" +
                                       ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(dir);
    fs::create_directories(dir);
  }
  void TearDown() override {
    fs::remove_all(dir);
    protobus::setLogLevel(protobus::LogLevel::Info);
  }
  void write(const std::string& name, const std::string& text) {
    fs::create_directories((dir / name).parent_path());
    std::ofstream(dir / name) << text;
  }
  fs::path dir;
};

const char* kShop = R"(
syntax = "proto3";
package shop;
service Orders {
  rpc place(Order) returns (Receipt);
  rpc watch(Order) returns (stream Receipt);
}
message Order { string id = 1; bigint amount = 2; timestamp at = 3; }
message Receipt { string id = 1; map<string, bigint> totals = 2; }
)";

TEST_F(FactoryTest, ParsesASchemaUsingTheCustomTypesWithoutAnImport) {
  protobus::MessageFactory f;
  f.init({});
  f.parse(kShop, "shop.Orders");
  EXPECT_TRUE(f.hasService("shop.Orders"));
  EXPECT_TRUE(f.isStreamingMethod("shop.Orders.watch"));
  EXPECT_FALSE(f.isStreamingMethod("shop.Orders.place"));
  EXPECT_FALSE(f.isStreamingMethod("shop.Orders.nope"));
  EXPECT_EQ(f.getServiceMethodNames("shop.Orders"), (std::vector<std::string>{"place", "watch"}));

  auto order = f.newMessage("shop.Order");
  const auto* d = order->GetDescriptor();
  EXPECT_EQ(d->FindFieldByName("amount")->message_type()->full_name(), "bigint");
  EXPECT_EQ(d->FindFieldByName("at")->message_type()->full_name(), "timestamp");
}

TEST_F(FactoryTest, EncodesAndDecodesDynamicMessagesByteForByteWithGeneratedOnes) {
  protobus::MessageFactory f;
  f.init({});
  // A runtime copy of a schema that is also compiled in.
  pbtest::Wallet typed;
  *typed.mutable_amount() = protobus::makeBigint(protobus::Uint256(42));
  (*typed.mutable_balances())["a"] = protobus::makeBigint(protobus::Uint256(1));
  auto dynamic = f.decodeMessage("pbtest.Wallet", typed.SerializeAsString());
  EXPECT_EQ(protobus::MessageFactory::encodeMessage(*dynamic), typed.SerializeAsString());
}

TEST_F(FactoryTest, DecodingRefusesAnOversizedBigint) {
  protobus::MessageFactory f;
  f.init({});
  pbtest::Wallet w;
  w.mutable_amount()->set_value(std::string(33, '\x02'));
  EXPECT_THROW(f.decodeMessage("pbtest.Wallet", w.SerializeAsString()), protobus::CustomTypeRangeError);
}

TEST_F(FactoryTest, ParsingIsIdempotent) {
  protobus::MessageFactory f;
  f.init({});
  f.parse(kShop, "shop.Orders");
  EXPECT_NO_THROW(f.parse(kShop, "shop.Orders"));
  EXPECT_NO_THROW(f.parse(kShop, "shop.Orders.instance7"));
  EXPECT_NO_THROW(f.parse(kShop));
}

TEST_F(FactoryTest, ASchemaAlreadyCompiledInIsSkipped) {
  protobus::MessageFactory f;
  f.init({});
  std::ifstream in(std::string(PROTOBUS_TEST_PROTO_DIR) + "/pbtest.proto");
  std::stringstream text;
  text << in.rdbuf();
  EXPECT_NO_THROW(f.parse(text.str(), "pbtest.Calc"));
  EXPECT_NO_THROW(f.init({PROTOBUS_TEST_PROTO_DIR}));
  EXPECT_TRUE(f.hasService("pbtest.Calc"));
  EXPECT_TRUE(f.hasService("deep.pkg.Store"));
}

TEST_F(FactoryTest, APartialRedefinitionIsAConflict) {
  protobus::MessageFactory f;
  f.init({});
  EXPECT_THROW(f.parse("syntax = \"proto3\"; package pbtest; message Nothing {} message Brand {}"),
               protobus::SchemaError);
}

TEST_F(FactoryTest, ASyntaxErrorNamesTheModule) {
  protobus::MessageFactory f;
  f.init({});
  try {
    f.parse("syntax = \"proto3\"; message {", "broken.Service");
    FAIL();
  } catch (const protobus::SchemaError& e) {
    EXPECT_NE(std::string(e.what()).find("broken.Service"), std::string::npos);
  }
}

TEST_F(FactoryTest, ParsingBeforeInitIsRefused) {
  protobus::MessageFactory f;
  EXPECT_THROW(f.parse(kShop, "shop.Orders"), protobus::NotInitializedError);
}

TEST_F(FactoryTest, LoadsADirectoryWhoseFilesImportEachOther) {
  write("common/money.proto", "syntax = \"proto3\"; package common; message Money { bigint units = 1; }");
  write("billing/invoice.proto",
        "syntax = \"proto3\"; package billing; import \"common/money.proto\";\n"
        "service Invoices { rpc total(Invoice) returns (common.Money); }\n"
        "message Invoice { repeated common.Money lines = 1; timestamp due = 2; }");
  write("notes.protocol.txt", "not a schema");
  protobus::MessageFactory f;
  f.init({dir.string()});
  EXPECT_TRUE(f.hasService("billing.Invoices"));
  EXPECT_TRUE(f.hasType("common.Money"));
}

TEST_F(FactoryTest, ARegisteredCustomTypeIsDefinedForSchemasThatUseIt) {
  protobus::MessageFactory f;
  f.init({});
  f.registerType({"uuid", protobus::CustomWireType::Bytes});
  f.parse("syntax = \"proto3\"; package ids; message Tagged { uuid id = 1; }", "ids");
  auto m = f.newMessage("ids.Tagged");
  const auto* field = m->GetDescriptor()->FindFieldByName("id");
  ASSERT_NE(field->message_type(), nullptr);
  EXPECT_EQ(field->message_type()->full_name(), "uuid");
  EXPECT_EQ(field->message_type()->FindFieldByName("value")->type(), google::protobuf::FieldDescriptor::TYPE_BYTES);
}

TEST_F(FactoryTest, MethodNamesSplitFromTheRight) {
  auto [svc, method] = protobus::MessageFactory::splitMethodName("com.example.Calc.add");
  EXPECT_EQ(svc, "com.example.Calc");
  EXPECT_EQ(method, "add");
  EXPECT_THROW(protobus::MessageFactory::splitMethodName("add"), protobus::InvalidMethodNameError);
  EXPECT_THROW(protobus::MessageFactory::splitMethodName("Calc."), protobus::InvalidMethodNameError);
  EXPECT_THROW(protobus::MessageFactory::splitMethodName(".add"), protobus::InvalidMethodNameError);
}

TEST_F(FactoryTest, ARequestOfTheWrongTypeIsRefused) {
  protobus::MessageFactory f;
  f.init({});
  EXPECT_THROW(f.buildRequest("pbtest.Calc.add", pbtest::Nothing()), protobus::InvalidRequestError);
  EXPECT_THROW(f.buildRequest("pbtest.Calc.nope", pbtest::Nothing()), protobus::UnknownMethodError);
  EXPECT_NO_THROW(f.buildRequest("pbtest.Calc.add", pbtest::AddRequest()));
}

}  // namespace
