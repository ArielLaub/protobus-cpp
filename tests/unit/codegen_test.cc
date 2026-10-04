// The generated code is compiled, not string-matched: the suites' own schema
// goes through the CLI and protoc into this binary, and the stub that
// `generate:service` writes is compiled here against the real headers.
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "deep/nested.protobus.h"
#include "helpers.h"

namespace {

namespace fs = std::filesystem;
using pbtesting::MemoryBus;

int run(const std::string& cmd, std::string* output = nullptr) {
  const std::string full = cmd + " 2>&1";
  FILE* p = ::popen(full.c_str(), "r");
  std::string out;
  char buf[4096];
  while (size_t n = std::fread(buf, 1, sizeof buf, p)) out.append(buf, n);
  const int status = ::pclose(p);
  if (output) *output = out;
  return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

class Store : public deep::pkg::StoreBase {
 public:
  using StoreBase::StoreBase;
  deep::pkg::Item delete_(const deep::pkg::Key& k, protobus::CallContext&) override {
    deep::pkg::Item i;
    i.set_id("deleted:" + k.id());
    return i;
  }
  deep::pkg::Item init_(const deep::pkg::Key& k, protobus::CallContext&) override {
    deep::pkg::Item i;
    i.set_id("init:" + k.id());
    *i.mutable_meta()->mutable_created() = protobus::makeTimestampMs(1000);
    return i;
  }
};

using GeneratedCodeTest = MemoryBus;

TEST_F(GeneratedCodeTest, RpcsNamedLikeKeywordsAndFrameworkMembersStillRoute) {
  auto s = std::make_shared<Store>(*ctx);
  s->init();
  services.push_back(s);
  deep::pkg::StoreProxy store(*ctx);
  store.init();
  deep::pkg::Key k;
  k.set_id("7");
  EXPECT_EQ(store.delete_(k).id(), "deleted:7");
  auto item = store.init_(k);
  EXPECT_EQ(item.id(), "init:7");
  EXPECT_EQ(item.meta().created().value(), 1000);
  // Left unimplemented: PROTOCOL_ERROR, for streams as for unary rpcs.
  try {
    for (const auto& i : store.list(k)) (void)i;
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROTOCOL_ERROR");
  }
}

TEST(Cli, ReportsItsVersionAndUsage) {
  std::string out;
  EXPECT_EQ(run(std::string(PROTOBUS_CLI) + " --version", &out), 0);
  EXPECT_EQ(out, std::string(protobus::kVersion) + "\n");
  EXPECT_EQ(run(std::string(PROTOBUS_CLI) + " --help", &out), 0);
  EXPECT_NE(out.find("generate:service"), std::string::npos);
  EXPECT_NE(run(std::string(PROTOBUS_CLI) + " frobnicate", &out), 0);
}

TEST(Cli, RefusesAServiceNameThatCouldEscapeItsDirectory) {
  std::string out;
  for (const char* bad : {"../evil", "a/b", "", "has space"}) {
    EXPECT_NE(run(std::string(PROTOBUS_CLI) + " generate:service '" + bad + "' --proto-dir " +
                      PROTOBUS_TEST_PROTO_DIR + " --services-dir /tmp/pbcpp-never",
                  &out),
              0)
        << bad;
    EXPECT_NE(out.find("invalid service name"), std::string::npos) << out;
  }
}

TEST(Cli, TheServiceStubCompiles) {
  const fs::path dir = fs::temp_directory_path() / ("pbcpp-stub-" + std::to_string(::getpid()));
  fs::remove_all(dir);
  const fs::path gen = dir / "gen";
  std::string out;
  ASSERT_EQ(run(std::string(PROTOBUS_CLI) + " generate --proto-dir " + PROTOBUS_TEST_PROTO_DIR + " --out " +
                    gen.string() + " --protoc " + PROTOBUS_PROTOC,
                &out),
            0)
      << out;
  EXPECT_TRUE(fs::exists(gen / "pbtest.protobus.h"));
  EXPECT_TRUE(fs::exists(gen / "deep" / "nested.pb.cc"));

  ASSERT_EQ(run(std::string(PROTOBUS_CLI) + " generate:service pbtest --proto-dir " + PROTOBUS_TEST_PROTO_DIR +
                    " --services-dir " + (dir / "services").string(),
                &out),
            0)
      << out;
  const fs::path stub = dir / "services" / "pbtest" / "pbtestService.cc";
  ASSERT_TRUE(fs::exists(stub)) << out;
  std::ifstream in(stub);
  std::stringstream text;
  text << in.rdbuf();
  EXPECT_NE(text.str().find("class CalcService : public pbtest::CalcBase"), std::string::npos) << text.str();
  EXPECT_NE(text.str().find("protobus::Generator<pbtest::Tick> ticks("), std::string::npos);

  // Refuses to overwrite.
  EXPECT_NE(run(std::string(PROTOBUS_CLI) + " generate:service pbtest --proto-dir " + PROTOBUS_TEST_PROTO_DIR +
                    " --services-dir " + (dir / "services").string(),
                &out),
            0);
  EXPECT_NE(out.find("already exists"), std::string::npos);

  const std::string compile = std::string(PROTOBUS_CXX) + " -std=c++20 -fsyntax-only -Wall -Wextra -Werror " +
                              "-Wno-unused-parameter -Wno-nullability-extension -I" + PROTOBUS_SOURCE_DIR +
                              "/include -I" + PROTOBUS_GENERATED_DIR + " -I" + gen.string() + " " +
                              PROTOBUS_EXTRA_INCLUDES + " " + stub.string();
  EXPECT_EQ(run(compile, &out), 0) << compile << "\n" << out;
  fs::remove_all(dir);
}

TEST(Cli, ACustomTypeIsDeclaredForTheSchemasThatUseIt) {
  const fs::path dir = fs::temp_directory_path() / ("pbcpp-custom-" + std::to_string(::getpid()));
  fs::remove_all(dir);
  fs::create_directories(dir / "proto");
  std::ofstream(dir / "proto" / "ids.proto")
      << "syntax = \"proto3\";\npackage ids;\nmessage Tagged { uuid id = 1; bigint n = 2; }\n";
  std::string out;
  ASSERT_EQ(run(std::string(PROTOBUS_CLI) + " generate --custom-type uuid:bytes --proto-dir " +
                    (dir / "proto").string() + " --out " + (dir / "gen").string() + " --protoc " + PROTOBUS_PROTOC,
                &out),
            0)
      << out;
  EXPECT_TRUE(fs::exists(dir / "gen" / "protobus" / "custom" / "uuid.pb.h"));
  std::ifstream in(dir / "gen" / "ids.pb.h");
  std::stringstream text;
  text << in.rdbuf();
  EXPECT_NE(text.str().find("protobus/custom/uuid.pb.h"), std::string::npos);
  EXPECT_NE(text.str().find("protobus/types.pb.h"), std::string::npos);
  fs::remove_all(dir);
}

}  // namespace
