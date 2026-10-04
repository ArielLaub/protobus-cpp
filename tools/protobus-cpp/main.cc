// protobus-cpp: generate C++ code from protobus schemas.
//
//   protobus-cpp generate [--proto-dir DIR]... [--out DIR] [--custom-type NAME:WIRETYPE]...
//   protobus-cpp generate:service <Name> [--proto-dir DIR] [--services-dir DIR]
//   protobus-cpp init
//
// `generate` stages the schemas with the protobus custom-type imports added
// where they are used (shared schemas do not import them), then runs protoc
// with --cpp_out and this program as the protobus plugin. protoc is found
// with --protoc, then $PROTOC, then on the PATH; use the protoc of the
// protobuf the program links against.
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "generator.h"
#include "protobuf_compat.h"
#include "protobus/custom_types.h"
#include "protobus/version.h"
#include "types_proto.h"

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

extern char** environ;

namespace fs = std::filesystem;
namespace gpb = google::protobuf;

namespace {

const char* kHelp = R"(protobus-cpp - code generation for protobus C++

Usage:
  protobus-cpp generate [options]               Generate C++ code from .proto files
  protobus-cpp generate:service <Name> [options] Generate a service stub from <Name>.proto
  protobus-cpp init                             Show project setup instructions
  protobus-cpp --help | --version

Options:
  --proto-dir DIR        Schema directory (repeatable; default ./proto)
  --out DIR              Output directory for generated code (default ./gen)
  --services-dir DIR     Where generate:service writes stubs (default ./services)
  --custom-type N:WIRE   Declare a custom type, e.g. uuid:bytes (repeatable).
                         WIRE is one of bytes, int64, uint64, string, int32,
                         uint32, double.
  --protoc PATH          The protoc to run (default $PROTOC, then PATH)

Every schema is shared verbatim with the TypeScript, Python and Go ports: it
needs no C++ options and no import for the custom types.
)";

const char* kInit = R"(
Protobus C++ Project Setup
==========================

1. Lay out the project:

   mkdir -p proto services

2. Write a schema, proto/Calculator.proto:

   syntax = "proto3";
   package Calculator;

   service Service {
     rpc add(AddRequest) returns (AddResponse);
   }

   message AddRequest { int32 a = 1; int32 b = 2; }
   message AddResponse { int32 result = 1; }

3. In CMakeLists.txt:

   find_package(protobus CONFIG REQUIRED)
   add_executable(calculator services/calculator/CalculatorService.cc)
   protobus_generate(TARGET calculator PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
   target_link_libraries(calculator PRIVATE protobus::protobus)

4. Generate a stub to start from:

   protobus-cpp generate:service Calculator

5. Run a broker and the service:

   docker run -d -p 5672:5672 rabbitmq:3-management
   AMQP_URL=amqp://guest:guest@localhost:5672/ ./build/calculator
)";

struct Options {
  std::vector<std::string> protoDirs;
  std::string out = "gen";
  std::string servicesDir = "services";
  std::string protoc;
  std::vector<std::string> positional;
};

[[noreturn]] void fail(const std::string& message) {
  std::cerr << "protobus-cpp: " << message << "\n";
  std::exit(1);
}

protobus::CustomWireType parseWireType(const std::string& w) {
  static const std::map<std::string, protobus::CustomWireType> types = {
      {"bytes", protobus::CustomWireType::Bytes},   {"int64", protobus::CustomWireType::Int64},
      {"uint64", protobus::CustomWireType::Uint64}, {"string", protobus::CustomWireType::String},
      {"int32", protobus::CustomWireType::Int32},   {"uint32", protobus::CustomWireType::Uint32},
      {"double", protobus::CustomWireType::Double},
  };
  auto it = types.find(w);
  if (it == types.end()) fail("unknown custom type wire type '" + w + "'");
  return it->second;
}

Options parseArgs(int argc, char** argv, int first) {
  Options o;
  for (int i = first; i < argc; ++i) {
    const std::string a = argv[i];
    auto value = [&](const char* flag) -> std::string {
      const std::string prefix = std::string(flag) + "=";
      if (a.rfind(prefix, 0) == 0) return a.substr(prefix.size());
      if (i + 1 >= argc) fail(std::string(flag) + " needs a value");
      return argv[++i];
    };
    auto is = [&](const char* flag) { return a == flag || a.rfind(std::string(flag) + "=", 0) == 0; };
    if (is("--proto-dir")) {
      o.protoDirs.push_back(value("--proto-dir"));
    } else if (is("--out")) {
      o.out = value("--out");
    } else if (is("--services-dir")) {
      o.servicesDir = value("--services-dir");
    } else if (is("--protoc")) {
      o.protoc = value("--protoc");
    } else if (is("--custom-type")) {
      const std::string spec = value("--custom-type");
      const auto colon = spec.find(':');
      if (colon == std::string::npos) fail("--custom-type takes NAME:WIRETYPE, got '" + spec + "'");
      try {
        protobus::registerCustomType({spec.substr(0, colon), parseWireType(spec.substr(colon + 1))});
      } catch (const std::exception& e) {
        fail(e.what());
      }
    } else if (a.rfind("--", 0) == 0) {
      fail("unknown option " + a);
    } else {
      o.positional.push_back(a);
    }
  }
  if (o.protoDirs.empty()) o.protoDirs.push_back("proto");
  return o;
}

std::string readFile(const fs::path& p) {
  std::ifstream in(p, std::ios::binary);
  if (!in) fail("cannot read " + p.string());
  std::stringstream buf;
  buf << in.rdbuf();
  return buf.str();
}

void writeFile(const fs::path& p, const std::string& text) {
  fs::create_directories(p.parent_path());
  std::ofstream out(p, std::ios::binary);
  if (!out) fail("cannot write " + p.string());
  out << text;
}

class StderrErrors : public gpb::io::ErrorCollector {
 public:
  explicit StderrErrors(std::string file) : file_(std::move(file)) {}
  void PROTOBUS_PB_ERROR(int line, gpb::io::ColumnNumber column, PROTOBUS_PB_TEXT message) override {
    std::cerr << file_ << ":" << line + 1 << ":" << column + 1 << ": " << message << "\n";
    failed = true;
  }
  bool failed = false;

 private:
  std::string file_;
};

void collectTypes(const gpb::DescriptorProto& m, std::set<std::string>& used, std::set<std::string>& declared) {
  declared.insert(m.name());
  for (const auto& f : m.field()) {
    if (!f.type_name().empty()) used.insert(f.type_name());
  }
  for (const auto& n : m.nested_type()) collectTypes(n, used, declared);
}

struct Staged {
  fs::path dir;
  std::vector<std::string> files;        // schema files, relative to dir
  std::vector<std::string> customFiles;  // custom-type files to generate
};

// The import a custom type is defined in.
std::string importFor(const protobus::CustomType& t) {
  if (t.name == "bigint" || t.name == "timestamp") return "protobus/types.proto";
  return "protobus/custom/" + t.name + ".proto";
}

// Copy every schema into a staging directory, adding the import of each
// custom type a file uses without declaring, and write the custom types'
// own definitions beside them.
Staged stage(const Options& o) {
  Staged s;
  s.dir = fs::temp_directory_path() / ("protobus-cpp-" + std::to_string(::getpid()));
  fs::remove_all(s.dir);
  fs::create_directories(s.dir);
  writeFile(s.dir / "protobus" / "types.proto", protobus::codegen::kTypesProto);

  for (const auto& root : o.protoDirs) {
    if (!fs::is_directory(root)) fail("proto directory not found: " + root);
    std::vector<fs::path> paths;
    for (const auto& e : fs::recursive_directory_iterator(root)) {
      if (e.is_regular_file() && e.path().extension() == ".proto") paths.push_back(e.path());
    }
    std::sort(paths.begin(), paths.end());
    for (const auto& p : paths) {
      const std::string rel = fs::relative(p, root).generic_string();
      if (rel.rfind("protobus/", 0) == 0) continue;
      std::string text = readFile(p);

      gpb::io::ArrayInputStream input(text.data(), static_cast<int>(text.size()));
      StderrErrors errors(p.string());
      gpb::io::Tokenizer tokenizer(&input, &errors);
      gpb::compiler::Parser parser;
      parser.RecordErrorsTo(&errors);
      gpb::FileDescriptorProto file;
      if (!parser.Parse(&tokenizer, &file) || errors.failed) fail("failed to parse " + p.string());

      std::set<std::string> used;
      std::set<std::string> declared;
      for (const auto& m : file.message_type()) collectTypes(m, used, declared);
      std::string imports;
      std::set<std::string> added;
      for (const auto& t : protobus::getCustomTypes()) {
        const bool uses = used.count(t.name) || used.count("." + t.name);
        const bool local = declared.count(t.name) && file.package().empty();
        if (!uses || local) continue;
        const std::string dep = importFor(t);
        if (std::find(file.dependency().begin(), file.dependency().end(), dep) != file.dependency().end()) continue;
        if (!added.insert(dep).second) continue;
        imports += "import \"" + dep + "\";\n";
      }
      if (!imports.empty()) {
        // After the syntax (or edition) statement, which must come first.
        static const std::regex decl(R"((syntax|edition)\s*=\s*"[^"]*"\s*;)");
        std::smatch match;
        if (std::regex_search(text, match, decl)) {
          const auto end = match.position(0) + match.length(0);
          text = text.substr(0, end) + "\n" + imports + text.substr(end);
        } else {
          text = imports + text;
        }
      }
      writeFile(s.dir / rel, text);
      s.files.push_back(rel);
    }
  }
  // Every declared custom type is generated, used or not, so a build knows
  // its outputs in advance.
  for (const auto& t : protobus::getCustomTypes()) {
    if (t.name == "bigint" || t.name == "timestamp") continue;
    const std::string rel = importFor(t);
    writeFile(s.dir / rel, "syntax = \"proto3\";\n\nmessage " + t.name + " {\n  optional " +
                               protobus::toString(t.wireType) + " value = 1;\n}\n");
    s.customFiles.push_back(rel);
  }
  return s;
}

std::string selfPath(const char* argv0) {
#if defined(__APPLE__)
  char buf[4096];
  uint32_t size = sizeof buf;
  if (_NSGetExecutablePath(buf, &size) == 0) return fs::canonical(buf).string();
#elif defined(__linux__)
  std::error_code ec;
  auto p = fs::read_symlink("/proc/self/exe", ec);
  if (!ec) return p.string();
#endif
  return fs::absolute(argv0).string();
}

int run(const std::vector<std::string>& args) {
  std::vector<char*> argv;
  for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
  argv.push_back(nullptr);
  pid_t pid;
  if (posix_spawnp(&pid, argv[0], nullptr, nullptr, argv.data(), environ) != 0) {
    fail("cannot run " + args[0] + " (set --protoc or $PROTOC)");
  }
  int status = 0;
  waitpid(pid, &status, 0);
  return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

int generate(const Options& o, const char* argv0) {
  Staged s = stage(o);
  if (s.files.empty()) fail("no .proto files found");
  fs::create_directories(o.out);
  std::string protoc = o.protoc;
  if (protoc.empty() && std::getenv("PROTOC")) protoc = std::getenv("PROTOC");
  if (protoc.empty()) protoc = "protoc";

  ::setenv("PROTOBUS_CPP_PLUGIN", "1", 1);
  std::vector<std::string> args = {protoc,
                                   "-I" + s.dir.string(),
                                   "--cpp_out=" + fs::absolute(o.out).string(),
                                   "--protobus-cpp_out=" + fs::absolute(o.out).string(),
                                   "--plugin=protoc-gen-protobus-cpp=" + selfPath(argv0)};
  for (const auto& f : s.files) args.push_back(f);
  for (const auto& f : s.customFiles) args.push_back(f);
  const int code = run(args);
  ::unsetenv("PROTOBUS_CPP_PLUGIN");
  fs::remove_all(s.dir);
  if (code != 0) return code;
  std::cout << "generated " << s.files.size() + s.customFiles.size() << " schema(s) into " << o.out << "\n";
  return 0;
}

// The name argument is joined onto paths, so it must not be able to name
// anything outside them.
void assertSafeServiceName(const std::string& name) {
  if (name.empty()) fail("invalid service name \"\": must be a non-empty string");
  if (name.size() > 100) fail("invalid service name \"" + name + "\": must be 100 characters or fewer");
  for (unsigned char c : name) {
    if (!(std::isalnum(c) || c == '_' || c == '-')) {
      fail("invalid service name \"" + name + "\": may contain only letters, digits, underscore and hyphen");
    }
  }
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
  return s;
}

std::string cppType(const gpb::Descriptor* d) {
  std::string pkg(d->file()->package());
  std::string name(d->full_name());
  std::string local = pkg.empty() ? name : name.substr(pkg.size() + 1);
  std::replace(local.begin(), local.end(), '.', '_');
  return (pkg.empty() ? std::string() : protobus::codegen::packageNamespace(pkg) + "::") + local;
}

int generateService(const Options& o) {
  if (o.positional.size() != 1) fail("usage: protobus-cpp generate:service <Name>");
  const std::string name = o.positional[0];
  assertSafeServiceName(name);
  Staged s = stage(o);
  const std::string target = name + ".proto";
  if (std::find(s.files.begin(), s.files.end(), target) == s.files.end()) {
    fail("Proto file not found: " + (fs::path(o.protoDirs.front()) / target).string());
  }

  gpb::compiler::DiskSourceTree tree;
  tree.MapPath("", s.dir.string());
  struct Errors : gpb::compiler::MultiFileErrorCollector {
    void PROTOBUS_PB_ERROR(PROTOBUS_PB_TEXT file, int line, int column, PROTOBUS_PB_TEXT message) override {
      std::cerr << file << ":" << line + 1 << ":" << column + 1 << ": " << message << "\n";
    }
  } errors;
  gpb::compiler::Importer importer(&tree, &errors);
  const gpb::FileDescriptor* file = importer.Import(target);
  if (file == nullptr) fail("failed to load " + target);
  if (file->service_count() == 0) fail(target + " declares no service");

  const fs::path dir = fs::path(o.servicesDir) / lower(name);
  const fs::path outFile = dir / (name + "Service.cc");
  if (fs::exists(outFile)) {
    fail("Service file already exists: " + outFile.string() + ". Remove it first if you want to regenerate.");
  }

  std::ostringstream code;
  std::vector<std::string> classes;
  code << "// Generated by protobus-cpp generate:service. Implement the TODO methods.\n"
       << "#include <cstdlib>\n\n#include <protobus/protobus.h>\n\n#include \"" << name << ".protobus.h\"\n";
  for (int i = 0; i < file->service_count(); ++i) {
    const auto* service = file->service(i);
    const std::string pkg(file->package());
    const std::string ns = pkg.empty() ? "" : protobus::codegen::packageNamespace(pkg) + "::";
    const std::string svc(service->name());
    // `service Service` keeps the established class name, <Package>Service.
    const std::string cls = svc == "Service" && !pkg.empty() ? pkg.substr(pkg.rfind('.') + 1) + "Service"
                                                              : svc + "Service";
    classes.push_back(cls);
    code << "\n// " << service->full_name() << " implementation.\n"
         << "class " << cls << " : public " << ns << svc << "Base {\n public:\n  using " << ns << svc << "Base::"
         << svc << "Base;\n";
    for (int m = 0; m < service->method_count(); ++m) {
      const auto* method = service->method(m);
      if (method->client_streaming()) continue;
      const std::string id = protobus::codegen::methodIdentifier(std::string(method->name()));
      const std::string req = cppType(method->input_type());
      const std::string res = cppType(method->output_type());
      code << "\n";
      if (method->server_streaming()) {
        // A server-streaming method is a coroutine: each co_yield is one
        // chunk to the caller.
        code << "  protobus::Generator<" << res << "> " << id << "(const " << req
             << "& request, protobus::CallContext& context) override {\n"
             << "    // TODO: Implement " << method->name() << "\n"
             << "    throw protobus::InvalidMethodError(\"Not implemented: " << method->name() << "\");\n"
             << "    co_return;\n  }\n";
      } else {
        code << "  " << res << " " << id << "(const " << req << "& request, protobus::CallContext& context) override {\n"
             << "    // TODO: Implement " << method->name() << "\n"
             << "    throw protobus::InvalidMethodError(\"Not implemented: " << method->name() << "\");\n  }\n";
      }
    }
    code << "};\n";
  }
  code << "\nint main() {\n"
       << "  const char* url = std::getenv(\"AMQP_URL\");\n"
       << "  protobus::Context context;\n"
       << "  context.init(url ? url : \"amqp://localhost\");\n";
  for (const auto& cls : classes) code << "  protobus::RunnableService::start<" << cls << ">(context);\n";
  code << "  return protobus::RunnableService::wait();\n}\n";

  writeFile(outFile, code.str());
  fs::remove_all(s.dir);
  std::cout << "Service generated: " << outFile.string() << "\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  // protoc runs this program again as the protobus plugin.
  if (std::getenv("PROTOBUS_CPP_PLUGIN") != nullptr && argc == 1) {
    protobus::codegen::ProtobusGenerator generator;
    return gpb::compiler::PluginMain(argc, argv, &generator);
  }
  if (argc < 2) {
    std::cout << kHelp;
    return 0;
  }
  const std::string command = argv[1];
  if (command == "--help" || command == "-h" || command == "help") {
    std::cout << kHelp;
    return 0;
  }
  if (command == "--version" || command == "-v") {
    std::cout << protobus::kVersion << "\n";
    return 0;
  }
  if (command == "init") {
    std::cout << kInit;
    return 0;
  }
  const Options options = parseArgs(argc, argv, 2);
  if (command == "generate") return generate(options, argv[0]);
  if (command == "generate:service") return generateService(options);
  std::cerr << "protobus-cpp: unknown command '" << command << "'\n" << kHelp;
  return 1;
}
