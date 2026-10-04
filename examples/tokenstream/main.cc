// Server-streaming with cancellation, in the shape a chat UI needs: a service
// streams tokens like a language model, and the caller stops it three ways.
//
//   docker compose up -d --wait
//   AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ ./build/examples/tokenstream
//
// After each run the demo asks the SERVER how much it generated. Cancellation
// that only stopped the reader would show the full count; here the producer
// stops, because the caller's cancel reaches it as its signal firing.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#include "chat.protobus.h"

std::vector<std::string> completion(const std::string& prompt) {
  std::string body = "Answering \"" + prompt + "\". ";
  for (int i = 0; i < 3; ++i) {
    body +=
        "Streaming responses arrive one token at a time, which is what lets a chat interface render text as it is "
        "produced rather than waiting for a whole reply. That same property is what makes stopping useful: when "
        "the reader has seen enough, every token after that point is wasted work on the server. ";
  }
  std::vector<std::string> words;
  std::istringstream in(body);
  for (std::string w; in >> w;) words.push_back(w + " ");
  return words;
}

// Streams a long canned completion, one word at a time.
class Assistant : public Chat::AssistantBase {
 public:
  using AssistantBase::AssistantBase;

  protobus::Generator<Chat::Token> generate(const Chat::GenerateRequest& request,
                                            protobus::CallContext& context) override {
    const auto words = completion(request.prompt());
    const auto delay = std::chrono::milliseconds(request.token_delay_ms() > 0 ? request.token_delay_ms() : 60);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.Clear();
    }
    for (size_t i = 0; i < words.size(); ++i) {
      // The signal fires when the caller cancels: it broke out of its loop,
      // aborted its own signal, or went idle. This is where a real service
      // would abort its upstream model call, and the point of the demo:
      // stopping saves the work, not just the reading.
      if (context.signal.waitFor(delay)) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.set_stopped_early(true);
        co_return;
      }
      Chat::Token token;
      token.set_index(static_cast<int32_t>(i));
      token.set_text(words[i]);
      co_yield token;
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.set_tokens_generated(static_cast<int32_t>(i + 1));
    }
  }

  Chat::StatsResponse stats(const Chat::StatsRequest&, protobus::CallContext&) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }

 private:
  std::mutex mutex_;
  Chat::StatsResponse stats_;
};

void header(const std::string& title) {
  const std::string rule(64, '=');
  std::cout << "\n" << rule << "\n" << title << "\n" << rule << std::endl;
}

void report(Chat::AssistantProxy& client) {
  std::this_thread::sleep_for(std::chrono::milliseconds(300));  // let the cancellation travel
  const auto stats = client.stats({});
  std::cout << "  server generated " << stats.tokens_generated() << " tokens; stopped early: "
            << (stats.stopped_early() ? "true" : "false") << std::endl;
}

// Stop lives outside the loop (another thread, a UI handler) and takes effect
// at once rather than at the next token.
void stopButton(Chat::AssistantProxy& client) {
  header("1. Stop button (an AbortSignal)");
  protobus::AbortController stop;
  std::thread button([&stop] {
    std::this_thread::sleep_for(std::chrono::milliseconds(900));
    std::cout << "\n  [user pressed Stop]" << std::endl;
    stop.abort();
  });
  Chat::GenerateRequest request;
  request.set_prompt("why does streaming matter?");
  request.set_token_delay_ms(60);
  protobus::StreamOptions options;
  options.signal = stop.signal();
  std::cout << "  ";
  // A cancelled stream ends rather than raising.
  for (const auto& token : client.generate(request, options)) std::cout << token.text() << std::flush;
  button.join();
  report(client);
}

// The decision is made inside the loop.
void breakOut(Chat::AssistantProxy& client) {
  header("2. break out of the loop");
  Chat::GenerateRequest request;
  request.set_prompt("only the first few words");
  request.set_token_delay_ms(60);
  std::cout << "  ";
  int printed = 0;
  for (const auto& token : client.generate(request)) {
    std::cout << token.text() << std::flush;
    if (++printed == 8) break;
  }
  std::cout << "\n  [consumer stopped reading]" << std::endl;
  report(client);
}

// The control, where nothing cancels.
void runToCompletion(Chat::AssistantProxy& client) {
  header("3. no cancellation");
  Chat::GenerateRequest request;
  request.set_prompt("short answer");
  request.set_token_delay_ms(1);
  int received = 0;
  for (const auto& token : client.generate(request)) {
    (void)token;
    ++received;
  }
  std::cout << "  received " << received << " tokens" << std::endl;
  report(client);
}

int main() {
  const char* url = std::getenv("AMQP_URL");
  protobus::Context context;
  context.init(url ? url : "amqp://guest:guest@127.0.0.1:25672/");

  // A streaming handler holds its prefetch slot for the life of its stream,
  // so concurrency is how many callers are served at once.
  protobus::MessageServiceOptions options;
  options.maxConcurrent = 8;
  auto assistant = std::make_shared<Assistant>(context, options);
  assistant->init();

  Chat::AssistantProxy client(context);
  client.init();
  stopButton(client);
  breakOut(client);
  runToCompletion(client);
  return 0;
}
