// The production transport, over rabbitmq-c.
//
// rabbitmq-c is not thread-safe: one connection state must be driven by one
// thread. Each connection therefore owns an I/O thread that does everything
// the broker sees: it reads frames, assembles deliveries and returns, matches
// confirms to publishes, and runs commands other threads submit through a
// queue and a wake pipe. Commands that wait for a broker reply block their
// submitting thread, never the I/O thread's frame handling for long: while
// rabbitmq-c waits for an RPC reply it queues every other frame, and the loop
// processes them as soon as the command returns.
#include <rabbitmq-c/amqp.h>
#include <fcntl.h>
#include <poll.h>
#include <rabbitmq-c/ssl_socket.h>
#include <rabbitmq-c/tcp_socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "amqp/url.h"
#include "protobus/logger.h"
#include "protobus/transport.h"

#ifndef PROTOBUS_VERSION
#define PROTOBUS_VERSION "dev"
#endif

namespace protobus::amqp {

namespace {

std::string bytesToString(amqp_bytes_t b) {
  if (b.len == 0 || b.bytes == nullptr) return std::string();
  return std::string(static_cast<const char*>(b.bytes), b.len);
}

amqp_bytes_t stringBytes(const std::string& s) {
  amqp_bytes_t b;
  b.len = s.size();
  b.bytes = const_cast<char*>(s.data());
  return b;
}

// ---- field tables ------------------------------------------------------------

FieldValue fromAmqpValue(const amqp_field_value_t& v);

FieldTable fromAmqpTable(const amqp_table_t& t) {
  FieldTable out;
  for (int i = 0; i < t.num_entries; ++i) {
    out[bytesToString(t.entries[i].key)] = fromAmqpValue(t.entries[i].value);
  }
  return out;
}

FieldValue fromAmqpValue(const amqp_field_value_t& v) {
  FieldValue f;
  switch (v.kind) {
    case AMQP_FIELD_KIND_BOOLEAN:
      f.kind = FieldValue::Kind::Bool;
      f.boolean = v.value.boolean != 0;
      break;
    case AMQP_FIELD_KIND_I8:
      f.kind = FieldValue::Kind::I8;
      f.integer = v.value.i8;
      break;
    case AMQP_FIELD_KIND_U8:
      f.kind = FieldValue::Kind::U8;
      f.unsigned_ = v.value.u8;
      break;
    case AMQP_FIELD_KIND_I16:
      f.kind = FieldValue::Kind::I16;
      f.integer = v.value.i16;
      break;
    case AMQP_FIELD_KIND_U16:
      f.kind = FieldValue::Kind::U16;
      f.unsigned_ = v.value.u16;
      break;
    case AMQP_FIELD_KIND_I32:
      f.kind = FieldValue::Kind::I32;
      f.integer = v.value.i32;
      break;
    case AMQP_FIELD_KIND_U32:
      f.kind = FieldValue::Kind::U32;
      f.unsigned_ = v.value.u32;
      break;
    case AMQP_FIELD_KIND_I64:
      f.kind = FieldValue::Kind::I64;
      f.integer = v.value.i64;
      break;
    case AMQP_FIELD_KIND_U64:
      f.kind = FieldValue::Kind::U64;
      f.unsigned_ = v.value.u64;
      break;
    case AMQP_FIELD_KIND_F32:
      f.kind = FieldValue::Kind::Float;
      f.real = v.value.f32;
      break;
    case AMQP_FIELD_KIND_F64:
      f.kind = FieldValue::Kind::Double;
      f.real = v.value.f64;
      break;
    case AMQP_FIELD_KIND_DECIMAL:
      f.kind = FieldValue::Kind::Decimal;
      f.decimalScale = v.value.decimal.decimals;
      f.decimalValue = v.value.decimal.value;
      break;
    case AMQP_FIELD_KIND_UTF8:
      f.kind = FieldValue::Kind::String;
      f.text = bytesToString(v.value.bytes);
      break;
    case AMQP_FIELD_KIND_BYTES:
      f.kind = FieldValue::Kind::Bytes;
      f.text = bytesToString(v.value.bytes);
      break;
    case AMQP_FIELD_KIND_TIMESTAMP:
      f.kind = FieldValue::Kind::Timestamp;
      f.integer = static_cast<int64_t>(v.value.u64);
      break;
    case AMQP_FIELD_KIND_TABLE:
      f.kind = FieldValue::Kind::Table;
      f.table = std::make_shared<FieldTable>(fromAmqpTable(v.value.table));
      break;
    case AMQP_FIELD_KIND_ARRAY: {
      f.kind = FieldValue::Kind::Array;
      FieldArray arr;
      for (int i = 0; i < v.value.array.num_entries; ++i) arr.push_back(fromAmqpValue(v.value.array.entries[i]));
      f.array = std::make_shared<FieldArray>(std::move(arr));
      break;
    }
    default:
      f.kind = FieldValue::Kind::Void;
      break;
  }
  return f;
}

// Owns the memory behind an amqp_table_t for as long as the call using it.
class TableBuilder {
 public:
  amqp_table_t build(const FieldTable& t) {
    amqp_table_t out{0, nullptr};
    if (t.empty()) return out;
    auto& entries = entryStore_.emplace_back(t.size());
    size_t i = 0;
    for (const auto& [key, value] : t) {
      entries[i].key = stringBytes(keep(key));
      entries[i].value = buildValue(value);
      ++i;
    }
    out.num_entries = static_cast<int>(entries.size());
    out.entries = entries.data();
    return out;
  }

 private:
  const std::string& keep(const std::string& s) { return strings_.emplace_back(s); }

  amqp_field_value_t buildValue(const FieldValue& v) {
    amqp_field_value_t out;
    std::memset(&out, 0, sizeof out);
    switch (v.kind) {
      case FieldValue::Kind::Void:
        out.kind = AMQP_FIELD_KIND_VOID;
        break;
      case FieldValue::Kind::Bool:
        out.kind = AMQP_FIELD_KIND_BOOLEAN;
        out.value.boolean = v.boolean ? 1 : 0;
        break;
      case FieldValue::Kind::I8:
        out.kind = AMQP_FIELD_KIND_I8;
        out.value.i8 = static_cast<int8_t>(v.integer);
        break;
      case FieldValue::Kind::U8:
        out.kind = AMQP_FIELD_KIND_U8;
        out.value.u8 = static_cast<uint8_t>(v.unsigned_);
        break;
      case FieldValue::Kind::I16:
        out.kind = AMQP_FIELD_KIND_I16;
        out.value.i16 = static_cast<int16_t>(v.integer);
        break;
      case FieldValue::Kind::U16:
        out.kind = AMQP_FIELD_KIND_U16;
        out.value.u16 = static_cast<uint16_t>(v.unsigned_);
        break;
      case FieldValue::Kind::I32:
        out.kind = AMQP_FIELD_KIND_I32;
        out.value.i32 = static_cast<int32_t>(v.integer);
        break;
      case FieldValue::Kind::U32:
        out.kind = AMQP_FIELD_KIND_U32;
        out.value.u32 = static_cast<uint32_t>(v.unsigned_);
        break;
      case FieldValue::Kind::I64:
        out.kind = AMQP_FIELD_KIND_I64;
        out.value.i64 = v.integer;
        break;
      case FieldValue::Kind::U64:
        out.kind = AMQP_FIELD_KIND_U64;
        out.value.u64 = v.unsigned_;
        break;
      case FieldValue::Kind::Float:
        out.kind = AMQP_FIELD_KIND_F32;
        out.value.f32 = static_cast<float>(v.real);
        break;
      case FieldValue::Kind::Double:
        out.kind = AMQP_FIELD_KIND_F64;
        out.value.f64 = v.real;
        break;
      case FieldValue::Kind::Decimal:
        out.kind = AMQP_FIELD_KIND_DECIMAL;
        out.value.decimal.decimals = v.decimalScale;
        out.value.decimal.value = v.decimalValue;
        break;
      case FieldValue::Kind::String:
        out.kind = AMQP_FIELD_KIND_UTF8;
        out.value.bytes = stringBytes(keep(v.text));
        break;
      case FieldValue::Kind::Bytes:
        out.kind = AMQP_FIELD_KIND_BYTES;
        out.value.bytes = stringBytes(keep(v.text));
        break;
      case FieldValue::Kind::Timestamp:
        out.kind = AMQP_FIELD_KIND_TIMESTAMP;
        out.value.u64 = static_cast<uint64_t>(v.integer);
        break;
      case FieldValue::Kind::Table:
        out.kind = AMQP_FIELD_KIND_TABLE;
        out.value.table = v.table ? build(*v.table) : amqp_table_t{0, nullptr};
        break;
      case FieldValue::Kind::Array: {
        out.kind = AMQP_FIELD_KIND_ARRAY;
        out.value.array.num_entries = 0;
        out.value.array.entries = nullptr;
        if (v.array && !v.array->empty()) {
          auto& values = valueStore_.emplace_back(v.array->size());
          for (size_t i = 0; i < v.array->size(); ++i) values[i] = buildValue((*v.array)[i]);
          out.value.array.num_entries = static_cast<int>(values.size());
          out.value.array.entries = values.data();
        }
        break;
      }
    }
    return out;
  }

  std::deque<std::string> strings_;
  std::deque<std::vector<amqp_table_entry_t>> entryStore_;
  std::deque<std::vector<amqp_field_value_t>> valueStore_;
};

Properties fromAmqpProperties(const amqp_basic_properties_t& p) {
  Properties out;
  if (p._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) out.contentType = bytesToString(p.content_type);
  if (p._flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) out.contentEncoding = bytesToString(p.content_encoding);
  if (p._flags & AMQP_BASIC_HEADERS_FLAG) out.headers = fromAmqpTable(p.headers);
  if (p._flags & AMQP_BASIC_DELIVERY_MODE_FLAG) out.deliveryMode = p.delivery_mode;
  if (p._flags & AMQP_BASIC_PRIORITY_FLAG) out.priority = p.priority;
  if (p._flags & AMQP_BASIC_CORRELATION_ID_FLAG) out.correlationId = bytesToString(p.correlation_id);
  if (p._flags & AMQP_BASIC_REPLY_TO_FLAG) out.replyTo = bytesToString(p.reply_to);
  if (p._flags & AMQP_BASIC_EXPIRATION_FLAG) out.expiration = bytesToString(p.expiration);
  if (p._flags & AMQP_BASIC_MESSAGE_ID_FLAG) out.messageId = bytesToString(p.message_id);
  if (p._flags & AMQP_BASIC_TIMESTAMP_FLAG) out.timestamp = p.timestamp;
  if (p._flags & AMQP_BASIC_TYPE_FLAG) out.type = bytesToString(p.type);
  if (p._flags & AMQP_BASIC_USER_ID_FLAG) out.userId = bytesToString(p.user_id);
  if (p._flags & AMQP_BASIC_APP_ID_FLAG) out.appId = bytesToString(p.app_id);
  return out;
}

amqp_basic_properties_t toAmqpProperties(const Properties& p, TableBuilder& tables) {
  amqp_basic_properties_t out;
  std::memset(&out, 0, sizeof out);
  auto setBytes = [&](const std::optional<std::string>& v, amqp_bytes_t& field, amqp_flags_t flag) {
    if (!v) return;
    field = stringBytes(*v);
    out._flags |= flag;
  };
  setBytes(p.contentType, out.content_type, AMQP_BASIC_CONTENT_TYPE_FLAG);
  setBytes(p.contentEncoding, out.content_encoding, AMQP_BASIC_CONTENT_ENCODING_FLAG);
  if (p.headers) {
    out.headers = tables.build(*p.headers);
    out._flags |= AMQP_BASIC_HEADERS_FLAG;
  }
  if (p.deliveryMode) {
    out.delivery_mode = *p.deliveryMode;
    out._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
  }
  if (p.priority) {
    out.priority = *p.priority;
    out._flags |= AMQP_BASIC_PRIORITY_FLAG;
  }
  setBytes(p.correlationId, out.correlation_id, AMQP_BASIC_CORRELATION_ID_FLAG);
  setBytes(p.replyTo, out.reply_to, AMQP_BASIC_REPLY_TO_FLAG);
  setBytes(p.expiration, out.expiration, AMQP_BASIC_EXPIRATION_FLAG);
  setBytes(p.messageId, out.message_id, AMQP_BASIC_MESSAGE_ID_FLAG);
  if (p.timestamp) {
    out.timestamp = *p.timestamp;
    out._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
  }
  setBytes(p.type, out.type, AMQP_BASIC_TYPE_FLAG);
  setBytes(p.userId, out.user_id, AMQP_BASIC_USER_ID_FLAG);
  setBytes(p.appId, out.app_id, AMQP_BASIC_APP_ID_FLAG);
  return out;
}

// ---- connection ----------------------------------------------------------------

struct PendingConfirm {
  ConfirmCallback callback;
  std::string messageId;
};

struct Consumer {
  DeliveryCallback onDelivery;
  std::function<void()> onCancel;
  bool noAck = false;
};

// Everything about a channel the I/O thread owns.
struct ChannelState {
  amqp_channel_t id = 0;
  bool closed = false;
  std::string closeReason;
  uint64_t nextSeq = 1;
  std::map<uint64_t, PendingConfirm> pending;
  // messageIds with a publish awaiting its confirm, and those of them the
  // broker returned. RabbitMQ sends basic.return before the confirm for the
  // same message, so the confirm can consult the set.
  std::unordered_multiset<std::string> awaiting;
  std::unordered_set<std::string> returned;
  std::map<std::string, Consumer> consumers;

  // Content assembly for a delivery or return in progress.
  enum class Assembly { None, Deliver, Return } assembly = Assembly::None;
  Delivery partial;
  bool haveHeader = false;
  uint64_t remaining = 0;

  std::mutex callbackMutex;
  std::vector<std::function<void(const std::string&)>> closeCallbacks;
};

class RabbitConnection;

class RabbitChannel : public Channel {
 public:
  RabbitChannel(std::shared_ptr<RabbitConnection> conn, std::shared_ptr<ChannelState> state)
      : conn_(std::move(conn)), state_(std::move(state)) {}
  ~RabbitChannel() override;

  void declareExchange(const std::string& name, const std::string& type, bool durable, bool autoDelete,
                       bool internal, const FieldTable& arguments) override;
  std::string declareQueue(const std::string& name, bool durable, bool exclusive, bool autoDelete,
                           const FieldTable& arguments) override;
  void bindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                 const FieldTable& arguments) override;
  void unbindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                   const FieldTable& arguments) override;
  void deleteQueue(const std::string& name) override;
  void purgeQueue(const std::string& name) override;
  void prefetch(uint16_t count) override;
  std::string consume(const std::string& queue, const std::string& consumerTag, bool noAck, bool exclusive,
                      DeliveryCallback onDelivery, std::function<void()> onCancel) override;
  void cancel(const std::string& consumerTag) override;
  void ack(uint64_t deliveryTag) override;
  void reject(uint64_t deliveryTag, bool requeue) override;
  void publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
               const Properties& properties, bool mandatory, ConfirmCallback onConfirm) override;
  void close() override;
  bool isOpen() const override;
  void onClose(std::function<void(const std::string& reason)> fn) override;

 private:
  std::shared_ptr<RabbitConnection> conn_;
  std::shared_ptr<ChannelState> state_;
};

class RabbitConnection : public Connection, public std::enable_shared_from_this<RabbitConnection> {
 public:
  RabbitConnection(amqp_connection_state_t conn, int channelMax) : conn_(conn), channelMax_(channelMax) {
    if (::pipe(wake_) != 0) throw AmqpError("cannot create the transport's wake pipe", 0, false);
    for (int fd : wake_) {
      ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
      ::fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
  }

  ~RabbitConnection() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopRequested_ = true;
    }
    wake();
    if (io_.joinable()) {
      if (io_.get_id() == std::this_thread::get_id()) {
        io_.detach();
      } else {
        io_.join();
      }
    }
    if (conn_ != nullptr) {
      amqp_destroy_connection(conn_);
      conn_ = nullptr;
    }
    ::close(wake_[0]);
    ::close(wake_[1]);
  }

  void start() {
    open_ = true;
    io_ = std::thread([self = shared_from_this()] { self->loop(); });
  }

  std::shared_ptr<Channel> openChannel() override {
    auto state = run([this]() -> std::shared_ptr<ChannelState> {
      const amqp_channel_t id = allocateChannel();
      amqp_channel_open(conn_, id);
      check(amqp_get_rpc_reply(conn_), id, "channel.open");
      auto st = std::make_shared<ChannelState>();
      st->id = id;
      channels_[id] = st;
      amqp_confirm_select(conn_, id);
      check(amqp_get_rpc_reply(conn_), id, "confirm.select");
      return st;
    });
    return std::make_shared<RabbitChannel>(shared_from_this(), state);
  }

  void close() override {
    if (onIoThread()) {
      closeRequested_ = true;
      return;
    }
    try {
      run([this] {
        closeRequested_ = true;
        return 0;
      });
    } catch (const AmqpError&) {
      // Already closed.
    }
    if (io_.joinable() && io_.get_id() != std::this_thread::get_id()) io_.join();
  }

  bool isOpen() const override { return open_.load(); }

  void onClose(std::function<void(std::optional<std::string>)> fn) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (finished_) {
      auto reason = finishReason_;
      lock.unlock();
      fn(reason);
      return;
    }
    closeCallbacks_.push_back(std::move(fn));
  }

  // Run `fn` on the I/O thread and return its result. Inline when already
  // there.
  template <typename F>
  auto run(F fn) -> decltype(fn()) {
    using R = decltype(fn());
    if (onIoThread()) {
      ensureOpen();
      return fn();
    }
    auto task = std::make_shared<std::packaged_task<R()>>([this, fn = std::move(fn)]() mutable {
      ensureOpen();
      return fn();
    });
    auto future = task->get_future();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (finished_ || stopRequested_) throw AmqpError("the connection is closed", 0, true);
      commands_.push_back([task] { (*task)(); });
    }
    wake();
    return future.get();
  }

  // Run `fn` on the I/O thread without waiting for it.
  void post(std::function<void()> fn) {
    if (onIoThread()) {
      fn();
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (finished_ || stopRequested_) return;
      commands_.push_back(std::move(fn));
    }
    wake();
  }

  bool onIoThread() const { return std::this_thread::get_id() == ioThreadId_.load(); }

  amqp_connection_state_t state() const { return conn_; }

  // Throw for a failed RPC. A channel exception closes that channel; a
  // connection exception or a library failure ends the connection.
  void check(amqp_rpc_reply_t reply, amqp_channel_t channel, const char* what) {
    switch (reply.reply_type) {
      case AMQP_RESPONSE_NORMAL:
        return;
      case AMQP_RESPONSE_NONE:
        throw AmqpError(std::string(what) + ": no reply from the broker", 0, true);
      case AMQP_RESPONSE_LIBRARY_EXCEPTION: {
        const std::string reason = std::string(what) + ": " + amqp_error_string2(reply.library_error);
        lose(reason);
        throw AmqpError(reason, 0, true);
      }
      case AMQP_RESPONSE_SERVER_EXCEPTION:
        if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
          auto* m = static_cast<amqp_channel_close_t*>(reply.reply.decoded);
          const int code = m->reply_code;
          const std::string text = bytesToString(m->reply_text);
          amqp_channel_close_ok_t ok;
          amqp_send_method(conn_, channel, AMQP_CHANNEL_CLOSE_OK_METHOD, &ok);
          closeChannel(channel, std::to_string(code) + " " + text);
          throw AmqpError(std::string(what) + ": " + text, code, true);
        }
        if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
          auto* m = static_cast<amqp_connection_close_t*>(reply.reply.decoded);
          const int code = m->reply_code;
          const std::string text = bytesToString(m->reply_text);
          amqp_connection_close_ok_t ok;
          amqp_send_method(conn_, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &ok);
          lose("connection closed by the broker: " + std::to_string(code) + " " + text);
          throw AmqpError(std::string(what) + ": " + text, code, true);
        }
        throw AmqpError(std::string(what) + ": unexpected server reply", 0, true);
    }
  }

  std::shared_ptr<ChannelState> channel(amqp_channel_t id) {
    auto it = channels_.find(id);
    return it == channels_.end() ? nullptr : it->second;
  }

  // Close a channel's state: fail its unconfirmed publishes as UNKNOWN and
  // tell its close listeners. I/O thread only.
  void closeChannel(amqp_channel_t id, const std::string& reason) {
    auto st = channel(id);
    if (!st || st->closed) return;
    st->closed = true;
    st->closeReason = reason;
    channels_.erase(id);
    failChannel(*st, reason);
  }

  void failChannel(ChannelState& st, const std::string& reason) {
    auto pending = std::move(st.pending);
    st.pending.clear();
    st.awaiting.clear();
    st.returned.clear();
    for (auto& [_, p] : pending) {
      try {
        p.callback(ConfirmOutcome::Closed, reason);
      } catch (...) {
      }
    }
    std::vector<std::function<void(const std::string&)>> callbacks;
    {
      std::lock_guard<std::mutex> lock(st.callbackMutex);
      callbacks.swap(st.closeCallbacks);
    }
    for (auto& cb : callbacks) {
      try {
        cb(reason);
      } catch (...) {
      }
    }
  }

 private:
  void ensureOpen() {
    if (lost_ || closeRequested_) throw AmqpError("the connection is closed", 0, true);
  }

  amqp_channel_t allocateChannel() {
    const int max = channelMax_ > 0 ? channelMax_ : 2047;
    for (int i = 0; i < max; ++i) {
      nextChannel_ = static_cast<amqp_channel_t>(nextChannel_ % max + 1);
      if (channels_.count(nextChannel_) == 0) return nextChannel_;
    }
    throw AmqpError("no free channel numbers on this connection", 0, false);
  }

  void wake() {
    const char b = 1;
    [[maybe_unused]] auto n = ::write(wake_[1], &b, 1);
  }

  void drainWake() {
    char buf[256];
    while (::read(wake_[0], buf, sizeof buf) > 0) {
    }
  }

  // The connection is gone: say so once, with the reason.
  void lose(const std::string& reason) {
    if (lost_) return;
    lost_ = true;
    lossReason_ = reason;
  }

  void loop() {
    ioThreadId_ = std::this_thread::get_id();
    const int sock = amqp_get_sockfd(conn_);
    while (!lost_) {
      std::deque<std::function<void()>> commands;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        commands.swap(commands_);
        if (stopRequested_) closeRequested_ = true;
      }
      for (auto& c : commands) {
        c();
        if (lost_) break;
      }
      if (lost_) break;
      if (closeRequested_) {
        gracefulClose();
        break;
      }

      if (!amqp_frames_enqueued(conn_) && !amqp_data_in_buffer(conn_)) {
        pollfd fds[2] = {{sock, POLLIN, 0}, {wake_[0], POLLIN, 0}};
        ::poll(fds, 2, 200);
        if (fds[1].revents & POLLIN) drainWake();
      }
      readFrames();
      if (!amqp_frames_enqueued(conn_)) amqp_maybe_release_buffers(conn_);
    }
    finish(lost_ ? std::optional<std::string>(lossReason_) : std::nullopt);
  }

  void gracefulClose() {
    if (!lost_) {
      amqp_rpc_reply_t r = amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
      (void)r;
    }
  }

  void readFrames() {
    for (int budget = 0; budget < 1024 && !lost_; ++budget) {
      amqp_frame_t frame;
      timeval zero{0, 0};
      const int status = amqp_simple_wait_frame_noblock(conn_, &frame, &zero);
      if (status == AMQP_STATUS_TIMEOUT) return;
      if (status != AMQP_STATUS_OK) {
        lose(std::string("connection lost: ") + amqp_error_string2(status));
        return;
      }
      handleFrame(frame);
    }
  }

  void handleFrame(const amqp_frame_t& frame) {
    auto st = channel(frame.channel);
    switch (frame.frame_type) {
      case AMQP_FRAME_METHOD:
        handleMethod(frame.channel, frame.payload.method, st);
        return;
      case AMQP_FRAME_HEADER:
        if (!st || st->assembly == ChannelState::Assembly::None) return;
        st->partial.properties =
            fromAmqpProperties(*static_cast<amqp_basic_properties_t*>(frame.payload.properties.decoded));
        st->haveHeader = true;
        st->remaining = frame.payload.properties.body_size;
        st->partial.body.reserve(static_cast<size_t>(st->remaining));
        if (st->remaining == 0) completeContent(*st);
        return;
      case AMQP_FRAME_BODY:
        if (!st || st->assembly == ChannelState::Assembly::None || !st->haveHeader) return;
        st->partial.body.append(static_cast<const char*>(frame.payload.body_fragment.bytes),
                                frame.payload.body_fragment.len);
        st->remaining -= std::min<uint64_t>(st->remaining, frame.payload.body_fragment.len);
        if (st->remaining == 0) completeContent(*st);
        return;
      default:
        return;
    }
  }

  void handleMethod(amqp_channel_t id, const amqp_method_t& method, const std::shared_ptr<ChannelState>& st) {
    switch (method.id) {
      case AMQP_BASIC_DELIVER_METHOD: {
        if (!st) return;
        auto* m = static_cast<amqp_basic_deliver_t*>(method.decoded);
        st->assembly = ChannelState::Assembly::Deliver;
        st->haveHeader = false;
        st->partial = Delivery{};
        st->partial.consumerTag = bytesToString(m->consumer_tag);
        st->partial.deliveryTag = m->delivery_tag;
        st->partial.redelivered = m->redelivered != 0;
        st->partial.exchange = bytesToString(m->exchange);
        st->partial.routingKey = bytesToString(m->routing_key);
        return;
      }
      case AMQP_BASIC_RETURN_METHOD: {
        if (!st) return;
        auto* m = static_cast<amqp_basic_return_t*>(method.decoded);
        st->assembly = ChannelState::Assembly::Return;
        st->haveHeader = false;
        st->partial = Delivery{};
        st->partial.exchange = bytesToString(m->exchange);
        st->partial.routingKey = bytesToString(m->routing_key);
        return;
      }
      case AMQP_BASIC_ACK_METHOD: {
        if (!st) return;
        auto* m = static_cast<amqp_basic_ack_t*>(method.decoded);
        settle(*st, m->delivery_tag, m->multiple != 0, false);
        return;
      }
      case AMQP_BASIC_NACK_METHOD: {
        if (!st) return;
        auto* m = static_cast<amqp_basic_nack_t*>(method.decoded);
        settle(*st, m->delivery_tag, m->multiple != 0, true);
        return;
      }
      case AMQP_BASIC_CANCEL_METHOD: {
        if (!st) return;
        auto* m = static_cast<amqp_basic_cancel_t*>(method.decoded);
        const std::string tag = bytesToString(m->consumer_tag);
        if (!m->nowait) {
          amqp_basic_cancel_ok_t ok;
          ok.consumer_tag = m->consumer_tag;
          amqp_send_method(conn_, id, AMQP_BASIC_CANCEL_OK_METHOD, &ok);
        }
        auto it = st->consumers.find(tag);
        if (it != st->consumers.end()) {
          auto onCancel = std::move(it->second.onCancel);
          st->consumers.erase(it);
          if (onCancel) {
            try {
              onCancel();
            } catch (...) {
            }
          }
        }
        return;
      }
      case AMQP_CHANNEL_CLOSE_METHOD: {
        auto* m = static_cast<amqp_channel_close_t*>(method.decoded);
        const std::string reason = std::to_string(m->reply_code) + " " + bytesToString(m->reply_text);
        amqp_channel_close_ok_t ok;
        amqp_send_method(conn_, id, AMQP_CHANNEL_CLOSE_OK_METHOD, &ok);
        Logger::debug("protobus: channel " + std::to_string(id) + " closed by the broker: " + reason);
        closeChannel(id, reason);
        return;
      }
      case AMQP_CONNECTION_CLOSE_METHOD: {
        auto* m = static_cast<amqp_connection_close_t*>(method.decoded);
        const std::string reason = std::to_string(m->reply_code) + " " + bytesToString(m->reply_text);
        amqp_connection_close_ok_t ok;
        amqp_send_method(conn_, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &ok);
        lose("connection closed by the broker: " + reason);
        return;
      }
      case AMQP_CONNECTION_BLOCKED_METHOD:
        Logger::warn("protobus: the broker blocked this connection (resource alarm); publishes will stall");
        return;
      case AMQP_CONNECTION_UNBLOCKED_METHOD:
        Logger::info("protobus: the broker unblocked this connection");
        return;
      default:
        return;
    }
  }

  void completeContent(ChannelState& st) {
    const auto kind = st.assembly;
    st.assembly = ChannelState::Assembly::None;
    st.haveHeader = false;
    Delivery d = std::move(st.partial);
    st.partial = Delivery{};
    if (kind == ChannelState::Assembly::Return) {
      // Only while a publish is actually waiting on that id: a return
      // arriving after its publish gave up would otherwise be read as the
      // verdict on the next publish reusing the id.
      if (d.properties.messageId && st.awaiting.count(*d.properties.messageId) > 0) {
        st.returned.insert(*d.properties.messageId);
      }
      return;
    }
    auto it = st.consumers.find(d.consumerTag);
    if (it == st.consumers.end()) {
      // The consumer was cancelled with this delivery in flight. Hand it
      // back to the broker rather than leaving it unacknowledged.
      amqp_basic_reject(conn_, st.id, d.deliveryTag, 1);
      return;
    }
    try {
      it->second.onDelivery(std::move(d));
    } catch (const std::exception& e) {
      Logger::error(std::string("protobus: a delivery callback threw: ") + e.what());
    }
  }

  void settle(ChannelState& st, uint64_t tag, bool multiple, bool nacked) {
    std::vector<PendingConfirm> done;
    auto end = multiple ? st.pending.upper_bound(tag) : st.pending.find(tag);
    if (multiple) {
      for (auto it = st.pending.begin(); it != end;) {
        done.push_back(std::move(it->second));
        it = st.pending.erase(it);
      }
    } else if (end != st.pending.end()) {
      done.push_back(std::move(end->second));
      st.pending.erase(end);
    }
    for (auto& p : done) {
      ConfirmOutcome outcome = nacked ? ConfirmOutcome::Nack : ConfirmOutcome::Ack;
      if (!p.messageId.empty()) {
        if (!nacked && st.returned.count(p.messageId) > 0) outcome = ConfirmOutcome::Returned;
        auto a = st.awaiting.find(p.messageId);
        if (a != st.awaiting.end()) st.awaiting.erase(a);
        if (st.awaiting.count(p.messageId) == 0) st.returned.erase(p.messageId);
      }
      try {
        p.callback(outcome, nacked ? "message nacked by the broker" : "");
      } catch (...) {
      }
    }
  }

  void finish(std::optional<std::string> reason) {
    open_ = false;
    const std::string channelReason = reason ? *reason : "the connection was closed";
    auto channels = std::move(channels_);
    channels_.clear();
    for (auto& [_, st] : channels) {
      st->closed = true;
      st->closeReason = channelReason;
      failChannel(*st, channelReason);
    }
    std::deque<std::function<void()>> leftover;
    std::vector<std::function<void(std::optional<std::string>)>> callbacks;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
      finishReason_ = reason;
      leftover.swap(commands_);
      callbacks.swap(closeCallbacks_);
    }
    // Commands still queued run against a closed connection, so their
    // waiters are released with an error rather than left hanging.
    for (auto& c : leftover) c();
    for (auto& cb : callbacks) {
      try {
        cb(reason);
      } catch (...) {
      }
    }
  }

  amqp_connection_state_t conn_;
  int channelMax_;
  int wake_[2] = {-1, -1};
  std::thread io_;
  std::atomic<std::thread::id> ioThreadId_{};
  std::atomic<bool> open_{false};

  std::mutex mutex_;
  std::deque<std::function<void()>> commands_;
  std::vector<std::function<void(std::optional<std::string>)>> closeCallbacks_;
  bool finished_ = false;
  bool stopRequested_ = false;
  std::optional<std::string> finishReason_;

  // I/O thread only.
  bool lost_ = false;
  bool closeRequested_ = false;
  std::string lossReason_;
  std::map<amqp_channel_t, std::shared_ptr<ChannelState>> channels_;
  amqp_channel_t nextChannel_ = 0;
};

// ---- channel -------------------------------------------------------------------

RabbitChannel::~RabbitChannel() = default;

void RabbitChannel::declareExchange(const std::string& name, const std::string& type, bool durable, bool autoDelete,
                                    bool internal, const FieldTable& arguments) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    TableBuilder tables;
    amqp_exchange_declare(conn_->state(), state_->id, stringBytes(name), stringBytes(type), 0, durable, autoDelete,
                          internal, tables.build(arguments));
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "exchange.declare");
    return 0;
  });
}

std::string RabbitChannel::declareQueue(const std::string& name, bool durable, bool exclusive, bool autoDelete,
                                        const FieldTable& arguments) {
  return conn_->run([&]() -> std::string {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    TableBuilder tables;
    auto* ok = amqp_queue_declare(conn_->state(), state_->id, stringBytes(name), 0, durable, exclusive, autoDelete,
                                  tables.build(arguments));
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "queue.declare");
    return ok ? bytesToString(ok->queue) : name;
  });
}

void RabbitChannel::bindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                              const FieldTable& arguments) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    TableBuilder tables;
    amqp_queue_bind(conn_->state(), state_->id, stringBytes(queue), stringBytes(exchange), stringBytes(routingKey),
                    tables.build(arguments));
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "queue.bind");
    return 0;
  });
}

void RabbitChannel::unbindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                                const FieldTable& arguments) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    TableBuilder tables;
    amqp_queue_unbind(conn_->state(), state_->id, stringBytes(queue), stringBytes(exchange), stringBytes(routingKey),
                      tables.build(arguments));
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "queue.unbind");
    return 0;
  });
}

void RabbitChannel::deleteQueue(const std::string& name) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    amqp_queue_delete(conn_->state(), state_->id, stringBytes(name), 0, 0);
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "queue.delete");
    return 0;
  });
}

void RabbitChannel::purgeQueue(const std::string& name) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    amqp_queue_purge(conn_->state(), state_->id, stringBytes(name));
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "queue.purge");
    return 0;
  });
}

void RabbitChannel::prefetch(uint16_t count) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    amqp_basic_qos(conn_->state(), state_->id, 0, count, 0);
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "basic.qos");
    return 0;
  });
}

std::string RabbitChannel::consume(const std::string& queue, const std::string& consumerTag, bool noAck,
                                   bool exclusive, DeliveryCallback onDelivery, std::function<void()> onCancel) {
  return conn_->run([&]() -> std::string {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    // Registered before the request: rabbitmq-c queues any delivery that
    // arrives while it waits for consume-ok, and the loop hands those over as
    // soon as this returns.
    state_->consumers[consumerTag] = Consumer{std::move(onDelivery), std::move(onCancel), noAck};
    amqp_basic_consume(conn_->state(), state_->id, stringBytes(queue), stringBytes(consumerTag), 0, noAck,
                       exclusive, amqp_empty_table);
    try {
      conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "basic.consume");
    } catch (...) {
      state_->consumers.erase(consumerTag);
      throw;
    }
    return consumerTag;
  });
}

void RabbitChannel::cancel(const std::string& consumerTag) {
  conn_->run([&] {
    if (state_->closed) throw AmqpError("channel is closed: " + state_->closeReason, 0, true);
    amqp_basic_cancel(conn_->state(), state_->id, stringBytes(consumerTag));
    conn_->check(amqp_get_rpc_reply(conn_->state()), state_->id, "basic.cancel");
    state_->consumers.erase(consumerTag);
    return 0;
  });
}

void RabbitChannel::ack(uint64_t deliveryTag) {
  auto st = state_;
  auto conn = conn_;
  conn_->post([conn, st, deliveryTag] {
    if (st->closed) return;
    amqp_basic_ack(conn->state(), st->id, deliveryTag, 0);
  });
}

void RabbitChannel::reject(uint64_t deliveryTag, bool requeue) {
  auto st = state_;
  auto conn = conn_;
  conn_->post([conn, st, deliveryTag, requeue] {
    if (st->closed) return;
    amqp_basic_reject(conn->state(), st->id, deliveryTag, requeue ? 1 : 0);
  });
}

void RabbitChannel::publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
                            const Properties& properties, bool mandatory, ConfirmCallback onConfirm) {
  auto st = state_;
  auto conn = conn_;
  auto task = [conn, st, exchange, routingKey, body, properties, mandatory,
               onConfirm = std::move(onConfirm)]() mutable {
    if (st->closed) throw AmqpError("channel is closed: " + st->closeReason, 0, true);
    TableBuilder tables;
    amqp_basic_properties_t props = toAmqpProperties(properties, tables);
    const int status = amqp_basic_publish(conn->state(), st->id, stringBytes(exchange), stringBytes(routingKey),
                                          mandatory ? 1 : 0, 0, &props, stringBytes(body));
    if (status != AMQP_STATUS_OK) {
      throw AmqpError(std::string("publish failed: ") + amqp_error_string2(status), 0, true);
    }
    const uint64_t seq = st->nextSeq++;
    const std::string id = properties.messageId.value_or("");
    if (!id.empty()) st->awaiting.insert(id);
    st->pending.emplace(seq, PendingConfirm{std::move(onConfirm), id});
    return 0;
  };
  conn_->run(std::move(task));
}

void RabbitChannel::close() {
  try {
    conn_->run([this] {
      if (state_->closed) return 0;
      amqp_rpc_reply_t r = amqp_channel_close(conn_->state(), state_->id, AMQP_REPLY_SUCCESS);
      (void)r;
      conn_->closeChannel(state_->id, "channel closed");
      return 0;
    });
  } catch (const AmqpError&) {
    // The connection is already gone, and the channel with it.
  }
}

bool RabbitChannel::isOpen() const { return !state_->closed && conn_->isOpen(); }

void RabbitChannel::onClose(std::function<void(const std::string& reason)> fn) {
  std::unique_lock<std::mutex> lock(state_->callbackMutex);
  if (state_->closed) {
    const std::string reason = state_->closeReason;
    lock.unlock();
    fn(reason);
    return;
  }
  state_->closeCallbacks.push_back(std::move(fn));
}

// ---- transport -----------------------------------------------------------------

class RabbitTransport : public Transport {
 public:
  std::shared_ptr<Connection> connect(const std::string& url, int heartbeatSeconds) override {
    const BrokerUrl u = parseBrokerUrl(url);
    int heartbeat = heartbeatSeconds;
    if (auto it = u.query.find("heartbeat"); it != u.query.end()) heartbeat = intParam(it->second, "heartbeat");
    int timeoutMs = 30000;
    if (auto it = u.query.find("connection_timeout"); it != u.query.end()) {
      timeoutMs = intParam(it->second, "connection_timeout");
    }
    int channelMax = 2047;
    if (auto it = u.query.find("channel_max"); it != u.query.end()) channelMax = intParam(it->second, "channel_max");
    int frameMax = 131072;
    if (auto it = u.query.find("frame_max"); it != u.query.end()) frameMax = intParam(it->second, "frame_max");

    amqp_connection_state_t conn = amqp_new_connection();
    if (conn == nullptr) throw AmqpError("cannot allocate an AMQP connection", 0, false);
    try {
      amqp_socket_t* socket = nullptr;
      if (u.tls) {
        socket = amqp_ssl_socket_new(conn);
        if (socket == nullptr) throw AmqpError("cannot create a TLS socket", 0, false);
        if (auto it = u.query.find("cacertfile"); it != u.query.end()) {
          if (amqp_ssl_socket_set_cacert(socket, it->second.c_str()) != AMQP_STATUS_OK) {
            throw AmqpError("cannot load cacertfile " + it->second, 0, false);
          }
        } else {
          amqp_ssl_socket_enable_default_verify_paths(socket);
        }
        auto cert = u.query.find("certfile");
        auto key = u.query.find("keyfile");
        if (cert != u.query.end() && key != u.query.end()) {
          if (amqp_ssl_socket_set_key(socket, cert->second.c_str(), key->second.c_str()) != AMQP_STATUS_OK) {
            throw AmqpError("cannot load the client certificate or key", 0, false);
          }
        }
        if (auto it = u.query.find("verify"); it != u.query.end() && it->second == "verify_none") {
          amqp_ssl_socket_set_verify_peer(socket, 0);
          amqp_ssl_socket_set_verify_hostname(socket, 0);
        } else {
          amqp_ssl_socket_set_verify_peer(socket, 1);
          amqp_ssl_socket_set_verify_hostname(socket, 1);
        }
      } else {
        socket = amqp_tcp_socket_new(conn);
        if (socket == nullptr) throw AmqpError("cannot create a TCP socket", 0, false);
      }

      timeval tv{timeoutMs / 1000, static_cast<suseconds_t>((timeoutMs % 1000) * 1000)};
      const int status = amqp_socket_open_noblock(socket, u.host.c_str(), u.port, &tv);
      if (status != AMQP_STATUS_OK) {
        throw AmqpError("cannot connect to " + u.host + ":" + std::to_string(u.port) + ": " +
                            amqp_error_string2(status),
                        0, false);
      }
      // Heartbeats only start once the connection is open, so without a
      // bound a broker that accepts the socket and then stalls would hang
      // the handshake forever.
      amqp_set_handshake_timeout(conn, &tv);
      timeval rpc{60, 0};
      amqp_set_rpc_timeout(conn, &rpc);

      amqp_table_entry_t capabilities[] = {
          {amqp_cstring_bytes("publisher_confirms"), {AMQP_FIELD_KIND_BOOLEAN, {.boolean = 1}}},
          {amqp_cstring_bytes("consumer_cancel_notify"), {AMQP_FIELD_KIND_BOOLEAN, {.boolean = 1}}},
          {amqp_cstring_bytes("basic.nack"), {AMQP_FIELD_KIND_BOOLEAN, {.boolean = 1}}},
          {amqp_cstring_bytes("connection.blocked"), {AMQP_FIELD_KIND_BOOLEAN, {.boolean = 1}}},
          {amqp_cstring_bytes("authentication_failure_close"), {AMQP_FIELD_KIND_BOOLEAN, {.boolean = 1}}},
      };
      amqp_table_entry_t props[3];
      props[0].key = amqp_cstring_bytes("product");
      props[0].value.kind = AMQP_FIELD_KIND_UTF8;
      props[0].value.value.bytes = amqp_cstring_bytes("protobus-cpp");
      props[1].key = amqp_cstring_bytes("version");
      props[1].value.kind = AMQP_FIELD_KIND_UTF8;
      props[1].value.value.bytes = amqp_cstring_bytes(PROTOBUS_VERSION);
      props[2].key = amqp_cstring_bytes("capabilities");
      props[2].value.kind = AMQP_FIELD_KIND_TABLE;
      props[2].value.value.table.num_entries = static_cast<int>(sizeof capabilities / sizeof capabilities[0]);
      props[2].value.value.table.entries = capabilities;
      amqp_table_t clientProperties{3, props};

      amqp_rpc_reply_t login = amqp_login_with_properties(conn, u.vhost.c_str(), channelMax, frameMax, heartbeat,
                                                          &clientProperties, AMQP_SASL_METHOD_PLAIN,
                                                          u.user.c_str(), u.password.c_str());
      if (login.reply_type != AMQP_RESPONSE_NORMAL) {
        std::string reason;
        if (login.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
          reason = amqp_error_string2(login.library_error);
        } else if (login.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION &&
                   login.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
          auto* m = static_cast<amqp_connection_close_t*>(login.reply.decoded);
          reason = std::to_string(m->reply_code) + " " + bytesToString(m->reply_text);
        } else {
          reason = "login failed";
        }
        throw AmqpError("cannot log in to " + u.host + ":" + std::to_string(u.port) + ": " + reason, 0, false);
      }
      const int negotiatedMax = amqp_get_channel_max(conn);
      auto c = std::make_shared<RabbitConnection>(conn, negotiatedMax > 0 ? negotiatedMax : channelMax);
      conn = nullptr;
      c->start();
      return c;
    } catch (...) {
      if (conn != nullptr) amqp_destroy_connection(conn);
      throw;
    }
  }

 private:
  static int intParam(const std::string& v, const char* name) {
    if (v.empty() || v.size() > 9 || v.find_first_not_of("0123456789") != std::string::npos) {
      throw AmqpError(std::string("broker URL parameter ") + name + " must be a non-negative integer", 0, false);
    }
    return std::stoi(v);
  }
};

}  // namespace

std::shared_ptr<Transport> rabbitmqTransport() {
  static auto t = std::make_shared<RabbitTransport>();
  return t;
}

}  // namespace protobus::amqp
