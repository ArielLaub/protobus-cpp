#include "uuid.h"

#include <array>
#include <cstdint>
#include <random>

namespace protobus::detail {

std::string randomUuid() {
  // One generator per thread, seeded from the OS: ids are minted on many
  // threads at once and must not collide across processes.
  thread_local std::mt19937_64 gen = [] {
    std::random_device rd;
    std::seed_seq seq{rd(), rd(), rd(), rd(), rd(), rd(), rd(), rd()};
    return std::mt19937_64(seq);
  }();
  std::array<uint8_t, 16> b{};
  const uint64_t hi = gen();
  const uint64_t lo = gen();
  for (int i = 0; i < 8; ++i) {
    b[i] = static_cast<uint8_t>(hi >> (56 - 8 * i));
    b[8 + i] = static_cast<uint8_t>(lo >> (56 - 8 * i));
  }
  b[6] = static_cast<uint8_t>((b[6] & 0x0f) | 0x40);
  b[8] = static_cast<uint8_t>((b[8] & 0x3f) | 0x80);
  static const char* hex = "0123456789abcdef";
  std::string out;
  out.reserve(36);
  for (int i = 0; i < 16; ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) out.push_back('-');
    out.push_back(hex[b[i] >> 4]);
    out.push_back(hex[b[i] & 0x0f]);
  }
  return out;
}

}  // namespace protobus::detail
