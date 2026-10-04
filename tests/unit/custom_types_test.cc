#include <gtest/gtest.h>

#include "pbtest.pb.h"
#include "protobus/custom_types.h"
#include "protobus/errors.h"

namespace protobus {
namespace {

TEST(Uint256, ParsesDecimalAndHex) {
  EXPECT_EQ(Uint256::parse("0").toString(), "0");
  EXPECT_EQ(Uint256::parse("1180591620717411303424").toString(), "1180591620717411303424");  // 2^70
  EXPECT_EQ(Uint256::parse("0xff").toString(), "255");
  EXPECT_EQ(Uint256::parse("0x10000000000000000").toString(), "18446744073709551616");
  EXPECT_EQ(Uint256::parse("255").toHex(), "0xff");
}

TEST(Uint256, RefusesWhatItCannotRepresent) {
  EXPECT_THROW(Uint256::parse("-1"), CustomTypeRangeError);
  EXPECT_THROW(Uint256::parse(""), CustomTypeRangeError);
  EXPECT_THROW(Uint256::parse("12a"), CustomTypeRangeError);
  EXPECT_THROW(Uint256::parse("0xzz"), CustomTypeRangeError);
  const std::string max =
      "115792089237316195423570985008687907853269984665640564039457584007913129639935";  // 2^256-1
  EXPECT_EQ(Uint256::parse(max), Uint256::max());
  EXPECT_THROW(Uint256::parse(max.substr(0, max.size() - 1) + "6"), CustomTypeRangeError);
}

TEST(Uint256, ArithmeticIsChecked) {
  EXPECT_EQ((Uint256(2) + Uint256(3)).toString(), "5");
  EXPECT_EQ((Uint256::parse("1180591620717411303424") + 1).toString(), "1180591620717411303425");
  EXPECT_EQ((Uint256(10) - Uint256(4)).toString(), "6");
  EXPECT_EQ((Uint256::parse("18446744073709551616") - 1).toString(), "18446744073709551615");
  EXPECT_EQ((Uint256::parse("4294967296") * Uint256::parse("4294967296")).toString(), "18446744073709551616");
  EXPECT_THROW(Uint256::max() + 1, CustomTypeRangeError);
  EXPECT_THROW(Uint256(1) - Uint256(2), CustomTypeRangeError);
  EXPECT_THROW(Uint256::max() * 2, CustomTypeRangeError);
  EXPECT_LT(Uint256(1), Uint256(2));
}

TEST(Bigint, IsExactly32BigEndianBytes) {
  auto b = makeBigint(Uint256(258));
  ASSERT_EQ(b.value().size(), 32u);
  EXPECT_EQ(static_cast<unsigned char>(b.value()[30]), 1);
  EXPECT_EQ(static_cast<unsigned char>(b.value()[31]), 2);
  EXPECT_EQ(toUint256(b), Uint256(258));
}

TEST(Bigint, ZeroIsStillWritten) {
  // TypeScript writes the 32 zero bytes; `optional` keeps presence so C++
  // does too.
  auto b = makeBigint(Uint256(0));
  EXPECT_TRUE(b.has_value());
  EXPECT_EQ(b.ByteSizeLong(), 34u);
}

TEST(Bigint, DecodesShortValuesAndRefusesWideOnes) {
  ::bigint b;
  b.set_value(std::string("\x01\x00", 2));
  EXPECT_EQ(toUint256(b), Uint256(256));
  ::bigint empty;
  EXPECT_EQ(toUint256(empty), Uint256(0));
  ::bigint wide;
  wide.set_value(std::string(33, '\x01'));
  EXPECT_THROW(toUint256(wide), CustomTypeRangeError);
}

TEST(Timestamp, CarriesSignedMilliseconds) {
  using namespace std::chrono;
  const auto t = system_clock::time_point(milliseconds(-14182940000));  // 1969-07-20T20:17:40Z
  auto ts = makeTimestamp(t);
  EXPECT_EQ(ts.value(), -14182940000);
  EXPECT_EQ(toTimePoint(ts).time_since_epoch().count(), -14182940000);
  EXPECT_THROW(makeTimestampMs(kMaxTimestampMs + 1), CustomTypeRangeError);
}

TEST(Validation, RefusesWideBigintsAnywhereInAMessage) {
  pbtest::Wallet ok;
  *ok.mutable_amount() = makeBigint(Uint256(5));
  (*ok.mutable_balances())["k"] = makeBigint(Uint256(7));
  *ok.add_parts() = makeBigint(Uint256(9));
  EXPECT_NO_THROW(validateCustomTypes(ok));

  pbtest::Wallet direct = ok;
  direct.mutable_amount()->set_value(std::string(33, 'x'));
  EXPECT_THROW(validateCustomTypes(direct), CustomTypeRangeError);

  pbtest::Wallet inMap = ok;
  (*inMap.mutable_balances())["bad"].set_value(std::string(40, 'x'));
  EXPECT_THROW(validateCustomTypes(inMap), CustomTypeRangeError);

  pbtest::Wallet repeated = ok;
  repeated.add_parts()->set_value(std::string(33, 'x'));
  EXPECT_THROW(validateCustomTypes(repeated), CustomTypeRangeError);

  // A message that cannot hold a bigint is not walked at all.
  pbtest::AddRequest plain;
  EXPECT_NO_THROW(validateCustomTypes(plain));
}

TEST(Registry, IdempotentButRefusesAWireTypeChange) {
  registerCustomType({"uuid_t1", CustomWireType::Bytes});
  EXPECT_NO_THROW(registerCustomType({"uuid_t1", CustomWireType::Bytes}));
  EXPECT_THROW(registerCustomType({"uuid_t1", CustomWireType::String}), CustomTypeConflictError);
  EXPECT_TRUE(isCustomType("uuid_t1"));
  EXPECT_TRUE(isCustomType("bigint"));
  EXPECT_THROW(registerCustomType({"bigint", CustomWireType::String}), CustomTypeConflictError);
  EXPECT_THROW(registerCustomType({"not valid", CustomWireType::Bytes}), CustomTypeConflictError);
}

}  // namespace
}  // namespace protobus
