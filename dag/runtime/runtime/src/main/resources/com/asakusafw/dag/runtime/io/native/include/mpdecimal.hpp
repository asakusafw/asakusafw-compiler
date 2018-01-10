/*
 * Copyright 2011-2018 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ASAKUSAFW_MPDECIMAL_HPP
#define ASAKUSAFW_MPDECIMAL_HPP

#include <vector>
#include <cstddef>
#include <cstdint>

namespace asakusafw {
namespace math {

/*
 * \brief Represents a comparing two values.
 */
enum class Sign : int {

    /*
     * \brief the first one is less than the second one.
     */
    less_than = -1,

    /*
     * \brief both are equivalent.
     */
    equal_to = 0,

    /*
     * \brief the first one is greater than the second one.
     */
    greater_than = +1,
};

/*
 * \brief Returns a negated sign value.
 * \param sign the original sign value
 * \return the negated sign value
 */
inline
Sign negate(Sign sign) {
    return static_cast<Sign>(-static_cast<int>(sign));
}

class MpInt;
class CompactDecimal;
class MpDecimal;

/*
 * \brief A simple multi-precision unsigned integer.
 */
class MpInt {
public:
    /*
     * \brief The default constructor.
     * The created object will just represent zero.
     */
    MpInt() = default;

    /*
     * \brief The destructor.
     */
    ~MpInt() = default;

    /*
     * \brief A constructor.
     * \param value the integer
     */
    MpInt(uint64_t value);

    /*
     * \brief A constructor.
     * \param bytes the binary integer, in network byte order
     * \param length the byte length of binary integer
     */
    MpInt(const uint8_t *bytes, std::size_t length);

    /*
     * \brief The copy constructor.
     * \param other the source object
     */
    MpInt(const MpInt& other) = default;

    /*
     * \brief The move constructor.
     * \param other the source object
     */
    MpInt(MpInt&& other) noexcept = default;

    MpInt& operator=(const MpInt&) = delete;
    MpInt& operator=(MpInt&&) noexcept = delete;

    /**
     * \brief Returns the number of available bits.
     * \return the number of available bits
     */
    std::size_t bits() const;

    /**
     * \brief Returns a copy of bytes, as the network byte order.
     * \return a copy of bytes as network byte order
     */
    std::vector<uint8_t> data() const;

    /**
     * \brief Compares this to another integer.
     * \param other another integer
     * \return less_than - if this is less than another,
     *     equal_to - if this and another are equivalent,
     *     greater_than - otherwise
     */
    Sign compare_to(uint64_t other) const;

    /**
     * \brief Compares this to another multi-precision integer.
     * \param other another integer
     * \return less_than - if this is less than another,
     *     equal_to - if this and another are equivalent,
     *     greater_than - otherwise
     */
    Sign compare_to(const MpInt& other) const;

    /**
     * \brief Returns 10^{exponent}.
     * \param exponent the exponent
     * \return 10^{exponent} as a multi-precision integer
     */
    static
    const MpInt& power_of_10(uint32_t exponent);

    /*
     * \brief Returns whether or not this and another integer are equivalent.
     * \param other another integer
     * \return true if both are equivalent, otherwise false
     */
    inline
    bool operator==(const MpInt& other) const {
        return m_members == other.m_members;
    }

    /*
     * \brief Returns whether or not this and another integer are NOT equivalent.
     * \param other another integer
     * \return true if both are NOT equivalent, otherwise false
     */
    inline
    bool operator!=(const MpInt& other) const {
        return !(*this == other);
    }

    /*
     * \brief Returns whether or not this and another integer are equivalent.
     * \param other another integer
     * \return true if both are equivalent, otherwise false
     */
    inline
    bool operator==(uint64_t other) const {
        if (m_members.empty()) {
            return other == UINT64_C(0);
        } else if (other == UINT64_C(0)) {
            return false;
        } else if (m_members.size() == 1) {
            return m_members.front() == other;
        } else if (m_members.size() == 2) {
            return m_members[0] == static_cast<uint32_t>(other)
                    && m_members[1] == static_cast<uint32_t>(other >> 32);
        } else {
            return false;
        }
    }

    /*
     * \brief Returns whether or not this and another integer are NOT equivalent.
     * \param other another integer
     * \return true if both are NOT equivalent, otherwise false
     */
    inline
    bool operator!=(uint64_t other) const {
        return !(*this == other);
    }

    /*
     * \brief Returns whether or not the two integers are equivalent.
     * \param a the first integer
     * \param b the second integer
     * \return true if both are equivalent, otherwise false
     */
    inline
    friend bool operator==(uint64_t a, const MpInt& b) {
        return b == a;
    }

    /*
     * \brief Returns whether or not the two integers are NOT equivalent.
     * \param a the first integer
     * \param b the second integer
     * \return true if both are NOT equivalent, otherwise false
     */
    inline
    friend bool operator!=(uint64_t a, const MpInt& b) {
        return !(a == b);
    }

    /*
     * \brief Returns the product of this and the multiplier.
     * \param multiplier the multiplier
     * \return the product
     */
    MpInt operator*(const MpInt& multiplier) const;

    /*
     * \brief Returns the product of this and the multiplier.
     * \param multiplier the multiplier
     * \return the product
     */
    MpInt operator*(uint32_t multiplier) const;

    /*
     * \brief Returns the product of the two integers.
     * \param a the multiplicand
     * \param a the multiplier
     * \return the product
     */
    inline
    friend MpInt operator*(uint32_t a, const MpInt& b) {
        return b * a;
    }

private:
    MpInt(const std::vector<uint32_t>& members);
    MpInt(std::vector<uint32_t>&& members);
    std::vector<uint32_t> m_members;
};

/*
 * \brief A simple compact unsigned decimal.
 * This only can have a significand in the range of [0, 2^{64}).
 */
class CompactDecimal {
public:
    /*
     * \brief The default constructor.
     * The created object will just represent zero.
     */
    CompactDecimal() = default;

    /*
     * \brief The destructor.
     */
    ~CompactDecimal() = default;

    /*
     * \brief The copy constructor.
     * \param other the source object
     */
    CompactDecimal(const CompactDecimal& other) = default;

    /*
     * \brief The move constructor.
     * \param other the source object
     */
    CompactDecimal(CompactDecimal&& other) noexcept = default;

    CompactDecimal& operator=(const CompactDecimal&) = delete;
    CompactDecimal& operator=(CompactDecimal&&) noexcept = delete;

    /*
     * \brief A constructor.
     * \param significand the significand
     * \param exponent the ten's exponent
     */
    CompactDecimal(uint64_t significand, int32_t exponent)
            : m_significand(significand)
            , m_exponent(exponent) {};

    /*
     * \brief Returns the significand.
     * \return the significand
     */
    inline
    uint64_t significand() const {
        return m_significand;
    }

    /*
     * \brief Returns the ten's exponent.
     * \return the exponent
     */
    inline
    int32_t exponent() const {
        return m_exponent;
    }

    /*
     * \brief Compares this with another decimal.
     * \param other another decimal
     * \return less_than - if this is less than another,
     *     equal_to - if this and another are equivalent,
     *     greater_than - otherwise
     */
    Sign compare_to(const CompactDecimal& other) const;

    /*
     * \brief Compares this with another decimal.
     * \param other another decimal
     * \return less_than - if this is less than another,
     *     equal_to - if this and another are equivalent,
     *     greater_than - otherwise
     */
    Sign compare_to(const MpDecimal& other) const;

private:
    const uint64_t m_significand;
    const int32_t m_exponent;
};

/*
 * \brief A simple multi-precision unsigned decimal.
 */
class MpDecimal {
public:
    /*
     * \brief The default constructor.
     * The created object will just represent zero.
     */
    MpDecimal() = default;

    /*
     * \brief The destructor.
     */
    ~MpDecimal() = default;

    /*
     * \brief The copy constructor.
     * \param other the source object
     */
    MpDecimal(const MpDecimal& other) = default;

    /*
     * \brief The move constructor.
     * \param other the source object
     */
    MpDecimal(MpDecimal&& other) noexcept
            : m_significand(std::move(other.m_significand))
            , m_exponent(other.m_exponent) {};

    MpDecimal& operator=(const MpDecimal&) = delete;
    MpDecimal& operator=(MpDecimal&&) noexcept = delete;

    /*
     * \brief A constructor.
     * \param bytes the binary integer of significand, in network byte order
     * \param length the byte length of binary integer
     * \param exponent the ten's exponent
     */
    MpDecimal(const uint8_t *bytes, std::size_t length, int32_t exponent)
            : m_significand(bytes, length)
            , m_exponent(exponent) {};

    /*
     * \brief A constructor.
     * \param significand the significand
     * \param exponent the ten's exponent
     */
    MpDecimal(const MpInt& significand, int32_t exponent)
            : m_significand(significand)
            , m_exponent(exponent) {};

    /*
     * \brief A constructor.
     * \param significand the significand
     * \param exponent the ten's exponent
     */
    MpDecimal(MpInt&& significand, int32_t exponent)
            : m_significand(std::move(significand))
            , m_exponent(exponent) {};

    /*
     * \brief Returns the significand.
     * \return the significand
     */
    inline
    const MpInt& significand() const {
        return m_significand;
    }

    /*
     * \brief Returns the ten's exponent.
     * \return the exponent
     */
    inline
    int32_t exponent() const {
        return m_exponent;
    };

    /*
     * \brief Compares this with another decimal.
     * \param other another decimal
     * \return less_than - if this is less than another,
     *     equal_to - if this and another are equivalent,
     *     greater_than - otherwise
     */
    Sign compare_to(const CompactDecimal& other) const;

    /*
     * \brief Compares this with another decimal.
     * \param other another decimal
     * \return less_than - if this is less than another,
     *     equal_to - if this and another are equivalent,
     *     greater_than - otherwise
     */
    Sign compare_to(const MpDecimal& other) const;

private:
    const MpInt m_significand;
    const int32_t m_exponent;
};

/*
 * \brief Compares two decimals.
 * \param a_buf the binary integer of significand in the first decimal, in network byte order
 * \param a_length the byte length of binary integer in the first decimal
 * \param a_exponent the ten's exponent in the first decimal
 * \param b_buf the binary integer of significand in the second decimal, in network byte order
 * \param b_length the byte length of binary integer in the second decimal
 * \param b_exponent the ten's exponent in the second decimal
 * \return less_than - if the first one is less than the second one,
 *     equal_to - if the two decimals are equivalent,
 *     greater_than - otherwise
 */
Sign compare_decimal(
        const uint8_t *a_buf, std::size_t a_length, int32_t a_exponent,
        const uint8_t *b_buf, std::size_t b_length, int32_t b_exponent);

/*
 * \brief Compares two decimals.
 * \param a_buf the binary integer of significand in the first decimal, in network byte order
 * \param a_length the byte length of binary integer in the first decimal
 * \param a_exponent the ten's exponent in the first decimal
 * \param b_significand the significand in the second decimal
 * \param b_exponent the ten's exponent in the second decimal
 * \return less_than - if the first one is less than the second one,
 *     equal_to - if the two decimals are equivalent,
 *     greater_than - otherwise
 */
Sign compare_decimal(
        const uint8_t *a_buf, std::size_t a_length, int32_t a_exponent,
        uint64_t b_significand, int32_t b_exponent);

/*
 * \brief Compares two decimals.
 * \param a_significand the significand in the first decimal
 * \param a_exponent the ten's exponent in the first decimal
 * \param b_buf the binary integer of significand in the second decimal, in network byte order
 * \param b_length the byte length of binary integer in the second decimal
 * \param b_exponent the ten's exponent in the second decimal
 * \return less_than - if the first one is less than the second one,
 *     equal_to - if the two decimals are equivalent,
 *     greater_than - otherwise
 */
inline
Sign compare_decimal(
        uint64_t a_significand, int32_t a_exponent,
        const uint8_t *b_buf, std::size_t b_length, int32_t b_exponent) {
    return negate(compare_decimal(b_buf, b_length, b_exponent, a_significand, a_exponent));
}

/*
 * \brief Compares two decimals.
 * \param a_significand the significand in the first decimal
 * \param a_exponent the ten's exponent in the first decimal
 * \param b_significand the significand in the second decimal
 * \param b_exponent the ten's exponent in the second decimal
 * \return less_than - if the first one is less than the second one,
 *     equal_to - if the two decimals are equivalent,
 *     greater_than - otherwise
 */
Sign compare_decimal(
        uint64_t a_significand, int32_t a_exponent,
        uint64_t b_significand, int32_t b_exponent);

} // namespace math
} // namespace asakusafw

#endif // ASAKUSAFW_MPDECIMAL_HPP
