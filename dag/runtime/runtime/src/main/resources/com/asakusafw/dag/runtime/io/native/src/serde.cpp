/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
#include "serde.hpp"
#include "mpdecimal.hpp"

using namespace asakusafw::serde;

int asakusafw::serde::compare_decimal(int8_t *&a, int8_t *&b) {
    auto head_a = read_value<int8_t>(a);
    auto head_b = read_value<int8_t>(b);
    if (head_a == DECIMAL_NULL) {
        if (head_b == DECIMAL_NULL) {
            return 0;
        } else {
            return -1;
        }
    } else if (head_b == DECIMAL_NULL) {
        return +1;
    }
    bool plus_a = (head_a & DECIMAL_PLUS_MASK) != 0;
    bool plus_b = (head_b & DECIMAL_PLUS_MASK) != 0;
    if (plus_a != plus_b) {
        return plus_a ? +1 : -1;
    }
    bool compact_a = (head_a & DECIMAL_COMPACT_MASK) != 0;
    bool compact_b = (head_b & DECIMAL_COMPACT_MASK) != 0;
    int32_t scale_a = static_cast<int32_t>(read_compact_int(a));
    int32_t scale_b = static_cast<int32_t>(read_compact_int(b));
    int64_t unscaled_a = read_compact_int(a);
    int64_t unscaled_b = read_compact_int(b);
    assert(unscaled_a >= 0);
    assert(unscaled_b >= 0);
    asakusafw::math::Sign sign;
    if (compact_a && compact_b) {
        sign = asakusafw::math::compare_decimal(
                static_cast<uint64_t>(unscaled_a), -scale_a,
                static_cast<uint64_t>(unscaled_b), -scale_b);
    } else if (compact_a) {
        const uint8_t *b_buf = reinterpret_cast<uint8_t*>(b);
        std::size_t b_length = static_cast<std::size_t>(unscaled_b);
        b += b_length;
        sign = asakusafw::math::compare_decimal(
                static_cast<uint64_t>(unscaled_a), -scale_a,
                b_buf, b_length, -scale_b);
    } else if (compact_b) {
        const uint8_t *a_buf = reinterpret_cast<uint8_t*>(a);
        std::size_t a_length = static_cast<std::size_t>(unscaled_a);
        a += a_length;
        sign = asakusafw::math::compare_decimal(
                a_buf, a_length, -scale_a,
                static_cast<uint64_t>(unscaled_b), -scale_b);
    } else {
        const uint8_t *a_buf = reinterpret_cast<uint8_t*>(a);
        std::size_t a_length = static_cast<std::size_t>(unscaled_a);
        a += a_length;
        const uint8_t *b_buf = reinterpret_cast<uint8_t*>(b);
        std::size_t b_length = static_cast<std::size_t>(unscaled_b);
        b += b_length;
        sign = asakusafw::math::compare_decimal(
                a_buf, a_length, -scale_a,
                b_buf, b_length, -scale_b);
    }
    return static_cast<int>(plus_a ? sign : asakusafw::math::negate(sign));
}
