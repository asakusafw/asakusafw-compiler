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
#ifndef ASAKUSAFW_SERDE_HPP
#define ASAKUSAFW_SERDE_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <stdexcept>

namespace asakusafw {
namespace serde {

static
const int8_t DECIMAL_NULL = 0;

static
const int8_t DECIMAL_PLUS_MASK = 1 << 1;

static
const int8_t DECIMAL_COMPACT_MASK = 1 << 2;

static
const int8_t COMPACT_INT_HEAD_MIN = INT8_MIN + 4;

static
const int8_t NULL_HEADER = 0;

static
const int8_t UNSIGNED_NULL = -1;

template<typename T>
static
inline
int compare_value(T a, T b) {
    return a == b ? 0 : a < b ? -1 : +1;
}

template<typename T>
static
inline
T read_value(int8_t *&p) {
    auto v = *(T *)(p);
    p += sizeof(T);
    return v;
}

static
inline
std::size_t compact_int_size(int8_t head) {
    if (head >= COMPACT_INT_HEAD_MIN) {
        return 1;
    }
    auto scale = COMPACT_INT_HEAD_MIN - head;
    return static_cast<std::size_t>(1 << (scale - 1)) + 1;
}

static
inline
int64_t read_compact_int(int8_t *&p) {
    auto b0 = read_value<int8_t>(p);
    if (b0 >= COMPACT_INT_HEAD_MIN) {
        return b0;
    }
    auto scale = COMPACT_INT_HEAD_MIN - b0;
    switch (scale) {
    case 1:
        return read_value<int8_t>(p);
    case 2:
        return read_value<int16_t>(p);
    case 3:
        return read_value<int32_t>(p);
    case 4:
        return read_value<int64_t>(p);
    }
    return 0;
}

static
inline
int compare_boolean(int8_t *&a, int8_t *&b) {
    auto va = read_value<int8_t>(a);
    auto vb = read_value<int8_t>(b);
    return compare_value(va, vb);
}

template<typename T>
static
inline
int compare_numeric(int8_t *&a, int8_t *&b) {
    auto na = read_value<int8_t>(a);
    auto nb = read_value<int8_t>(b);
    if (na == NULL_HEADER) {
        if (nb == NULL_HEADER) {
            return 0;
        } else {
            return -1;
        }
    } else if (nb == NULL_HEADER) {
        return +1;
    }
    auto va = read_value<T>(a);
    auto vb = read_value<T>(b);
    return compare_value(va, vb);
}

static
inline
int compare_byte(int8_t *&a, int8_t *&b) {
    return compare_numeric<int8_t>(a, b);
}

static
inline
int compare_short(int8_t *&a, int8_t *&b) {
    return compare_numeric<int16_t>(a, b);
}

static
inline
int compare_int(int8_t *&a, int8_t *&b) {
    return compare_numeric<int32_t>(a, b);
}

static
inline
int compare_long(int8_t *&a, int8_t *&b) {
    return compare_numeric<int64_t>(a, b);
}

static
inline
int compare_float(int8_t *&a, int8_t *&b) {
    return compare_numeric<float>(a, b);
}

static
inline
int compare_double(int8_t *&a, int8_t *&b) {
    return compare_numeric<double>(a, b);
}

template<typename T>
static
inline
int compare_unsigned(int8_t *&a, int8_t *&b) {
    auto va = read_value<T>(a);
    auto vb = read_value<T>(b);
    if (va < 0) {
        if (vb < 0) {
            return 0;
        } else {
            return -1;
        }
    } else if (vb < 0) {
        return +1;
    }
    return compare_value(va, vb);
}

static
inline
int compare_date(int8_t *&a, int8_t *&b) {
    return compare_unsigned<int32_t>(a, b);
}

static
inline
int compare_date_time(int8_t *&a, int8_t *&b) {
    return compare_unsigned<int64_t>(a, b);
}

static
inline
int compare_string(int8_t *&a, int8_t *&b) {
    auto len_a = read_compact_int(a);
    auto len_b = read_compact_int(b);
    if (len_a < 0) {
        if (len_b < 0) {
            return 0;
        } else {
            return -1;
        }
    } else if (len_b < 0) {
        return +1;
    }
    int diff = memcmp(a, b, static_cast<std::size_t>(std::min(len_a, len_b)));
    if (diff != 0) {
        return diff;
    }
    a += len_a;
    b += len_b;
    return compare_value(len_a, len_b);
}

int compare_decimal(int8_t *&a, int8_t *&b);

template<typename T>
static
inline
void skip_value(int8_t *&p) {
    p += sizeof(T);
}

template<typename T>
static
inline
void skip_numeric(int8_t *&p) {
    auto n = read_value<int8_t>(p);
    if (n != NULL_HEADER) {
        skip_value<T>(p);
    }
}

static
inline
void skip_boolean(int8_t *&p) {
    skip_value<int8_t>(p);
}

static
inline
void skip_byte(int8_t *&p) {
    skip_numeric<int8_t>(p);
}

static
inline
void skip_short(int8_t *&p) {
    skip_numeric<int16_t>(p);
}

static
inline
void skip_int(int8_t *&p) {
    skip_numeric<int32_t>(p);
}

static
inline
void skip_long(int8_t *&p) {
    skip_numeric<int64_t>(p);
}

static
inline
void skip_float(int8_t *&p) {
    skip_numeric<float>(p);
}

static
inline
void skip_double(int8_t *&p) {
    skip_numeric<double>(p);
}

static
inline
void skip_date(int8_t *&p) {
    p += sizeof(int32_t);
}

static
inline
void skip_date_time(int8_t *&p) {
    p += sizeof(int64_t);
}

static
inline
void skip_string(int8_t *&p) {
    auto len = read_compact_int(p);
    if (len > 0) {
        p += len;
    }
}

static
inline
void skip_decimal(int8_t *&p) {
    auto head = read_value<int8_t>(p);
    if (head == DECIMAL_NULL) {
        return;
    }
    auto compact = (head & DECIMAL_COMPACT_MASK) != 0;
    // scale
    p += compact_int_size(*p);
    if (compact) {
        // compact
        p += compact_int_size(*p);
    } else {
        // body length
        auto len = read_compact_int(p);
        // body
        p += len;
    }
}

} // namespace serde
} // namespace asakusafw

#endif // ASAKUSAFW_SERDE_HPP
