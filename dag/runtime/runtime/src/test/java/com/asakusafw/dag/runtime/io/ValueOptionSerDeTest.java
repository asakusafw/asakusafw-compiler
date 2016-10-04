/**
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
package com.asakusafw.dag.runtime.io;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.DataInput;
import java.math.BigDecimal;

import org.junit.Test;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.runtime.io.util.DataBuffer;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;

/**
 * Test for {@link ValueOptionSerDe}.
 */
public class ValueOptionSerDeTest {

    /**
     * {@link BooleanOption}.
     */
    @Test
    public void serde_boolean() {
        check(new BooleanOption());
        check(new BooleanOption(true));
        check(new BooleanOption(false));
    }

    /**
     * {@link ByteOption}.
     */
    @Test
    public void serde_byte() {
        check(new ByteOption());
        check(new ByteOption((byte) 0));
        check(new ByteOption((byte) +1));
        check(new ByteOption((byte) -1));
        check(new ByteOption(Byte.MAX_VALUE));
        check(new ByteOption(Byte.MIN_VALUE));
    }

    /**
     * {@link ShortOption}.
     */
    @Test
    public void serde_short() {
        check(new ShortOption());
        check(new ShortOption((short) 0));
        check(new ShortOption((short) +1));
        check(new ShortOption((short) -1));
        check(new ShortOption(Short.MAX_VALUE));
        check(new ShortOption(Short.MIN_VALUE));
    }

    /**
     * {@link IntOption}.
     */
    @Test
    public void serde_int() {
        check(new IntOption());
        check(new IntOption(0));
        check(new IntOption(+1));
        check(new IntOption(-1));
        check(new IntOption(Integer.MAX_VALUE));
        check(new IntOption(Integer.MIN_VALUE));
    }

    /**
     * {@link LongOption}.
     */
    @Test
    public void serde_long() {
        check(new LongOption());
        check(new LongOption(0));
        check(new LongOption(+1));
        check(new LongOption(-1));
        check(new LongOption(Long.MAX_VALUE));
        check(new LongOption(Long.MIN_VALUE));
    }

    /**
     * {@link FloatOption}.
     */
    @Test
    public void serde_float() {
        check(new FloatOption());
        check(new FloatOption(0f));
        check(new FloatOption(+1f));
        check(new FloatOption(-1f));
        check(new FloatOption(Float.MAX_VALUE));
        check(new FloatOption(Float.MIN_VALUE));
        check(new FloatOption(Float.POSITIVE_INFINITY));
        check(new FloatOption(Float.NEGATIVE_INFINITY));
        check(new FloatOption(Float.NaN));
    }

    /**
     * {@link DoubleOption}.
     */
    @Test
    public void serde_double() {
        check(new DoubleOption());
        check(new DoubleOption(0));
        check(new DoubleOption(+1));
        check(new DoubleOption(-1));
        check(new DoubleOption(Double.MAX_VALUE));
        check(new DoubleOption(Double.MIN_VALUE));
        check(new DoubleOption(Double.POSITIVE_INFINITY));
        check(new DoubleOption(Double.NEGATIVE_INFINITY));
        check(new DoubleOption(Double.NaN));
    }

    /**
     * {@link DateOption}.
     */
    @Test
    public void serde_date() {
        check(new DateOption());
        check(new DateOption(new Date(0)));
        check(new DateOption(new Date(1)));
        check(new DateOption(new Date(2015, 4, 1)));
        check(new DateOption(new Date(Integer.MAX_VALUE)));
    }

    /**
     * {@link DateTimeOption}.
     */
    @Test
    public void serde_date_time() {
        check(new DateTimeOption());
        check(new DateTimeOption(new DateTime(0)));
        check(new DateTimeOption(new DateTime(1)));
        check(new DateTimeOption(new DateTime(2015, 4, 1, 0, 1, 2)));
        check(new DateTimeOption(new DateTime(Long.MAX_VALUE)));
    }

    /**
     * {@link StringOption}.
     */
    @Test
    public void serde_string() {
        check(new StringOption());
        check(new StringOption(""));
        check(new StringOption("0"));
        check(new StringOption("Hello, world!"));
    }

    /**
     * {@link StringOption} for same byte length.
     */
    @Test
    public void serde_string_opt() {
        StringOption buf = new StringOption();

        deserialize(buf, serialize(new StringOption("000")));
        assertThat(buf, is(new StringOption("000")));

        deserialize(buf, serialize(new StringOption("123")));
        assertThat(buf, is(new StringOption("123")));
    }

    /**
     * {@link DateTimeOption}.
     */
    @Test
    public void serde_decimal() {
        check(new DecimalOption());

        check(new DecimalOption(new BigDecimal(0)));
        check(new DecimalOption(new BigDecimal(1)));
        check(new DecimalOption(new BigDecimal(-1)));

        check(new DecimalOption(new BigDecimal(Long.MAX_VALUE)));
        check(new DecimalOption(new BigDecimal(Long.MIN_VALUE + 1)));

        check(new DecimalOption(new BigDecimal("3.14")));
        check(new DecimalOption(new BigDecimal("-3.14")));

        check(new DecimalOption(new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE)));
        check(new DecimalOption(new BigDecimal(Long.MIN_VALUE + 1).subtract(BigDecimal.ONE)));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareBoolean(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_boolean() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareBoolean;

        compare(cmp, new BooleanOption(false), new BooleanOption(false));
        compare(cmp, new BooleanOption(true), new BooleanOption(true));
        compare(cmp, new BooleanOption(true), new BooleanOption(false));
        compare(cmp, new BooleanOption(false), new BooleanOption(true));

        compare(cmp, new BooleanOption(), new BooleanOption());
        compare(cmp, new BooleanOption(false), new BooleanOption());
        compare(cmp, new BooleanOption(), new BooleanOption(false));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareByte(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_byte() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareByte;

        compare(cmp, new ByteOption((byte) 0), new ByteOption((byte) 0));
        compare(cmp, new ByteOption((byte) 1), new ByteOption((byte) 0));
        compare(cmp, new ByteOption((byte) 0), new ByteOption((byte) 1));

        compare(cmp, new ByteOption(), new ByteOption());
        compare(cmp, new ByteOption((byte) -1), new ByteOption());
        compare(cmp, new ByteOption(), new ByteOption((byte) -1));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareShort(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_short() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareShort;

        compare(cmp, new ShortOption((short) 0), new ShortOption((short) 0));
        compare(cmp, new ShortOption((short) 1), new ShortOption((short) 0));
        compare(cmp, new ShortOption((short) 0), new ShortOption((short) 1));

        compare(cmp, new ShortOption(), new ShortOption());
        compare(cmp, new ShortOption((short) -1), new ShortOption());
        compare(cmp, new ShortOption(), new ShortOption((short) -1));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareInt(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_int() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareInt;

        compare(cmp, new IntOption(0), new IntOption(0));
        compare(cmp, new IntOption(1), new IntOption(0));
        compare(cmp, new IntOption(0), new IntOption(1));

        compare(cmp, new IntOption(), new IntOption());
        compare(cmp, new IntOption(-1), new IntOption());
        compare(cmp, new IntOption(), new IntOption(-1));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareLong(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_long() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareLong;

        compare(cmp, new LongOption(0), new LongOption(0));
        compare(cmp, new LongOption(1), new LongOption(0));
        compare(cmp, new LongOption(0), new LongOption(1));

        compare(cmp, new LongOption(), new LongOption());
        compare(cmp, new LongOption(-1), new LongOption());
        compare(cmp, new LongOption(), new LongOption(-1));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareFloat(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_float() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareFloat;

        compare(cmp, new FloatOption(0), new FloatOption(0));
        compare(cmp, new FloatOption(1), new FloatOption(0));
        compare(cmp, new FloatOption(0), new FloatOption(1));

        compare(cmp, new FloatOption(), new FloatOption());
        compare(cmp, new FloatOption(-1), new FloatOption());
        compare(cmp, new FloatOption(), new FloatOption(-1));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareDouble(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_double() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareDouble;

        compare(cmp, new DoubleOption(0), new DoubleOption(0));
        compare(cmp, new DoubleOption(1), new DoubleOption(0));
        compare(cmp, new DoubleOption(0), new DoubleOption(1));

        compare(cmp, new DoubleOption(), new DoubleOption());
        compare(cmp, new DoubleOption(-1), new DoubleOption());
        compare(cmp, new DoubleOption(), new DoubleOption(-1));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareDate(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_date() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareDate;

        compare(cmp, newDate(0), newDate(0));
        compare(cmp, newDate(1), newDate(0));
        compare(cmp, newDate(0), newDate(1));

        compare(cmp, new DateOption(), new DateOption());
        compare(cmp, newDate(0), new DateOption());
        compare(cmp, new DateOption(), newDate(0));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareDateTime(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_date_time() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareDateTime;

        compare(cmp, newDateTime(0), newDateTime(0));
        compare(cmp, newDateTime(1), newDateTime(0));
        compare(cmp, newDateTime(0), newDateTime(1));

        compare(cmp, new DateTimeOption(), new DateTimeOption());
        compare(cmp, newDateTime(0), new DateTimeOption());
        compare(cmp, new DateTimeOption(), newDateTime(0));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareString(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_string() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareString;

        compare(cmp, new StringOption("a"), new StringOption("a"));
        compare(cmp, new StringOption("b"), new StringOption("a"));
        compare(cmp, new StringOption("a"), new StringOption("b"));

        compare(cmp, new StringOption("AAA"), new StringOption("AAA"));
        compare(cmp, new StringOption("ABA"), new StringOption("AAB"));
        compare(cmp, new StringOption("AAB"), new StringOption("ABA"));

        compare(cmp, new StringOption("A"), new StringOption("AA"));
        compare(cmp, new StringOption("AB"), new StringOption("ABA"));

        compare(cmp, new StringOption(), new StringOption());
        compare(cmp, new StringOption("a"), new StringOption());
        compare(cmp, new StringOption(), new StringOption("a"));
    }

    /**
     * Test for {@link ValueOptionSerDe#compareDecimal(DataInput, DataInput)}.
     * @throws Exception if failed
     */
    @Test
    public void compare_decimal() throws Exception {
        DataComparator cmp = ValueOptionSerDe::compareDecimal;

        compare(cmp, newDecimal("1"), newDecimal("1"));
        compare(cmp, newDecimal("1.1"), newDecimal("1"));
        compare(cmp, newDecimal("1.10"), newDecimal("1"));
        compare(cmp, newDecimal("1.10"), newDecimal("2"));
        compare(cmp, newDecimal("1"), newDecimal("1.1"));
        compare(cmp, newDecimal("1"), newDecimal("1.10"));
        compare(cmp, newDecimal("2"), newDecimal("1.10"));

        compare(cmp, newDecimal("1"), newDecimal("1"));
        compare(cmp, newDecimal("1"), newDecimal("-1"));
        compare(cmp, newDecimal("-1"), newDecimal("1"));

        compare(cmp, new DecimalOption(), new DecimalOption());
        compare(cmp, newDecimal("1.1"), new DecimalOption());
        compare(cmp, new DecimalOption(), newDecimal("1.1"));
    }

    private <T extends ValueOption<T>> void check(T option) {
        try {
            ValueOption<?> copy = option.getClass().newInstance();
            DataBuffer buffer = serialize(option);
            deserialize(copy, buffer);
            assertThat(buffer.getReadRemaining(), is(0));
            assertThat(copy, equalTo(option));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private DataBuffer serialize(ValueOption<?> option) {
        try {
            DataBuffer buffer = new DataBuffer();
            ValueOptionSerDe.serializeAny(option, buffer);
            buffer.reset(0, buffer.getWritePosition());
            return buffer;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private <T extends ValueOption<?>> T deserialize(T option, DataInput input) {
        try {
            ValueOptionSerDe.deserializeAny(option, input);
            return option;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private DateOption newDate(int v) {
        return new DateOption(new Date(v));
    }

    private DateTimeOption newDateTime(long v) {
        return new DateTimeOption(new DateTime(v));
    }

    private DecimalOption newDecimal(String v) {
        return new DecimalOption(new BigDecimal(v));
    }

    private <T extends ValueOption<T>> void compare(DataComparator cmp, T a, T b) {
        compare0(cmp, a, b);
        compare0(cmp, b, a);
    }

    private <T extends ValueOption<T>> void compare0(DataComparator cmp, T a, T b) throws AssertionError {
        try {
            DataBuffer aBuffer = new DataBuffer();
            DataBuffer bBuffer = new DataBuffer();
            ValueOptionSerDe.serializeAny(a, aBuffer);
            ValueOptionSerDe.serializeAny(b, bBuffer);
            int sign = a.compareTo(b);
            int result = cmp.compare(aBuffer, bBuffer);
            assertThat(aBuffer.getReadRemaining(), is(0));
            assertThat(bBuffer.getReadRemaining(), is(0));
            if (sign == 0) {
                assertThat(result, equalTo(0));
            } else if (sign < 0) {
                assertThat(result, lessThan(0));
            } else {
                assertThat(result, greaterThan(0));
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
