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
package com.asakusafw.dag.runtime.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.dag.runtime.directio.OutputPatternSerDe.Format;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.ValueOption;

/**
 * Test for {@link OutputPatternSerDe}.
 */
public class OutputPatternSerDeTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        OutputPatternSerDe sd = serde()
                .text("hello");

        MockData data = new MockData().set(100, "Hello, world!");
        byte[] key = dump(o -> sd.serializeKey(data, o));
        byte[] value = dump(o -> sd.serializeValue(data, o));

        assertThat(sd.deserializeKey(data(key)), is("hello"));
        assertThat(sd.deserializePair(data(key), data(value)), has(100, "Hello, world!"));
    }

    /**
     * use random numbers.
     * @throws Exception if failed
     */
    @Test
    public void random() throws Exception {
        OutputPatternSerDe sd = serde()
                .text("r-")
                .random(1, 9);

        MockData data = new MockData().set(100, "Hello, world!");
        byte[] key = dump(o -> sd.serializeKey(data, o));

        Object restored = sd.deserializeKey(data(key));
        assertThat(restored, regex("r-[1-9]"));
        assertThat(sd.deserializeKey(data(key)), is(restored));
        assertThat(sd.deserializeKey(data(key)), is(restored));
    }

    /**
     * use property - natural.
     * @throws Exception if failed
     */
    @Test
    public void property_natural() throws Exception {
        OutputPatternSerDe sd = serde(d -> d.getStringValueOption())
                .text("p-")
                .property(Format.NATURAL, null);

        MockData data = new MockData().set(100, "v");
        byte[] key = dump(o -> sd.serializeKey(data, o));

        Object restored = sd.deserializeKey(data(key));
        assertThat(restored, is("p-v"));
    }

    /**
     * use property - date.
     * @throws Exception if failed
     */
    @SuppressWarnings("deprecation")
    @Test
    public void property_date() throws Exception {
        OutputPatternSerDe sd = serde(d -> d.getDateValueOption())
                .text("p-")
                .property(Format.DATE, "yyyyMMdd");

        MockData data = new MockData();
        data.getDateValueOption().modify(new Date(2000, 1, 2));
        byte[] key = dump(o -> sd.serializeKey(data, o));

        Object restored = sd.deserializeKey(data(key));
        assertThat(restored, is("p-20000102"));
    }

    /**
     * use property - date time.
     * @throws Exception if failed
     */
    @SuppressWarnings("deprecation")
    @Test
    public void property_datetime() throws Exception {
        OutputPatternSerDe sd = serde(d -> d.getDatetimeValueOption())
                .text("p-")
                .property(Format.DATETIME, "yyyyMMdd");

        MockData data = new MockData();
        data.getDatetimeValueOption().modify(new DateTime(2000, 1, 2, 3, 4, 5));
        byte[] key = dump(o -> sd.serializeKey(data, o));

        Object restored = sd.deserializeKey(data(key));
        assertThat(restored, is("p-20000102"));
    }

    private static Matcher<Object> has(int key, String value) {
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                MockData data = (MockData) item;
                return data.getKey() == key && Objects.equals(data.getValue(), value);
            }
            @Override
            public void describeTo(Description description) {
                description.appendValue(new Tuple<>(key, value));
            }
        };
    }

    private static Matcher<Object> regex(String pattern) {
        Pattern p = Pattern.compile(pattern);
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                return p.matcher((CharSequence) item).matches();
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("matches ").appendValue(p);
            }
        };
    }

    private byte[] dump(Action<DataOutput, Exception> action) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        Lang.safe(() -> action.perform(new DataOutputStream(buffer)));
        return buffer.toByteArray();
    }

    private DataInput data(byte... values) {
        return new DataInputStream(new ByteArrayInputStream(values));
    }

    @SafeVarargs
    private static OutputPatternSerDe serde(Function<MockData, ValueOption<?>>... extractors) {
        return new OutputPatternSerDe() {
            @Override
            protected Object getProperty(Object object, int index) {
                MockData d = (MockData) object;
                return extractors[index].apply(d);
            }
            @Override
            public void serializeValue(Object object, DataOutput output) throws IOException {
                ((MockData) object).write(output);
            }
            @Override
            public Object deserializePair(DataInput keyInput, DataInput valueInput) throws IOException {
                MockData d = new MockData();
                d.readFields(valueInput);
                return d;
            }
        };
    }
}
