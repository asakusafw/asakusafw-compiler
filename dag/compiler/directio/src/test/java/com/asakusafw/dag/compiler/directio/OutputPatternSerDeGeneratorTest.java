/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateTime;

/**
 * Test for {@link OutputPatternSerDeGenerator}.
 */
public class OutputPatternSerDeGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        serde(sd -> {
            MockData data = new MockData().set(100, "Hello, world!");
            byte[] key = dump(o -> sd.serializeKey(data, o));
            byte[] value = dump(o -> sd.serializeValue(data, o));

            assertThat(sd.deserializeKey(data(key)), is("hello"));
            assertThat(sd.deserializePair(data(key), data(value)), has(100, "Hello, world!"));
        }, "hello");
    }

    /**
     * use random numbers.
     * @throws Exception if failed
     */
    @Test
    public void random() throws Exception {
        serde(sd -> {
            MockData data = new MockData().set(100, "Hello, world!");
            byte[] key = dump(o -> sd.serializeKey(data, o));

            Object restored = sd.deserializeKey(data(key));
            assertThat(restored, regex("r-[1-9]"));
            assertThat(sd.deserializeKey(data(key)), is(restored));
            assertThat(sd.deserializeKey(data(key)), is(restored));
        }, "r-[1..9]");
    }

    /**
     * use property - natural.
     * @throws Exception if failed
     */
    @Test
    public void property_natural() throws Exception {
        serde(sd -> {
            MockData data = new MockData().set(100, "v");
            byte[] key = dump(o -> sd.serializeKey(data, o));

            Object restored = sd.deserializeKey(data(key));
            assertThat(restored, is("p-v"));
        }, "p-{string_value}");
    }

    /**
     * use property - int.
     * @throws Exception if failed
     */
    @SuppressWarnings("deprecation")
    @Test
    public void property_int() throws Exception {
        serde(sd -> {
            MockData data = new MockData();
            data.getIntValueOption().modify(1);
            byte[] key = dump(o -> sd.serializeKey(data, o));

            Object restored = sd.deserializeKey(data(key));
            assertThat(restored, is("p-0001"));
        }, "p-{int_value:0000}");
    }

    /**
     * use property - date.
     * @throws Exception if failed
     */
    @SuppressWarnings("deprecation")
    @Test
    public void property_date() throws Exception {
        serde(sd -> {
            MockData data = new MockData();
            data.getDateValueOption().modify(new Date(2000, 1, 2));
            byte[] key = dump(o -> sd.serializeKey(data, o));

            Object restored = sd.deserializeKey(data(key));
            assertThat(restored, is("p-20000102"));
        }, "p-{date_value:yyyyMMdd}");
    }

    /**
     * use property - date time.
     * @throws Exception if failed
     */
    @SuppressWarnings("deprecation")
    @Test
    public void property_datetime() throws Exception {
        serde(sd -> {
            MockData data = new MockData();
            data.getDatetimeValueOption().modify(new DateTime(2000, 1, 2, 3, 4, 5));
            byte[] key = dump(o -> sd.serializeKey(data, o));

            Object restored = sd.deserializeKey(data(key));
            assertThat(restored, is("p-20000102"));
        }, "p-{datetime_value:yyyyMMdd}");
    }

    /**
     * orders.
     * @throws Exception if failed
     */
    @SuppressWarnings("deprecation")
    @Test
    public void order() throws Exception {
        serde(sd -> {
            MockData data = new MockData();
            data.getIntValueOption().modify(1);
            data.getDateValueOption().modify(2);
            data.getDatetimeValueOption().modify(3);
            data.getStringValueOption().modify("Hello, world!");

            byte[] key = dump(o -> sd.serializeKey(data, o));
            byte[] value = dump(o -> sd.serializeValue(data, o));
            MockData restored = (MockData) sd.deserializePair(data(key), data(value));

            assertThat(restored.getIntValueOption(), is(data.getIntValueOption()));
            assertThat(restored.getDateValueOption(), is(data.getDateValueOption()));
            assertThat(restored.getDatetimeValueOption(), is(data.getDatetimeValueOption()));
            assertThat(restored.getStringValueOption(), is(data.getStringValueOption()));
        }, "hello", "date_value", "int_value");
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

    private void serde(Action<KeyValueSerDe, Exception> action, String pattern, String... order) {
        ClassGeneratorContext gc = context();
        DataModelReference ref = gc.getDataModelLoader().load(Descriptions.typeOf(MockData.class));
        OutputPattern rp = OutputPattern.compile(ref, pattern, Arrays.asList(order));
        ClassDescription gen = add(c -> new OutputPatternSerDeGenerator().generate(ref, rp, c));
        loading(gen, c -> {
            KeyValueSerDe sd = (KeyValueSerDe) c.newInstance();
            action.perform(sd);
        });
    }
}
