/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.StringOption;

/**
 * Mock data model class.
 */
@SuppressWarnings({ "deprecation", "javadoc" })
public class MockData implements DataModel<MockData>, Writable {

    private final IntOption intValue = new IntOption();

    private final StringOption stringValue = new StringOption();

    private final DateOption dateValue = new DateOption();

    private final DateTimeOption datetimeValue = new DateTimeOption();

    public static void put(ModelOutput<MockData> output, Map<Integer, String> entries) throws IOException {
        MockData buf = new MockData();
        for (Map.Entry<Integer, String> entry : entries.entrySet()) {
            output.write(buf.set(entry.getKey(), entry.getValue()));
        }
    }

    public static Map<Integer, String> collect(ModelInput<MockData> input) throws IOException {
        MockData buf = new MockData();
        Map<Integer, String> results = new LinkedHashMap<>();
        while (input.readTo(buf)) {
            results.put(buf.getKey(), buf.getValue());
        }
        return results;
    }

    public MockData set(int key, String value) {
        intValue.modify(key);
        stringValue.modify(value);
        return this;
    }

    public int getKey() {
        return intValue.get();
    }

    public String getValue() {
        return stringValue.getAsString();
    }

    public IntOption getIntValueOption() {
        return intValue;
    }

    public StringOption getStringValueOption() {
        return stringValue;
    }

    public DateOption getDateValueOption() {
        return dateValue;
    }

    public DateTimeOption getDatetimeValueOption() {
        return datetimeValue;
    }

    @Override
    public void reset() {
        intValue.setNull();
        stringValue.setNull();
        dateValue.setNull();
        datetimeValue.setNull();
    }

    @Override
    public void copyFrom(MockData other) {
        intValue.copyFrom(other.intValue);
        stringValue.copyFrom(other.stringValue);
        dateValue.copyFrom(other.dateValue);
        datetimeValue.copyFrom(other.datetimeValue);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        intValue.write(out);
        stringValue.write(out);
        dateValue.write(out);
        datetimeValue.write(out);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        intValue.readFields(input);
        stringValue.readFields(input);
        dateValue.readFields(input);
        datetimeValue.readFields(input);
    }
}
