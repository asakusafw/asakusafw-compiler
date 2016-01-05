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
package com.asakusafw.lang.compiler.extension.testdriver.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.StringOption;

@SuppressWarnings({ "deprecation", "javadoc" })
public class MockData implements DataModel<MockData>, Writable {

    private final IntOption key = new IntOption();

    private final StringOption value = new StringOption();

    public MockData() {
        return;
    }

    public MockData(int key, String value) {
        this.key.modify(key);
        this.value.modify(value);
    }

    public IntOption getKeyOption() {
        return key;
    }

    public StringOption getValueOption() {
        return value;
    }

    @Override
    public void reset() {
        key.setNull();
        value.setNull();
    }

    @Override
    public void copyFrom(MockData other) {
        key.copyFrom(other.key);
        value.copyFrom(other.value);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        key.readFields(input);
        value.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        key.write(output);
        value.write(output);
    }
}
