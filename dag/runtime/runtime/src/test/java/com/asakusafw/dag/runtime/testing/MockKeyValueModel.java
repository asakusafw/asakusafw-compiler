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
package com.asakusafw.dag.runtime.testing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.StringOption;

@SuppressWarnings({ "deprecation", "javadoc" })
public final class MockKeyValueModel implements DataModel<MockKeyValueModel>, Writable {

    private final IntOption key = new IntOption();

    private final StringOption value = new StringOption();

    public MockKeyValueModel() {
        return;
    }

    public MockKeyValueModel(MockKeyValueModel data) {
        copyFrom(data);
    }

    public MockKeyValueModel(String value) {
        this.value.modify(value);
    }

    public MockKeyValueModel(int key, String value) {
        this.key.modify(key);
        this.value.modify(value);
    }

    public int getKey() {
        return key.get();
    }

    public String getValue() {
        return value.getAsString();
    }

    public void setKey(int v) {
        key.modify(v);
    }

    public void setValue(String v) {
        value.modify(v);
    }

    public IntOption getKeyOption() {
        return key;
    }

    public StringOption getValueOption() {
        return value;
    }

    @Override
    public void copyFrom(MockKeyValueModel other) {
        key.copyFrom(other.key);
        value.copyFrom(other.value);
    }

    @Override
    public void reset() {
        key.setNull();
        value.setNull();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + key.hashCode();
        result = prime * result + value.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MockKeyValueModel other = (MockKeyValueModel) obj;
        if (!key.equals(other.key)) {
            return false;
        }
        if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }
    @Override
    public String toString() {
        return String.format("{key=%s, value=%s}", key, value);
    }
}
