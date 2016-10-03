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
import com.asakusafw.runtime.value.StringOption;

@SuppressWarnings({ "deprecation", "javadoc" })
public final class MockValueModel implements DataModel<MockValueModel>, Writable {

    private final StringOption value = new StringOption();

    public MockValueModel() {
        return;
    }

    public MockValueModel(MockValueModel data) {
        copyFrom(data);
    }

    public MockValueModel(String value) {
        this.value.modify(value);
    }

    public String getValue() {
        return value.getAsString();
    }

    public void setValue(String v) {
        value.modify(v);
    }

    public StringOption getValueOption() {
        return value;
    }

    @Override
    public void copyFrom(MockValueModel other) {
        value.copyFrom(other.value);
    }

    @Override
    public void reset() {
        value.setNull();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
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
        MockValueModel other = (MockValueModel) obj;
        if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }
    @Override
    public String toString() {
        return String.format("{value=%s}", value);
    }
}
