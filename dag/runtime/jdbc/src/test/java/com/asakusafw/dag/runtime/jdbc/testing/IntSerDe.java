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
package com.asakusafw.dag.runtime.jdbc.testing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.asakusafw.dag.api.common.ValueSerDe;

/**
 * ser/de {@code int} values.
 */
public class IntSerDe implements ValueSerDe {

    private final int saveDelta;

    private final int loadDelta;

    /**
     * Creates a new instance.
     */
    public IntSerDe() {
        this(0);
    }

    /**
     * Creates a new instance.
     * @param delta the serializing delta
     */
    public IntSerDe(int delta) {
        this(delta, -delta);
    }

    /**
     * Creates a new instance.
     * @param saveDelta delta on saving object
     * @param loadDelta delta on loading object
     */
    public IntSerDe(int saveDelta, int loadDelta) {
        this.saveDelta = saveDelta;
        this.loadDelta = loadDelta;
    }

    @Override
    public void serialize(Object object, DataOutput output) throws IOException, InterruptedException {
        output.writeInt((Integer) object + saveDelta);
    }

    @Override
    public Object deserialize(DataInput input) throws IOException, InterruptedException {
        return input.readInt() + loadDelta;
    }
}
