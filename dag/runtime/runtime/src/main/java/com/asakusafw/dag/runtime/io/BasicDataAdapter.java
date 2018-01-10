/**
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
package com.asakusafw.dag.runtime.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.runtime.data.DataAdapter;
import com.asakusafw.runtime.model.DataModel;

/**
 * A basic implementation of {@link DataAdapter}.
 * @param <T> the data type
 * @since 0.4.1
 */
public class BasicDataAdapter<T extends DataModel<T> & Writable> implements DataAdapter<T> {

    private final Supplier<? extends T> supplier;

    /**
     * Creates a new instance.
     * @param supplier the data model object supplier
     */
    public BasicDataAdapter(Supplier<? extends T> supplier) {
        this.supplier = supplier;
    }

    /**
     * Creates a new instance.
     * @param dataType the data model type
     */
    public BasicDataAdapter(Class<? extends T> dataType) {
        this.supplier = () -> {
            try {
                return dataType.newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }

    @Override
    public T create() {
        return supplier.get();
    }

    @Override
    public void copy(T source, T destination) {
        destination.copyFrom(source);
    }

    @Override
    public void write(T source, DataOutput output) throws IOException {
        source.write(output);
    }

    @Override
    public void read(DataInput input, T destination) throws IOException {
        destination.readFields(input);
    }
}
