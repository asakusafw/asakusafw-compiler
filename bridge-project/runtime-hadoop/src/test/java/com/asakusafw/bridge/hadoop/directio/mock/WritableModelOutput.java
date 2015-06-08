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
package com.asakusafw.bridge.hadoop.directio.mock;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.io.ModelOutput;

/**
 * A {@link ModelOutput} for Hadoop writable objects.
 * @param <T> the data model type
 */
public class WritableModelOutput<T extends Writable> implements ModelOutput<T> {

    private final DataOutput output;

    private boolean first = true;

    private boolean closed = false;

    /**
     * Creates a new instance.
     * @param output the output
     */
    public WritableModelOutput(DataOutput output) {
        this.output = output;
    }

    /**
     * Creates a new instance.
     * @param <T> the data model type
     * @param file the target file
     * @return the created instance
     * @throws IOException if failed to open the file
     */
    public static <T extends Writable> WritableModelOutput<T> create(File file) throws IOException {
        return new WritableModelOutput<>(new DataOutputStream(FileEditor.create(file)));
    }

    @Override
    public void write(T model) throws IOException {
        if (first) {
            output.writeBoolean(true);
            Text.writeString(output, model.getClass().getName());
            first = false;
        }
        output.writeBoolean(true);
        model.write(output);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        output.writeBoolean(false);
        if (output instanceof Closeable) {
            ((Closeable) output).close();
        }
    }
}
