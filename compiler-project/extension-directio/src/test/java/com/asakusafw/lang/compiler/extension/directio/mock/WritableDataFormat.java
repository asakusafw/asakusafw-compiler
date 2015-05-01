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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.directio.BinaryStreamFormat;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * {@link BinaryStreamFormat} for {@link WritableModelInput} and {@link WritableModelOutput}.
 * @param <T> the target data type
 */
public abstract class WritableDataFormat<T extends Writable> extends BinaryStreamFormat<T> {

    @Override
    public long getPreferredFragmentSize() throws IOException, InterruptedException {
        return -1;
    }

    @Override
    public long getMinimumFragmentSize() throws IOException, InterruptedException {
        return -1;
    }

    @Override
    public ModelInput<T> createInput(
            Class<? extends T> dataType, String path, InputStream stream,
            long offset, long fragmentSize) {
        return new WritableModelInput<>(new DataInputStream(stream));
    }

    @Override
    public ModelOutput<T> createOutput(Class<? extends T> dataType, String path, OutputStream stream) {
        return new WritableModelOutput<>(new DataOutputStream(stream));
    }
}
