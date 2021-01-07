/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.internalio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.runtime.adapter.ModelInputTaskInfo;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Io.Initializer;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.stage.temporary.TemporaryFile;
import com.asakusafw.runtime.stage.temporary.TemporaryFileInput;

/**
 * A {@link TaskInfo} for internal input.
 * @param <T> the input data type
 * @since 0.4.0
 */
public class LocalInternalInputTaskInfo<T extends Writable> implements ModelInputTaskInfo<T> {

    private final File file;

    private final int blockOffset;

    private final int blockLength;

    private final Supplier<? extends T> objectFactory;

    /**
     * Creates a new instance.
     * @param file the target file
     * @param blockOffset the block offset
     * @param blockLength the block length
     * @param objectFactory the data model object supplier
     */
    public LocalInternalInputTaskInfo(
            File file, int blockOffset, int blockLength,
            Supplier<? extends T> objectFactory) {
        Arguments.requireNonNull(file);
        Arguments.requireNonNull(objectFactory);
        this.file = file;
        this.blockOffset = blockOffset;
        this.blockLength = blockLength;
        this.objectFactory = objectFactory;
    }

    @Override
    public ModelInput<T> open() throws IOException, InterruptedException {
        return open(file, blockOffset, blockLength);
    }

    /**
     * Opens a model input.
     * @param <T> the input data type
     * @param file the target file
     * @return the opened file
     * @throws IOException if I/O error was occurred while opening the file
     */
    public static <T extends Writable> ModelInput<T> open(File file) throws IOException {
        return open(file, 0, 0);
    }

    /**
     * Opens a model input.
     * @param <T> the input data type
     * @param file the target file
     * @param blockOffset the block offset
     * @param blockLength the block length
     * @return the opened file
     * @throws IOException if I/O error was occurred while opening the file
     */
    public static <T extends Writable> ModelInput<T> open(
            File file,
            int blockOffset, int blockLength) throws IOException {
        try (Initializer<InputStream> init = new Initializer<>(new FileInputStream(file))) {
            if (blockOffset > 0) {
                long rest = (long) blockOffset * TemporaryFile.BLOCK_SIZE;
                while (rest > 0) {
                    long skipped = init.get().skip(rest);
                    if (skipped < 0) {
                        throw new IOException();
                    }
                    rest -= skipped;
                }
            }
            return new TemporaryFileInput<>(init.done(), blockLength);
        }
    }

    @Override
    public T newDataObject() {
        return objectFactory.get();
    }
}
