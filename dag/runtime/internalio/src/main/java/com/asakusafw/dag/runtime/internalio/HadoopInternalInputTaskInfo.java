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

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class HadoopInternalInputTaskInfo<T extends Writable> implements ModelInputTaskInfo<T> {

    private final FileSystem fileSystem;

    private final Path file;

    private final int blockOffset;

    private final int blockLength;

    private final Supplier<? extends T> objectFactory;

    /**
     * Creates a new instance.
     * @param fileSystem the Hadoop file system
     * @param file the target file
     * @param blockOffset the block offset
     * @param blockLength the block length
     * @param objectFactory the data model object supplier
     */
    public HadoopInternalInputTaskInfo(
            FileSystem fileSystem, Path file,
            int blockOffset, int blockLength,
            Supplier<? extends T> objectFactory) {
        Arguments.requireNonNull(fileSystem);
        Arguments.requireNonNull(file);
        Arguments.requireNonNull(objectFactory);
        this.fileSystem = fileSystem;
        this.file = file;
        this.blockOffset = blockOffset;
        this.blockLength = blockLength;
        this.objectFactory = objectFactory;
    }

    @Override
    public ModelInput<T> open() throws IOException, InterruptedException {
        return open(fileSystem, file, blockOffset, blockLength);
    }

    /**
     * Opens a model input.
     * @param <T> the input data type
     * @param fileSystem the Hadoop file system
     * @param file the target file
     * @return the opened file
     * @throws IOException if I/O error was occurred while opening the file
     */
    public static <T extends Writable> ModelInput<T> open(FileSystem fileSystem, Path file) throws IOException {
        return open(fileSystem, file, 0, 0);
    }

    /**
     * Opens a model input.
     * @param <T> the input data type
     * @param fileSystem the Hadoop file system
     * @param file the target file
     * @param blockOffset the block offset
     * @param blockLength the block length
     * @return the opened file
     * @throws IOException if I/O error was occurred while opening the file
     */
    public static <T extends Writable> ModelInput<T> open(
            FileSystem fileSystem, Path file,
            int blockOffset, int blockLength) throws IOException {
        try (Initializer<FSDataInputStream> init = new Initializer<>(fileSystem.open(file))) {
            if (blockOffset > 0) {
                init.get().seek((long) blockOffset * TemporaryFile.BLOCK_SIZE);
            }
            return new TemporaryFileInput<>(init.done(), blockLength);
        }
    }

    @Override
    public T newDataObject() {
        return objectFactory.get();
    }
}
