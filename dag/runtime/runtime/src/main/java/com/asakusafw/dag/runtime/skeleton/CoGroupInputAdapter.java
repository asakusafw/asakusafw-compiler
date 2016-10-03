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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.flow.ArrayListBuffer;
import com.asakusafw.runtime.flow.FileMapListBuffer;
import com.asakusafw.runtime.flow.ListBuffer;
import com.asakusafw.runtime.model.DataModel;

/**
 * {@link InputAdapter} for co-group edge inputs.
 * @since 0.4.0
 */
public class CoGroupInputAdapter implements InputAdapter<CoGroupOperation.Input> {

    /**
     * The configuration key of the on-heap window size for file mapped inputs
     * (the number of entries, per input*thread).
     * @see BufferType#FILE
     */
    public static final String KEY_FILE_WINDOW_SIZE =
            "com.asakusafw.dag.input.file.window.size"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_FILE_WINDOW_SIZE}.
     */
    public static final int DEFAULT_FILE_WINDOW_SIZE = 256;

    private final List<Consumer<CoGroupInputHandler.Builder>> actions = new ArrayList<>();

    private final int fileWindowSize;

    /**
     * Creates a new instance.
     * @param context the context
     */
    public CoGroupInputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        this.fileWindowSize = Util.getProperty(
                context,
                "window size",
                KEY_FILE_WINDOW_SIZE, DEFAULT_FILE_WINDOW_SIZE);
    }

    /**
     * Bind the input.
     * @param name the input name
     * @param supplierClass the supplier class
     * @return this
     */
    public final CoGroupInputAdapter bind(String name, Class<?> supplierClass) {
        Arguments.requireNonNull(name);
        Arguments.requireNonNull(supplierClass);
        return bind(name, supplierClass, BufferType.getDefault());
    }

    /**
     * Bind the input.
     * @param name the input name
     * @param supplierClass the supplier class
     * @param bufferType the buffer type
     * @return this
     */
    public final CoGroupInputAdapter bind(String name, Class<?> supplierClass, BufferType bufferType) {
        Arguments.requireNonNull(name);
        Arguments.requireNonNull(supplierClass);
        Arguments.requireNonNull(bufferType);
        bind0(name, supplierClass, bufferType);
        return this;
    }

    @SuppressWarnings("unchecked")
    private <T extends DataModel<T> & Writable> void bind0(
            String name, Class<?> supplierClass, BufferType bufferType) {
        Supplier<? extends T> objects = Invariants.safe(() -> (Supplier<? extends T>) supplierClass.newInstance());
        actions.add(b -> {
            ListBuffer<T> buffer = newBuffer(bufferType);
            b.addInput(name, objects, buffer);
        });
    }

    private <T extends Writable> ListBuffer<T> newBuffer(BufferType bufferType) {
        switch (bufferType) {
        case HEAP:
            return new ArrayListBuffer<>();
        case FILE:
            if (fileWindowSize <= 0) {
                return new ArrayListBuffer<>();
            } else {
                return new FileMapListBuffer<>();
            }
        default:
            throw new AssertionError(bufferType);
        }
    }

    @Override
    public final InputHandler<CoGroupOperation.Input, ? super EdgeIoProcessorContext> newHandler()
            throws IOException, InterruptedException {
        CoGroupInputHandler.Builder builder = CoGroupInputHandler.builder();
        actions.forEach(a -> a.accept(builder));
        return builder.build();
    }

    /**
     * Represents a buffer type.
     * @since 0.4.0
     */
    public enum BufferType {

        /**
         * Uses on-heap buffer.
         */
        HEAP,

        /**
         * Uses buffer with file backing store.
         */
        FILE,
        ;

        /**
         * Returns the default value.
         * @return the default value
         */
        public static BufferType getDefault() {
            return HEAP;
        }
    }
}
