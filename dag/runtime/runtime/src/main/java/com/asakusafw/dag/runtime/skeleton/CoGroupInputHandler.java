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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.basic.CoGroupReader;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation.Cursor;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.runtime.flow.ListBuffer;
import com.asakusafw.runtime.model.DataModel;

/**
 * An {@link InputHandler} for {@link CoGroupOperation}.
 * @since 0.4.0
 */
final class CoGroupInputHandler implements InputHandler<CoGroupOperation.Input, EdgeIoProcessorContext> {

    private final Input<?>[] inputs;

    CoGroupInputHandler(List<Input<?>> inputs) {
        Invariants.require(inputs.isEmpty() == false, "CoGroup-like operation must have at least one input");
        this.inputs = inputs.toArray(new Input<?>[inputs.size()]);
    }

    /**
     * Creates a new builder.
     * @return the created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public InputSession<CoGroupOperation.Input> start(
            EdgeIoProcessorContext context) throws IOException, InterruptedException {
        assert inputs.length != 0;
        if (inputs.length == 1) {
            Input<?> input = inputs[0];
            return new Single(input.build(context), input);
        } else {
            CoGroupReader reader;
            List<GroupReader> groups = new ArrayList<>();
            try {
                for (Input<?> in : inputs) {
                    groups.add(in.build(context));
                }
                reader = new CoGroupReader(groups);
                groups.clear();
            } finally {
                for (GroupReader r : groups) {
                    r.close();
                }
            }
            return new Multiple(reader, inputs);
        }
    }

    /**
     * Returns the number of groups.
     * @return the number of groups
     */
    public int getGroupCount() {
        return inputs.length;
    }

    /**
     * A builder for {@link CoGroupInputHandler}.
     * @since 0.4.0
     */
    public static final class Builder {

        private final Map<String, Input<?>> inputs = new LinkedHashMap<>();

        /**
         * Adds an input.
         * @param <T> the data type
         * @param name the input name
         * @param supplier the data model object supplier
         * @param buffer the group buffer
         * @return this
         */
        public <T extends DataModel<T>> Builder addInput(
                String name,
                Supplier<? extends T> supplier, ListBuffer<T> buffer) {
            Arguments.requireNonNull(name);
            Arguments.requireNonNull(supplier);
            Arguments.requireNonNull(buffer);
            Arguments.require(inputs.containsKey(name) == false, MessageFormat.format(
                    "input \"{0}\" is already registered", //$NON-NLS-1$
                    name));
            Input<T> input = new Input<>(name, supplier, buffer);
            inputs.put(name, input);
            return this;
        }

        /**
         * Builds a {@link CoGroupInputHandler} from added inputs.
         * @return the created driver
         * @throws IOException if I/O error was occurred while building driver
         * @throws InterruptedException if interrupted while building driver
         */
        public CoGroupInputHandler build() throws IOException, InterruptedException {
            Invariants.require(inputs.isEmpty() == false, "CoGroup-like operation must have at least one input");
            return new CoGroupInputHandler(new ArrayList<>(inputs.values()));
        }
    }

    private static final class Input<T extends DataModel<T>> {

        final String name;

        final Supplier<? extends T> objects;

        private final Wrapper<T> wrapper;

        private final ListBuffer<T> buffer;

        Input(String name, Supplier<? extends T> objects, ListBuffer<T> buffer) {
            this.name = name;
            this.objects = objects;
            this.wrapper = new Wrapper<>();
            this.buffer = buffer;
        }

        GroupReader build(EdgeIoProcessorContext context) throws IOException, InterruptedException {
            return (GroupReader) context.getInput(name);
        }

        <S> Wrapper<S> wrap(ObjectCursor group) {
            return wrapper.wrap(group);
        }

        @SuppressWarnings("unchecked")
        <S> ListBuffer<S> fill(ObjectCursor cursor) throws IOException, InterruptedException {
            ListBuffer<T> buf = buffer;
            Supplier<? extends T> sup = objects;
            buf.shrink();
            buf.begin();
            while (cursor.nextObject()) {
                T object = (T) cursor.getObject();
                if (buf.isExpandRequired()) {
                    buf.expand(sup.get());
                }
                buf.advance().copyFrom(object);
            }
            buf.end();
            return (ListBuffer<S>) buf;
        }

        void close() {
            buffer.shrink();
        }
    }

    private static final class Single
            implements InputSession<CoGroupOperation.Input>, CoGroupOperation.Input {

        private final GroupReader reader;

        private final Input<?> input;

        Single(GroupReader reader, Input<?> input) {
            this.reader = reader;
            this.input = input;
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            if (reader.nextGroup() == false) {
                return false;
            }
            return true;
        }

        @Override
        public CoGroupOperation.Input get() throws IOException, InterruptedException {
            return this;
        }

        @Override
        public <T> Cursor<T> getCursor(int index) throws IOException, InterruptedException {
            return input.wrap(reader);
        }

        @Override
        public <T> ListBuffer<T> getList(int index) throws IOException, InterruptedException {
            return input.fill(reader);
        }

        @Override
        public void close() throws IOException, InterruptedException {
            reader.close();
            input.close();
        }
    }

    private static final class Multiple
            implements InputSession<CoGroupOperation.Input>, CoGroupOperation.Input {

        private final CoGroupReader reader;

        private final Input<?>[] inputs;

        Multiple(CoGroupReader reader, Input<?>[] inputs) {
            this.reader = reader;
            this.inputs = inputs;
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            if (reader.nextCoGroup() == false) {
                return false;
            }
            return true;
        }

        @Override
        public CoGroupOperation.Input get() throws IOException, InterruptedException {
            return this;
        }

        @Override
        public <T> Cursor<T> getCursor(int index) throws IOException, InterruptedException {
            return inputs[index].wrap(reader.getGroup(index));
        }

        @Override
        public <T> List<T> getList(int index) throws IOException, InterruptedException {
            return inputs[index].fill(reader.getGroup(index));
        }

        @Override
        public void close() throws IOException, InterruptedException {
            reader.close();
            for (Input<?> input : inputs) {
                input.close();
            }
        }
    }

    private static final class Wrapper<T> implements CoGroupOperation.Cursor<T> {

        private ObjectCursor cursor;

        Wrapper() {
            return;
        }

        @SuppressWarnings("unchecked")
        <S> Wrapper<S> wrap(ObjectCursor newCursor) {
            this.cursor = newCursor;
            return (Wrapper<S>) this;
        }

        @Override
        public boolean nextObject() throws IOException, InterruptedException {
            return cursor.nextObject();
        }

        @SuppressWarnings("unchecked")
        @Override
        public T getObject() throws IOException, InterruptedException {
            return (T) cursor.getObject();
        }
    }
}
