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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An {@link InputHandler} for {@link ExtractOperation}.
 * @since 0.4.0
 */
final class ExtractInputHandler implements InputHandler<ExtractOperation.Input, EdgeIoProcessorContext> {

    private final String name;

    /**
     * Creates a new instance.
     * @param name the input name
     */
    ExtractInputHandler(String name) {
        Arguments.requireNonNull(name);
        this.name = name;
    }

    @Override
    public InputSession<ExtractOperation.Input> start(
            EdgeIoProcessorContext context) throws IOException, InterruptedException {
        return new ExtractDriver(context, name);
    }

    private static final class ExtractDriver
            implements InputHandler.InputSession<ExtractOperation.Input>, ExtractOperation.Input {

        private final ObjectReader reader;

        private Object next;

        ExtractDriver(EdgeIoProcessorContext context, String name) throws IOException, InterruptedException {
            Arguments.requireNonNull(context);
            Arguments.requireNonNull(name);
            this.reader = (ObjectReader) context.getInput(name);
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            if (reader.nextObject()) {
                this.next = reader.getObject();
                return true;
            }
            return false;
        }

        @Override
        public ExtractOperation.Input get() throws IOException, InterruptedException {
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject() {
            return (T) next;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            reader.close();
        }
    }
}
