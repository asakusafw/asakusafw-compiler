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

import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.ModelInputTaskInfo;
import com.asakusafw.runtime.io.ModelInput;

/**
 * {@link InputHandler} which handles {@link ModelInputTaskInfo}.
 * @since 0.4.0
 */
public class ModelInputHandler implements InputHandler<ExtractOperation.Input, TaskProcessorContext> {

    @Override
    public InputSession<ExtractOperation.Input> start(
            TaskProcessorContext context) throws IOException, InterruptedException {
        ModelInputTaskInfo<?> info = context.getTaskInfo()
                .map(ModelInputTaskInfo.class::cast)
                .orElseThrow(IllegalStateException::new);
        return newDriver(info);
    }

    private static <T> Driver<T> newDriver(ModelInputTaskInfo<T> info) throws IOException, InterruptedException {
        T buffer = info.newDataObject();
        ModelInput<T> input = info.open();
        return new Driver<>(input, buffer);
    }

    private static final class Driver<T>
            implements InputSession<ExtractOperation.Input>, ExtractOperation.Input {

        private final T buffer;

        private final ModelInput<T> input;

        Driver(ModelInput<T> input, T buffer) {
            this.input = input;
            this.buffer = buffer;
        }

        @Override
        public ExtractOperation.Input get() throws IOException, InterruptedException {
            return this;
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            return input.readTo(buffer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <S> S getObject() {
            return (S) buffer;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            input.close();
        }
    }
}
