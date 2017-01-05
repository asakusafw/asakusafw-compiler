/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
import java.util.Collections;
import java.util.Optional;

import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * A {@link VertexProcessor} which has no inputs, outputs, and operations.
 * @since 0.4.0
 */
public class VoidVertexProcessor implements VertexProcessor {

    @Override
    public Optional<? extends TaskSchedule> initialize(VertexProcessorContext context) {
        return Optionals.of(new BasicTaskSchedule(Collections.emptyList()));
    }

    @Override
    public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        return VoidTaskProcessor.INSTANCE;
    }

    @Override
    public String toString() {
        return "no-operations"; //$NON-NLS-1$
    }
}
