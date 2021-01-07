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
package com.asakusafw.vanilla.testkit.inprocess.testing;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.model.basic.BasicVertexDescriptor;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskInfo;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * A simple application for testing.
 */
public class Callback implements Supplier<GraphInfo> {

    /**
     * the callback.
     */
    public static Consumer<ProcessorContext> action;

    @Override
    public GraphInfo get() {
        GraphInfo graph = new GraphInfo();
        graph.addVertex("v", new BasicVertexDescriptor(SupplierInfo.of(Proc.class.getName())));
        return graph;
    }

    /**
     * Performs {@link Callback#action}.
     */
    public static class Proc implements VertexProcessor {
        @Override
        public Optional<? extends TaskSchedule> initialize(VertexProcessorContext context) {
            assertThat(action, is(notNullValue()));
            BasicTaskInfo task = new BasicTaskInfo();
            return Optionals.of(new BasicTaskSchedule(task));
        }
        @Override
        public TaskProcessor createTaskProcessor() {
            return c -> action.accept(c);
        }
    }
}
