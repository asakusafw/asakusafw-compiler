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
package com.asakusafw.dag.api.processor.testing;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.basic.AbstractEdgeIoProcessorContext;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Mock implementation of {@link TaskProcessorContext}.
 */
public class MockTaskProcessorContext extends AbstractEdgeIoProcessorContext<MockTaskProcessorContext>
        implements TaskProcessorContext {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private String vertexId = "mock";

    private final String taskId;

    private final TaskInfo info;

    private ClassLoader classLoader = getClass().getClassLoader();


    /**
     * Creates a new instance.
     */
    public MockTaskProcessorContext() {
        this(null, null);
    }

    /**
     * Creates a new instance.
     * @param id the task ID
     */
    public MockTaskProcessorContext(String id) {
        this(id, null);
    }

    /**
     * Creates a new instance.
     * @param info the custom task information
     */
    public MockTaskProcessorContext(TaskInfo info) {
        this(null, info);
    }

    /**
     * Creates a new instance.
     * @param id the task ID
     * @param info the custom task information
     */
    public MockTaskProcessorContext(String id, TaskInfo info) {
        this.taskId = Optionals.of(id).orElseGet(() -> String.format("id-%d", COUNTER.incrementAndGet()));
        this.info = info;
    }

    /**
     * Sets the vertex ID.
     * @param id the vertex ID
     * @return this
     */
    public MockTaskProcessorContext withVertexId(String id) {
        this.vertexId = id;
        return this;
    }

    /**
     * Sets the class loader.
     * @param cl the class loader
     * @return this
     */
    public MockTaskProcessorContext with(ClassLoader cl) {
        this.classLoader = cl;
        return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public String getVertexId() {
        return vertexId;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public Optional<TaskInfo> getTaskInfo() {
        return Optionals.of(info);
    }

    @Override
    protected MockTaskProcessorContext self() {
        return this;
    }
}
