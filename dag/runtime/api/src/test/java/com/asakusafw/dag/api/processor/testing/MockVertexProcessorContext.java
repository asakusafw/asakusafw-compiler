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
package com.asakusafw.dag.api.processor.testing;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.AbstractEdgeIoProcessorContext;

/**
 * Mock implementation of {@link VertexProcessorContext}.
 */
public class MockVertexProcessorContext extends AbstractEdgeIoProcessorContext<MockVertexProcessorContext>
        implements VertexProcessorContext {

    private String vertexId = "mock";

    private ClassLoader classLoader = getClass().getClassLoader();

    @Override
    protected MockVertexProcessorContext self() {
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

    /**
     * Sets the vertex ID.
     * @param id the vertex ID
     * @return this
     */
    public MockVertexProcessorContext withId(String id) {
        this.vertexId = id;
        return this;
    }

    /**
     * Sets the class loader.
     * @param loader the class loader
     * @return this
     */
    public MockVertexProcessorContext with(ClassLoader loader) {
        this.classLoader = loader;
        return this;
    }

    /**
     * Sets the class loader from the defining class.
     * @param defining the defining class
     * @return this
     */
    public MockVertexProcessorContext with(Class<?> defining) {
        return with(defining.getClassLoader());
    }
}
