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
package com.asakusafw.dag.api.processor.basic;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A basic implementation of {@link ProcessorContext}.
 * @since 0.4.0
 */
public class BasicProcessorContext extends AbstractProcessorContext<BasicProcessorContext> {

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param classLoader the class loader
     */
    public BasicProcessorContext(ClassLoader classLoader) {
        Arguments.requireNonNull(classLoader);
        this.classLoader = classLoader;
    }

    @Override
    protected BasicProcessorContext self() {
        return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
