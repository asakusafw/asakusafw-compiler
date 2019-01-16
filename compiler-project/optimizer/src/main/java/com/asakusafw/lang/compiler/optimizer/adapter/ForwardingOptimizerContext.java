/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.optimizer.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.OptimizerToolkit;

/**
 * Forwarding {@link OptimizerContext}.
 * @since 0.1.0
 * @version 0.3.1
 */
public abstract class ForwardingOptimizerContext implements OptimizerContext {

    private final OptimizerContext delegate;

    /**
     * Creates a new instance.
     * @param delegate the forwarding target
     */
    public ForwardingOptimizerContext(OptimizerContext delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompilerOptions getOptions() {
        return delegate.getOptions();
    }

    @Override
    public String getBatchId() {
        return delegate.getBatchId();
    }

    @Override
    public String getFlowId() {
        return delegate.getFlowId();
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return delegate.getDataModelLoader();
    }

    @Override
    public OptimizerToolkit getToolkit() {
        return delegate.getToolkit();
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        return delegate.addClassFile(aClass);
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return delegate.addResourceFile(location);
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }
}
