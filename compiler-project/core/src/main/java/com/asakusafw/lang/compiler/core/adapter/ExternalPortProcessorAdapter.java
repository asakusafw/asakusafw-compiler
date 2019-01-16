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
package com.asakusafw.lang.compiler.core.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * An adapter for {@link ExternalPortProcessor}.
 */
public class ExternalPortProcessorAdapter implements ExternalPortProcessor.Context {

    private final JobflowCompiler.Context delegate;

    private final String batchId;

    private final String flowId;

    private final DataModelLoaderAdapter dataModelLoader;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     * @param batch the structural information of the target batch
     * @param jobflow the structural information of the target jobflow
     */
    public ExternalPortProcessorAdapter(JobflowCompiler.Context delegate, BatchInfo batch, JobflowInfo jobflow) {
        this(delegate, batch.getBatchId(), jobflow.getFlowId());
    }

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     * @param batchId the target batch ID
     * @param flowId the target flow ID
     */
    public ExternalPortProcessorAdapter(JobflowCompiler.Context delegate, String batchId, String flowId) {
        this.delegate = delegate;
        this.batchId = batchId;
        this.flowId = flowId;
        this.dataModelLoader = new DataModelLoaderAdapter(delegate);
    }

    @Override
    public CompilerOptions getOptions() {
        return delegate.getOptions();
    }

    @Override
    public String getBatchId() {
        return batchId;
    }

    @Override
    public String getFlowId() {
        return flowId;
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getProject().getClassLoader();
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return dataModelLoader;
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        return addResourceFile(Util.toClassFileLocation(aClass));
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return delegate.getOutput().addResource(location);
    }

    @Override
    public void addTask(Phase phase, TaskReference task) {
        delegate.getTaskContainerMap().getTaskContainer(phase).add(task);
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }
}
