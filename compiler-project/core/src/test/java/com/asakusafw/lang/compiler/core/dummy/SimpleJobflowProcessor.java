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
package com.asakusafw.lang.compiler.core.dummy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;
import com.asakusafw.lang.compiler.packaging.ZipRepository;

/**
 * Mock implementation of {@link JobflowProcessor}.
 */
public class SimpleJobflowProcessor implements JobflowProcessor {

    private static final String MODULE_NAME = SimpleJobflowProcessor.class.getSimpleName().toLowerCase();

    private static final Location MARKER = Location.of(SimpleJobflowProcessor.class.getName(), '.');

    private boolean useExternalPort = false;

    /**
     * Set whether the processor uses external port or not.
     * @param value the value
     * @return this
     */
    public SimpleJobflowProcessor withUseExternalPort(boolean value) {
        this.useExternalPort = value;
        return this;
    }

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(JobflowCompiler.Context context) {
        return containsFile(context) && containsTask(context);
    }

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @param flowId the target flow ID
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(BatchCompiler.Context context, String flowId) {
        Location location = JobflowPackager.getLibraryLocation(flowId);
        File library = context.getOutput().toFile(location);
        try (ResourceRepository.Cursor cursor = new ZipRepository(library).createCursor()) {
            while (cursor.next()) {
                if (cursor.getLocation().equals(MARKER)) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return false;
    }

    private static boolean containsFile(JobflowCompiler.Context context) {
        File file = context.getOutput().toFile(MARKER);
        return file.isFile();
    }

    private static boolean containsTask(JobflowCompiler.Context context) {
        for (TaskReference t : context.getTaskContainerMap().getTasks(TaskReference.Phase.MAIN)) {
            if (t instanceof CommandTaskReference) {
                if (t.getModuleName().contains(MODULE_NAME)) {
                    return true;
                }
            }
        }
        return false;
    }



    @Override
    public void process(Context context, Jobflow source) throws IOException {
        try (OutputStream output = context.addResourceFile(MARKER)) {
            output.write("testing".getBytes("UTF-8"));
        }
        context.addTask(
                MODULE_NAME,
                "testing",
                Location.of("simple.sh"),
                Collections.emptyList());
        if (useExternalPort) {
            for (ExternalInput port : source.getOperatorGraph().getInputs().values()) {
                context.addExternalInput(port.getName(), port.getInfo());
            }
            for (ExternalOutput port : source.getOperatorGraph().getOutputs().values()) {
                context.addExternalOutput(port.getName(), port.getInfo(), Collections.singleton(port.getName()));
            }
        }
    }
}
