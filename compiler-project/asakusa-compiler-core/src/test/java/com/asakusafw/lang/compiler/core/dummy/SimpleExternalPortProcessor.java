/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Mock implementation of {@link ExternalPortProcessor}.
 */
public class SimpleExternalPortProcessor implements ExternalPortProcessor {

    private static final String MODULE_NAME = SimpleExternalPortProcessor.class.getSimpleName().toLowerCase();

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(JobflowCompiler.Context context) {
        for (TaskReference t : context.getTaskContainerMap().getTasks(TaskReference.Phase.FINALIZE)) {
            if (t instanceof CommandTaskReference) {
                if (t.getModuleName().contains(MODULE_NAME)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return true;
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(description.getClass()),
                MODULE_NAME,
                Descriptions.classOf(String.class),
                ExternalInputInfo.DataSize.UNKNOWN);
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        return new ExternalOutputInfo.Basic(
                Descriptions.classOf(description.getClass()),
                MODULE_NAME,
                Descriptions.classOf(String.class));
    }

    @Override
    public void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        return;
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        return new ExternalInputReference(name, info, Collections.singleton(name));
    }

    @Override
    public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        return new ExternalOutputReference(name, info, internalOutputPaths);
    }

    @Override
    public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs)
            throws IOException {
        context.addTask(TaskReference.Phase.FINALIZE, new CommandTaskReference(
                MODULE_NAME,
                "testing",
                Location.of("simple.sh"),
                Collections.<CommandToken>emptyList(),
                Collections.<TaskReference>emptyList()));
    }
}
