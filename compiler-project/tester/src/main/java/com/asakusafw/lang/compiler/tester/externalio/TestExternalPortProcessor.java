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
package com.asakusafw.lang.compiler.tester.externalio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalPortReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * An implementation of {@link ExternalPortProcessor} for {@link TestInput} and {@link TestOutput}.
 * @since 0.4.0
 */
public class TestExternalPortProcessor implements ExternalPortProcessor {

    /**
     * The module name.
     */
    public static final String MODULE_NAME = "testexternalio"; //$NON-NLS-1$

    /**
     * The profile name of input operations.
     */
    public static final String INPUT_PROFILE_NAME = "input"; //$NON-NLS-1$

    /**
     * The profile name of output operations.
     */
    public static final String OUTPUT_PROFILE_NAME = "output"; //$NON-NLS-1$

    /**
     * The phase of input operations.
     */
    public static final Phase PHASE_INPUT = Phase.IMPORT;

    /**
     * The phase of output operations.
     */
    public static final Phase PHASE_OUTPUT = Phase.EXPORT;

    private static final Location INPUT_PREFIX = Location.of("__TEST__/input"); //$NON-NLS-1$

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return TestInput.class.isAssignableFrom(descriptionClass)
                || TestOutput.class.isAssignableFrom(descriptionClass);
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        if (description instanceof TestInput) {
            return ((TestInput) description).toInfo();
        }
        throw new IllegalStateException();
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        if (description instanceof TestOutput) {
            return ((TestOutput) description).toInfo();
        }
        throw new IllegalStateException();
    }

    @Override
    public void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        return;
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        String path = context.getOptions().getRuntimeWorkingPath(INPUT_PREFIX.append(name).toPath());
        ExternalInputReference reference = new ExternalInputReference(name, info, Collections.singleton(path));
        return reference;
    }

    @Override
    public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        ExternalOutputReference reference = new ExternalOutputReference(name, info, internalOutputPaths);
        return reference;
    }

    @Override
    public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs) {
        for (ExternalInputReference reference : inputs) {
            CommandTaskReference task = new CommandTaskReference(
                    MODULE_NAME,
                    INPUT_PROFILE_NAME,
                    Location.of("/bin/echo"),
                    toArguments(reference),
                    Collections.emptyList());
            context.addTask(PHASE_INPUT, task);
        }
        for (ExternalOutputReference reference : outputs) {
            CommandTaskReference task = new CommandTaskReference(
                    MODULE_NAME,
                    OUTPUT_PROFILE_NAME,
                    Location.of("/bin/echo"),
                    toArguments(reference),
                    Collections.emptyList());
            context.addTask(PHASE_OUTPUT, task);
        }
        return;
    }

    private static List<CommandToken> toArguments(ExternalPortReference reference) {
        List<CommandToken> results = new ArrayList<>();
        results.add(CommandToken.of(((ImmediateDescription) reference.getContents()).toString()));
        results.add(CommandToken.of(reference.getDataModelClass().getBinaryName()));
        reference.getPaths().stream()
                .map(CommandToken::of)
                .forEach(results::add);
        return results;
    }

    @Override
    public <T> T getAdaper(AnalyzeContext context, Class<T> adapterType, Class<?> descriptionClass) {
        return null;
    }
}
