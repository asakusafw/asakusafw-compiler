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
package com.asakusafw.lang.compiler.api.testing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * A mock {@link ExternalPortProcessor} implementation for
 * {@link MockImporterDescription} and {@link MockExporterDescription}.
 * <p>
 * This does nothing in {@link #process(com.asakusafw.lang.compiler.api.ExternalPortProcessor.Context, List, List)}.
 * Clients should prepare the jobflow input datasets onto {@link ExternalInputReference#getPaths()},
 * and obtain the jobflow output datasets from {@link ExternalOutputReference#getPaths()}.
 * </p>
 * <p>
 * This may be used for testing {@link JobflowProcessor}s.
 * </p>
 * @see MockJobflowProcessor
 */
public class MockExternalPortProcessor implements ExternalPortProcessor {

    private static final Location DEFAULT_INPUT_PREFIX = Location.of("mock/input"); //$NON-NLS-1$

    private final Location inputPrefix;

    private final List<?> adapters;

    /**
     * Creates a new instance.
     */
    public MockExternalPortProcessor() {
        this(DEFAULT_INPUT_PREFIX);
    }

    /**
     * Creates a new instance.
     * @param inputPrefix the input path prefix
     */
    public MockExternalPortProcessor(Location inputPrefix) {
        this(inputPrefix, Collections.emptyList());
    }

    /**
     * Creates a new instance.
     * @param inputPrefix the input path prefix
     * @param adapters the adapter objects for this
     */
    public MockExternalPortProcessor(Location inputPrefix, List<?> adapters) {
        this.inputPrefix = inputPrefix;
        this.adapters = new ArrayList<>(adapters);
    }

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return MockImporterDescription.class.isAssignableFrom(descriptionClass)
                || MockExporterDescription.class.isAssignableFrom(descriptionClass);
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        if (description instanceof MockImporterDescription) {
            return ((MockImporterDescription) description).toInfo();
        }
        throw new IllegalStateException();
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        if (description instanceof MockExporterDescription) {
            return ((MockExporterDescription) description).toInfo();
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
        String path = context.getOptions().getRuntimeWorkingPath(inputPrefix.append(name).toPath());
        return new ExternalInputReference(name, info, Collections.singleton(path));
    }

    @Override
    public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        return new ExternalOutputReference(name, info, internalOutputPaths);
    }

    @Override
    public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs) {
        return;
    }

    @Override
    public <T> T getAdaper(AnalyzeContext context, Class<T> adapterType, Class<?> descriptionClass) {
        for (Object adapter : adapters) {
            if (adapterType.isInstance(adapter)) {
                return adapterType.cast(adapter);
            }
        }
        return null;
    }
}
