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
package com.asakusafw.lang.compiler.extension.externalio;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * An abstract implementation of {@link ExternalPortProcessor}.
 * @param <TInput> the importer description type
 * @param <TOutput> the exporter description type
 * @since 0.1.0
 * @version 0.3.0
 */
public abstract class AbstractExternalPortProcessor<
            TInput extends ImporterDescription,
            TOutput extends ExporterDescription>
        implements ExternalPortProcessor {

    /**
     * Returns the external I/O module name.
     * @return the module name
     */
    protected abstract String getModuleName();

    /**
     * Returns the importer description type.
     * @return the importer description type
     */
    protected abstract Class<TInput> getInputDescriptionType();

    /**
     * Returns the exporter description type.
     * @return the exporter description type
     */
    protected abstract Class<TOutput> getOutputDescriptionType();

    /**
     * Returns the properties of the target importer description.
     * @param context  the current context
     * @param name the input name
     * @param description the target description
     * @return the extracted properties, or {@code null} if the input does not have any additional properties
     */
    protected abstract ValueDescription analyzeInputProperties(
            AnalyzeContext context, String name, TInput description);

    /**
     * Returns the properties of the target exporter description.
     * @param context  the current context
     * @param name the input name
     * @param description the target description
     * @return the extracted properties, or {@code null} if the output does not have any additional properties
     */
    protected abstract ValueDescription analyzeOutputProperties(
            AnalyzeContext context, String name, TOutput description);

    /**
     * Returns the parameter names of the target importer description.
     * @param context  the current context
     * @param name the input name
     * @param description the target description
     * @return the parameter names
     * @since 0.3.0
     */
    protected abstract Set<String> analyzeInputParameterNames(
            AnalyzeContext context, String name, TInput description);

    /**
     * Returns the parameter names of the target exporter description.
     * @param context  the current context
     * @param name the input name
     * @param description the target description
     * @return the parameter names
     * @since 0.3.0
     */
    protected abstract Set<String> analyzeOutputParameterNames(
            AnalyzeContext context, String name, TOutput description);

    /**
     * Returns input paths, which will be used in main phase, of the target external input.
     * @param context  the current context
     * @param name the input name
     * @param info the structural information of the target input
     * @return the computed input paths
     */
    protected abstract Set<String> computeInputPaths(Context context, String name, ExternalInputInfo info);

    /**
     * Returns a temporary path for this component execution.
     * @param context the current context
     * @param phase the target phase
     * @param location relative location, or {@code null} for base path
     * @return a temporary working path
     */
    protected String getTemporaryPath(Context context, TaskReference.Phase phase, Location location) {
        Location path = Location.of(getModuleName()).append(phase.getSymbol());
        if (location != null) {
            path = path.append(location);
        }
        return context.getOptions().getRuntimeWorkingPath(path.toPath());
    }

    @Override
    public final boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        Class<TInput> input = getInputDescriptionType();
        Class<TOutput> output = getOutputDescriptionType();
        return input.isAssignableFrom(descriptionClass) || output.isAssignableFrom(descriptionClass);
    }

    @Override
    public final ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        Class<TInput> type = getInputDescriptionType();
        if (type.isInstance(description) == false) {
            throw new IllegalArgumentException();
        }
        TInput desc = type.cast(description);
        ValueDescription properties = analyzeInputProperties(context, name, desc);
        Set<String> parameters = analyzeInputParameterNames(context, name, desc);
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(desc.getClass()),
                getModuleName(),
                Descriptions.classOf(desc.getModelType()),
                convert(desc.getDataSize()),
                parameters,
                properties);
    }

    @Override
    public final ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        Class<TOutput> type = getOutputDescriptionType();
        if (type.isInstance(description) == false) {
            throw new IllegalArgumentException();
        }
        TOutput desc = type.cast(description);
        ValueDescription properties = analyzeOutputProperties(context, name, desc);
        Set<String> parameters = analyzeOutputParameterNames(context, name, desc);
        return new ExternalOutputInfo.Basic(
                Descriptions.classOf(desc.getClass()),
                getModuleName(),
                Descriptions.classOf(desc.getModelType()),
                parameters,
                properties);
    }

    @Override
    public void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        return;
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        Collection<String> paths = computeInputPaths(context, name, info);
        return new ExternalInputReference(name, info, paths);
    }

    @Override
    public ExternalOutputReference resolveOutput(
            Context context,
            String name,
            ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        return new ExternalOutputReference(name, info, internalOutputPaths);
    }

    @Override
    public <T> T getAdaper(AnalyzeContext context, Class<T> adapterType, Class<?> descriptionClass) {
        if (isSupported(context, descriptionClass)) {
            return getAdapter(adapterType);
        }
        return null;
    }

    /**
     * Returns the adapter object.
     * @param <T> the adapter type
     * @param adapterType the adapter type
     * @return the adapter object for the type, or {@code null} if this does not support the target adapter type
     */
    protected <T> T getAdapter(Class<T> adapterType) {
        return null;
    }

    private static ExternalInputInfo.DataSize convert(ImporterDescription.DataSize value) {
        if (value == null) {
            return ExternalInputInfo.DataSize.UNKNOWN;
        }
        switch (value) {
        case TINY:
            return ExternalInputInfo.DataSize.TINY;
        case SMALL:
            return ExternalInputInfo.DataSize.SMALL;
        case LARGE:
            return ExternalInputInfo.DataSize.LARGE;
        default:
            return ExternalInputInfo.DataSize.UNKNOWN;
        }
    }
}
