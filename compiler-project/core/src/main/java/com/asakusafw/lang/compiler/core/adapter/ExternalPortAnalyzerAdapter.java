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
package com.asakusafw.lang.compiler.core.adapter;

import java.util.Map;

import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.core.AnalyzerContext;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * An adapter for {@link ExternalPortAnalyzer}.
 */
public class ExternalPortAnalyzerAdapter implements ExternalPortAnalyzer {

    private final ExternalPortProcessor processor;

    private final ExternalPortProcessor.AnalyzeContext context;

    /**
     * Creates a new instance.
     * @param context the delegate context
     */
    public ExternalPortAnalyzerAdapter(AnalyzerContext context) {
        this.processor = context.getTools().getExternalPortProcessor();
        this.context = new ContextAdapter(context);
    }

    /**
     * Creates a new instance.
     * @param processor the delegate processor
     * @param context the delegate context
     */
    public ExternalPortAnalyzerAdapter(ExternalPortProcessor processor, ExternalPortProcessor.AnalyzeContext context) {
        this.processor = processor;
        this.context = context;
    }

    @Override
    public ExternalInputInfo analyze(String name, ImporterDescription description) {
        return processor.analyzeInput(context, name, description);
    }

    @Override
    public ExternalOutputInfo analyze(String name, ExporterDescription description) {
        return processor.analyzeOutput(context, name, description);
    }

    @Override
    public void validate(Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        processor.validate(context, inputs, outputs);
    }

    private static final class ContextAdapter implements ExternalPortProcessor.AnalyzeContext {

        private final AnalyzerContext delegate;

        private final DataModelLoader dataModelLoader;

        ContextAdapter(AnalyzerContext delegate) {
            this.delegate = delegate;
            this.dataModelLoader = new DataModelLoaderAdapter(delegate);
        }

        @Override
        public ClassLoader getClassLoader() {
            return delegate.getProject().getClassLoader();
        }

        @Override
        public DataModelLoader getDataModelLoader() {
            return dataModelLoader;
        }
    }
}
