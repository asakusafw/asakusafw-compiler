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
package com.asakusafw.lang.compiler.extension.iterative.mock;

import java.util.Map;

import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Mock implementation of {@link ExternalPortAnalyzer}.
 */
public class MockExternalPortAnalyzer implements ExternalPortAnalyzer {

    @Override
    public ExternalInputInfo analyze(String name, ImporterDescription description) {
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(description.getClass()),
                "mock",
                Descriptions.classOf(description.getModelType()),
                ExternalInputInfo.DataSize.UNKNOWN);
    }

    @Override
    public ExternalOutputInfo analyze(String name, ExporterDescription description) {
        return new ExternalOutputInfo.Basic(
                Descriptions.classOf(description.getClass()),
                "mock",
                Descriptions.classOf(description.getModelType()));
    }

    @Override
    public void validate(Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        return;
    }
}
