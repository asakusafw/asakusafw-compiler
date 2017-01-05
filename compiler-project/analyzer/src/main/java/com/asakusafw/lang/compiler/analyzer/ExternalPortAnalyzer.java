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
package com.asakusafw.lang.compiler.analyzer;

import java.util.Map;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Analyzes {@link ImporterDescription} and {@link ExporterDescription} objects.
 */
public interface ExternalPortAnalyzer {

    /**
     * Analyzes the target importer description.
     * @param name the input name
     * @param description the target importer description
     * @return the structural information of the target importer description
     * @throws DiagnosticException if failed to resolve the target description
     */
    ExternalInputInfo analyze(String name, ImporterDescription description);

    /**
     * Analyzes the target exporter description.
     * @param name the input name
     * @param description the target exporter description
     * @return the structural information of the target exporter description
     * @throws DiagnosticException if failed to resolve the target description
     */
    ExternalOutputInfo analyze(String name, ExporterDescription description);

    /**
     * Validates inputs and outputs.
     * @param inputs the external inputs
     * @param outputs the external outputs
     * @throws DiagnosticException if failed to resolve the target description
     */
    void validate(Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs);
}
