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
package com.asakusafw.lang.compiler.tester;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalPortReferenceMap;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Provides external I/O port information.
 * @since 0.8.0
 */
public final class ExternalPortMap {

    private final Map<String, ExternalInputInfo> inputInfos = new LinkedHashMap<>();

    private final Map<String, ExternalOutputInfo> outputInfos = new LinkedHashMap<>();

    private final Map<String, ExternalInputReference> inputRefs = new LinkedHashMap<>();

    private final Map<String, ExternalOutputReference> outputRefs = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @see #analyze(OperatorGraph, ExternalPortReferenceMap)
     */
    public ExternalPortMap() {
        return;
    }

    /**
     * Analyze external I/O ports.
     * @param graph the target graph
     * @param references the reference map
     * @return the analyzed map
     */
    public static ExternalPortMap analyze(OperatorGraph graph, ExternalPortReferenceMap references) {
        ExternalPortMap result = new ExternalPortMap();
        for (Map.Entry<String, ExternalInput> entry : graph.getInputs().entrySet()) {
            if (entry.getValue().isExternal()) {
                result.inputInfos.put(entry.getKey(), entry.getValue().getInfo());
            }
        }
        for (Map.Entry<String, ExternalOutput> entry : graph.getOutputs().entrySet()) {
            if (entry.getValue().isExternal()) {
                result.outputInfos.put(entry.getKey(), entry.getValue().getInfo());
            }
        }
        for (ExternalInputReference ref : references.getInputs()) {
            result.inputRefs.put(ref.getName(), ref);
        }
        for (ExternalOutputReference ref : references.getOutputs()) {
            result.outputRefs.put(ref.getName(), ref);
        }
        return result;
    }

    /**
     * Returns the available input name.
     * @return the port names
     */
    public Set<String> getInputs() {
        return Collections.unmodifiableSet(inputInfos.keySet());
    }

    /**
     * Returns the available output name.
     * @return the port names
     */
    public Set<String> getOutputs() {
        return Collections.unmodifiableSet(outputInfos.keySet());
    }

    /**
     * Returns the external input info.
     * @param name the input name
     * @return the related information
     */
    public ExternalInputInfo findInputInfo(String name) {
        return inputInfos.get(name);
    }

    /**
     * Returns the external output info.
     * @param name the output name
     * @return the related information
     */
    public ExternalOutputInfo findOutputInfo(String name) {
        return outputInfos.get(name);
    }

    /**
     * Returns the external input reference.
     * @param name the input name
     * @return the related reference, or {@code null} if there is no such a reference
     */
    public ExternalInputReference findInputReference(String name) {
        return inputRefs.get(name);
    }

    /**
     * Returns the external output reference.
     * @param name the output name
     * @return the related reference, or {@code null} if there is no such a reference
     */
    public ExternalOutputReference findOutputReference(String name) {
        return outputRefs.get(name);
    }
}
