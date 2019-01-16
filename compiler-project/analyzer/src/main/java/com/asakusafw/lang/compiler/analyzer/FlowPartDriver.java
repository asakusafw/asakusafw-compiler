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
package com.asakusafw.lang.compiler.analyzer;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;

/**
 * Builds an {@link OperatorGraph} from generic {@link FlowDescription} object.
 * @see JobflowAnalyzer
 */
public class FlowPartDriver {

    private final FlowGraphAnalyzer graphAnalyzer;

    private final FlowDescriptionAnalyzer descriptionAnalyzer = new FlowDescriptionAnalyzer();

    /**
     * Creates a new instance.
     * @param analyzer the element analyzer
     */
    public FlowPartDriver(FlowGraphAnalyzer analyzer) {
        this.graphAnalyzer = analyzer;
    }

    /**
     * Adds an external input operator.
     * @param <T> input data type
     * @param name the port name
     * @param description the external input description
     * @return the created operator
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public <T> In<T> addInput(String name, ImporterDescription description) {
        return descriptionAnalyzer.addInput(name, description);
    }

    /**
     * Adds an external output operator.
     * @param <T> output data type
     * @param name the port name
     * @param description the external output description
     * @return the created operator
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public <T> Out<T> addOutput(String name, ExporterDescription description) {
        return descriptionAnalyzer.addOutput(name, description);
    }

    /**
     * Builds a jobflow object.
     * @param flowId the target flow ID
     * @param description the target flow description
     * @return the built jobflow object
     * @throws DiagnosticException if failed to analyze flow DSL
     */
    public Jobflow build(String flowId, FlowDescription description) {
        OperatorGraph graph = build(description);
        return new Jobflow(new JobflowInfo.Basic(flowId, Descriptions.classOf(description.getClass())), graph);
    }

    /**
     * Builds an {@link OperatorGraph}.
     * @param description the target flow description
     * @return the built operator graph
     * @throws DiagnosticException if failed to analyze flow DSL
     */
    public OperatorGraph build(FlowDescription description) {
        return build(description, descriptionAnalyzer);
    }

    /**
     * Builds an {@link OperatorGraph}.
     * @param description the target flow description
     * @param analyzer the driver for {@link FlowDescription}
     * @return the built operator graph
     * @throws DiagnosticException if failed to analyze flow DSL
     */
    public OperatorGraph build(FlowDescription description, FlowDescriptionAnalyzer analyzer) {
        FlowGraph flowGraph = analyzer.analyze(description);
        FlowGraphVerifier.verify(flowGraph);
        OperatorGraph graph = graphAnalyzer.analyze(flowGraph);
        return graph;
    }
}
