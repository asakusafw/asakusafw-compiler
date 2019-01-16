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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.analyzer.adapter.JobflowAdapter;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;

/**
 * Analyzes a <em>jobflow class</em> described as <em>Asakusa Flow DSL</em>.
 */
public class JobflowAnalyzer {

    static final Logger LOG = LoggerFactory.getLogger(JobflowAnalyzer.class);

    private final FlowGraphAnalyzer elementAnalyzer;

    /**
     * Creates a new instance.
     * @param elementAnalyzer the flow graph analyzer
     */
    public JobflowAnalyzer(FlowGraphAnalyzer elementAnalyzer) {
        this.elementAnalyzer = elementAnalyzer;
    }

    /**
     * Analyzes the target <em>jobflow class</em> and returns a complete graph model object.
     * @param aClass the target class
     * @return the related complete graph model object
     */
    public Jobflow analyze(Class<?> aClass) {
        JobflowAdapter adapter = JobflowAdapter.analyze(aClass);
        return analyze(adapter);
    }

    /**
     * Analyzes the target jobflow using DSL adapter, and returns a complete graph model object.
     * @param adapter a DSL adapter for the target jobflow
     * @return the related complete graph model object
     */
    public Jobflow analyze(JobflowAdapter adapter) {
        LOG.debug("analyzing jobflow class: {}", adapter.getDescription().getName()); //$NON-NLS-1$
        FlowGraph flowGraph = toFlowGraph(adapter);
        FlowGraphVerifier.verify(flowGraph);
        OperatorGraph graph = elementAnalyzer.analyze(flowGraph);
        return new Jobflow(adapter.getInfo(), graph);
    }

    private FlowGraph toFlowGraph(JobflowAdapter adapter) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        List<JobflowAdapter.Parameter> parameters = adapter.getParameters();
        List<Object> arguments = new ArrayList<>();
        FlowDescriptionAnalyzer analyzer = new FlowDescriptionAnalyzer();
        for (int i = 0, n = parameters.size(); i < n; i++) {
            JobflowAdapter.Parameter parameter = parameters.get(i);
            String name = parameter.getName();
            Object desc;
            try {
                desc = parameter.getDescriptionClass().newInstance();
            } catch (Exception e) {
                raise(diagnostics, adapter.getDescription(), i, MessageFormat.format(
                        "failed to initialize description of jobflow {0}: {1}",
                        parameter.getDirection(),
                        parameter.getDescriptionClass().getName()), null);
                continue;
            }
            if (parameter.getDirection() == JobflowAdapter.Direction.INPUT) {
                ImporterDescription description = (ImporterDescription) desc;
                if (description.getModelType() != parameter.getDataModelClass()) {
                    raise(diagnostics, adapter.getDescription(), i, MessageFormat.format(
                            "inconsistent jobflow input data type: {0} <-> {1}",
                            className(description.getModelType()), className(parameter.getDataModelClass())), null);
                    continue;
                }
                arguments.add(analyzer.addInput(name, description));
            } else {
                ExporterDescription description = (ExporterDescription) desc;
                if (description.getModelType() != parameter.getDataModelClass()) {
                    raise(diagnostics, adapter.getDescription(), i, MessageFormat.format(
                            "inconsistent jobflow output data type: {0} <-> {1}",
                            className(description.getModelType()), className(parameter.getDataModelClass())), null);
                    continue;
                }
                arguments.add(analyzer.addOutput(name, description));
            }
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        FlowDescription flowDescription = adapter.newInstance(arguments);
        return analyzer.analyze(flowDescription);
    }

    private static String className(Class<?> aClass) {
        return aClass == null ? "null" : aClass.getName(); //$NON-NLS-1$
    }

    private static void raise(
            List<Diagnostic> diagnostics, Class<?> atClass, int atIndex, String message, Exception cause) {
        String decorated = MessageFormat.format(
                "{0} ({1} at parameter #{2})",
                message,
                atClass.getName(),
                atIndex);
        diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, decorated, cause));
    }
}
