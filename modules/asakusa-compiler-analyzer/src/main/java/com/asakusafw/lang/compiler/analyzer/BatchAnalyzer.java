package com.asakusafw.lang.compiler.analyzer;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.analyzer.adapter.BatchAdapter;
import com.asakusafw.lang.compiler.api.Diagnostic;
import com.asakusafw.lang.compiler.api.DiagnosticException;
import com.asakusafw.lang.compiler.api.basic.BasicDiagnostic;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;
import com.asakusafw.vocabulary.batch.BatchDescription;
import com.asakusafw.vocabulary.batch.JobFlowWorkDescription;
import com.asakusafw.vocabulary.batch.Work;
import com.asakusafw.vocabulary.batch.WorkDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;

/**
 * Analyzes a <em>batch class</em> described as <em>Asakusa Batch DSL</em>.
 */
public class BatchAnalyzer {

    private final JobflowAnalyzer elementAnalyzer;

    /**
     * Creates a new instance.
     * @param elementAnalyzer the jobflow analyzer
     */
    public BatchAnalyzer(JobflowAnalyzer elementAnalyzer) {
        this.elementAnalyzer = elementAnalyzer;
    }

    /**
     * Analyzes the target <em>batch class</em> and returns a complete graph model object.
     * @param aClass the target class
     * @return the related complete graph model object
     */
    public Batch analyze(Class<?> aClass) {
        BatchAdapter adapter = BatchAdapter.analyze(aClass);
        return analyze(adapter);
    }

    /**
     * Analyzes the target batch using DSL adapter, and returns a complete graph model object.
     * @param adapter a DSL adapter for the target batch
     * @return the related complete graph model object
     */
    private Batch analyze(BatchAdapter adapter) {
        Graph<Class<?>> jobflowGraph = toJobflowGraph(adapter);
        List<Diagnostic> diagnostics = new ArrayList<>();
        Map<Class<?>, Jobflow> jobflowMap = new HashMap<>();
        Batch result = new Batch(adapter.getInfo());
        for (Class<?> aClass : Graphs.sortPostOrder(jobflowGraph)) {
            try {
                Jobflow analyzed = elementAnalyzer.analyze(aClass);
                jobflowMap.put(aClass, analyzed);
                result.addElement(analyzed);
            } catch (DiagnosticException e) {
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        for (Graph.Vertex<Class<?>> vertex : jobflowGraph) {
            Jobflow currentJobflow = jobflowMap.get(vertex.getNode());
            assert currentJobflow != null;
            BatchElement currentElement = result.findElement(currentJobflow);
            assert currentElement != null;
            for (Class<?> blocker : vertex.getConnected()) {
                Jobflow blockerJobflow = jobflowMap.get(blocker);
                assert blockerJobflow != null;
                BatchElement blockerElement = result.findElement(blockerJobflow);
                assert blockerElement != null;
                currentElement.addBlockerElement(blockerElement);
            }
        }
        return result;
    }

    private Graph<Class<?>> toJobflowGraph(BatchAdapter adapter) {
        BatchDescription instance = adapter.newInstance();
        try {
            instance.start();
        } catch (Exception e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while executing {0}#describe()",
                    adapter.getDescription().getName()), e);
        }
        List<Diagnostic> diagnostics = new ArrayList<>();
        Graph<Class<?>> results = Graphs.newInstance();
        for (Work work : instance.getWorks()) {
            WorkDescription desc = work.getDescription();
            if ((desc instanceof JobFlowWorkDescription) == false) {
                raise(diagnostics, adapter.getDescription(), MessageFormat.format(
                        "each batch element must be a jobflow: {0}",
                        desc));
            }
            Class<? extends FlowDescription> flow = ((JobFlowWorkDescription) desc).getFlowClass();
            assert results.contains(flow) == false;
            if (results.contains(flow)) {
                // may not occur in current implementation
                raise(diagnostics, adapter.getDescription(), MessageFormat.format(
                        "each batch element must be unique: {0}",
                        flow.getName()));
            }
            results.addNode(flow);
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        for (Work currentWork : instance.getWorks()) {
            Class<?> currentFlow = ((JobFlowWorkDescription) currentWork.getDescription()).getFlowClass();
            for (Work blockerWork : currentWork.getDependencies()) {
                Class<?> blockerFlow = ((JobFlowWorkDescription) blockerWork.getDescription()).getFlowClass();
                results.addEdge(currentFlow, blockerFlow);
            }
        }
        return results;
    }

    private static void raise(List<Diagnostic> diagnostics, Class<?> atClass, String message) {
        String decorated = MessageFormat.format(
                "{0} ({1})",
                message,
                atClass.getName());
        diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, decorated, null));
    }
}
