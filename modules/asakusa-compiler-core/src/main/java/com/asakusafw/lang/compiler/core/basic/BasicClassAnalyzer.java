package com.asakusafw.lang.compiler.core.basic;

import com.asakusafw.lang.compiler.analyzer.BatchAnalyzer;
import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.analyzer.FlowGraphAnalyzer;
import com.asakusafw.lang.compiler.analyzer.JobflowAnalyzer;
import com.asakusafw.lang.compiler.core.AnalyzerContext;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.adapter.ExternalPortAnalyzerAdapter;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Analyzes Asakusa DSL elements.
 */
public class BasicClassAnalyzer implements ClassAnalyzer {

    @Override
    public Batch analyzeBatch(Context context, Class<?> batchClass) {
        return createBatchAnalyzer(context).analyze(batchClass);
    }

    @Override
    public Jobflow analyzeJobflow(Context context, Class<?> jobflowClass) {
        return createJobflowAnalyzer(context).analyze(jobflowClass);
    }

    /**
     * Returns a new sub-analyzer for batches.
     * @param context the current context
     * @return the created sub-analyzer
     */
    public BatchAnalyzer createBatchAnalyzer(AnalyzerContext context) {
        return new BatchAnalyzer(createJobflowAnalyzer(context));
    }

    /**
     * Returns a new sub-analyzer for jobflows.
     * @param context the current context
     * @return the created sub-analyzer
     */
    public JobflowAnalyzer createJobflowAnalyzer(AnalyzerContext context) {
        return new JobflowAnalyzer(createFlowGraphAnalyzer(context));
    }

    /**
     * Returns a new sub-analyzer for flow-graphs.
     * @param context the current context
     * @return the created sub-analyzer
     */
    public FlowGraphAnalyzer createFlowGraphAnalyzer(AnalyzerContext context) {
        return new FlowGraphAnalyzer(createExternalPortAnalyzer(context));
    }

    private ExternalPortAnalyzer createExternalPortAnalyzer(AnalyzerContext context) {
        return new ExternalPortAnalyzerAdapter(context);
    }
}
