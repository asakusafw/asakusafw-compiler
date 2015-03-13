package com.asakusafw.lang.compiler.core.basic;

import com.asakusafw.lang.compiler.analyzer.BatchAnalyzer;
import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.analyzer.FlowGraphAnalyzer;
import com.asakusafw.lang.compiler.analyzer.JobflowAnalyzer;
import com.asakusafw.lang.compiler.analyzer.adapter.BatchAdapter;
import com.asakusafw.lang.compiler.analyzer.adapter.JobflowAdapter;
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
    public boolean isBatchClass(Context context, Class<?> aClass) {
        return BatchAdapter.isBatch(aClass);
    }

    @Override
    public boolean isJobflowClass(Context context, Class<?> aClass) {
        return JobflowAdapter.isJobflow(aClass);
    }

    @Override
    public Batch analyzeBatch(Context context, Class<?> batchClass) {
        return createBatchAnalyzer(context).analyze(batchClass);
    }

    @Override
    public Jobflow analyzeJobflow(Context context, Class<?> jobflowClass) {
        return createJobflowAnalyzer(context).analyze(jobflowClass);
    }

    BatchAnalyzer createBatchAnalyzer(AnalyzerContext context) {
        return new BatchAnalyzer(createJobflowAnalyzer(context));
    }

    JobflowAnalyzer createJobflowAnalyzer(AnalyzerContext context) {
        return new JobflowAnalyzer(createFlowGraphAnalyzer(context));
    }

    FlowGraphAnalyzer createFlowGraphAnalyzer(AnalyzerContext context) {
        return new FlowGraphAnalyzer(createExternalPortAnalyzer(context));
    }

    ExternalPortAnalyzer createExternalPortAnalyzer(AnalyzerContext context) {
        return new ExternalPortAnalyzerAdapter(context);
    }
}
