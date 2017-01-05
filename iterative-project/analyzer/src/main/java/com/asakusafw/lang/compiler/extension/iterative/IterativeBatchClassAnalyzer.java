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
package com.asakusafw.lang.compiler.extension.iterative;

import java.util.Set;

import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.analyzer.FlowGraphAnalyzer;
import com.asakusafw.lang.compiler.analyzer.JobflowAnalyzer;
import com.asakusafw.lang.compiler.core.AnalyzerContext;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.adapter.ExternalPortAnalyzerAdapter;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.vocabulary.iterative.IterativeBatch;

/**
 * Analyzes classes with {@link IterativeBatch} annotation.
 * @since 0.3.0
 */
public class IterativeBatchClassAnalyzer implements ClassAnalyzer {

    @Override
    public boolean isBatchClass(Context context, Class<?> aClass) {
        return IterativeBatchAnalyzer.isBatch(aClass);
    }

    @Override
    public boolean isJobflowClass(Context context, Class<?> aClass) {
        return isBatchClass(context, aClass);
    }

    @Override
    public boolean isFlowObject(Context context, Object object) {
        return false;
    }

    @Override
    public Batch analyzeBatch(Context context, Class<?> batchClass) {
        return createBatchAnalyzer(context).analyze(batchClass);
    }

    @Override
    public Jobflow analyzeJobflow(Context context, Class<?> jobflowClass) {
        Batch batch = analyzeBatch(context, jobflowClass);
        Set<BatchElement> elements = batch.getElements();
        assert elements.size() == 1;
        return elements.iterator().next().getJobflow();
    }

    @Override
    public OperatorGraph analyzeFlow(Context context, Object flowObject) {
        throw new UnsupportedOperationException();
    }

    static IterativeBatchAnalyzer createBatchAnalyzer(AnalyzerContext context) {
        return new IterativeBatchAnalyzer(createJobflowAnalyzer(context));
    }

    static JobflowAnalyzer createJobflowAnalyzer(AnalyzerContext context) {
        return new JobflowAnalyzer(createFlowGraphAnalyzer(context));
    }

    static FlowGraphAnalyzer createFlowGraphAnalyzer(AnalyzerContext context) {
        return new FlowGraphAnalyzer(createExternalPortAnalyzer(context), new IterativeOperatorAnalyzer());
    }

    static ExternalPortAnalyzer createExternalPortAnalyzer(AnalyzerContext context) {
        return new ExternalPortAnalyzerAdapter(context);
    }
}
