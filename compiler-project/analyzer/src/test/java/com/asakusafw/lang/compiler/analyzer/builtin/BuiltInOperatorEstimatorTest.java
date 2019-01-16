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
package com.asakusafw.lang.compiler.analyzer.builtin;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOperatorEstimate;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;

/**
 * Test for {@link BuiltInOperatorEstimator}.
 *
 */
public class BuiltInOperatorEstimatorTest extends BuiltInOptimizerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Operator operator = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();

        OperatorEstimate estimate = estimate(context(), operator, 100);
        assertThat(estimate.getSize(output(operator)), closeTo(100.0, 0.0));
    }

    /**
     * join case.
     */
    @Test
    public void join() {
        Operator operator = OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "join")
                .input("m", typeOf(String.class))
                .input("t", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();

        OperatorEstimate estimate = estimate(context(), operator, Double.NaN, 100.0);
        assertThat(estimate.getSize(output(operator)), closeTo(100.0 * BuiltInOperatorEstimator.JOIN_SCALE, 1.0));
    }

    /**
     * complex case.
     */
    @Test
    public void complex() {
        Operator operator = OperatorExtractor.extract(Extract.class, Ops.class, "extract")
                .input("i", typeOf(String.class))
                .output("r", typeOf(String.class))
                .build();

        OperatorEstimate estimate = estimate(context(), operator, 100.0);
        assertThat(estimate.getSize(output(operator)), is(OperatorEstimate.UNKNOWN_SIZE));
    }

    /**
     * custom engine.
     */
    @Test
    public void custom() {
        Operator operator = OperatorExtractor.extract(Extract.class, Ops.class, "extract")
                .input("i", typeOf(String.class))
                .output("r", typeOf(String.class))
                .build();

        String key = BuiltInOperatorEstimator.PREFIX_KEY + Extract.class.getSimpleName();
        OperatorEstimate estimate = estimate(context(key, "10"), operator, 100.0);
        assertThat(estimate.getSize(output(operator)), closeTo(1000.0, 1.0));
    }

    private OperatorEstimate estimate(OptimizerContext context, Operator operator, double... inputSizes) {
        OperatorEstimatorAdapter adapter = new OperatorEstimatorAdapter(context);
        List<OperatorInput> inputs = operator.getInputs();
        assertThat(inputs, hasSize(inputSizes.length));
        for (int i = 0; i < inputSizes.length; i++) {
            OperatorInput input = inputs.get(i);
            MarkerOperator upstream = MarkerOperator.builder(input.getDataType()).build();
            BasicOperatorEstimate estimate = new BasicOperatorEstimate();
            estimate.putSize(upstream.getOutput(), inputSizes[i]);
            adapter.put(upstream, estimate);
            input.connect(upstream.getOutput());
        }
        adapter.apply(new BuiltInOperatorEstimator(), Collections.singleton(operator));
        return adapter.estimate(operator);
    }

    private OperatorOutput output(Operator operator) {
        return output(operator, 0);
    }

    private OperatorOutput output(Operator operator, int index) {
        return operator.getOutputs().get(index);
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @MasterJoinUpdate
        public abstract void join(String m, String t);

        @Extract
        public abstract void extract(String i, Result<String> r);
    }
}
