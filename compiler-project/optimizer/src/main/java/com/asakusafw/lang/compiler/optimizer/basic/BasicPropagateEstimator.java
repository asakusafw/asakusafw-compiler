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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.List;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimators;

/**
 * Propagates estimated data size.
 */
public class BasicPropagateEstimator implements OperatorEstimator {

    private final int inputIndex;

    private final double scale;

    /**
     * Creates a new instance.
     */
    public BasicPropagateEstimator() {
        this(0, 1.0);
    }

    /**
     * Creates a new instance.
     * @param inputIndex the input index
     * @param scale the multiplying factor while propagating input size into outputs
     */
    public BasicPropagateEstimator(int inputIndex, double scale) {
        this.inputIndex = inputIndex;
        this.scale = scale;
    }

    @Override
    public void perform(Context context, Operator operator) {
        List<OperatorInput> inputs = operator.getInputs();
        if (inputs.size() >= inputIndex) {
            double size = OperatorEstimators.getSize(context, inputs.get(inputIndex));
            if (Double.isNaN(size) == false) {
                OperatorEstimators.putSize(context, operator, size * scale);
            }
        }
    }
}
