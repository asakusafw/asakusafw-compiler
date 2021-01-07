/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimators;

/**
 * Provides constant data size.
 */
public class BasicConstantEstimator implements OperatorEstimator {

    private final double size;

    /**
     * Creates a new instance.
     * @param size the constant data size
     */
    public BasicConstantEstimator(double size) {
        this.size = size;
    }

    @Override
    public void perform(Context context, Operator operator) {
        OperatorEstimators.putSize(context, operator, size);
    }
}
