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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.optimizer.OptimizerTestRoot;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;

/**
 * Test for {@link BasicPropagateEstimator}.
 */
public class BasicPropagateEstimatorTest extends OptimizerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b").connect("a", "b");

        OperatorEstimatorAdapter context = new OperatorEstimatorAdapter(context());
        perform(context, new BasicConstantEstimator(10.0), mock.get("a"));
        perform(context, new BasicPropagateEstimator(), mock.get("b"));

        assertThat(context.estimate(mock.get("b")).getSize(mock.getOutput("b")), closeTo(10.0, 0.0));
    }
}
