/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OptimizerTestRoot;

/**
 * Test for {@link BasicExternalInputEstimator}.
 */
public class BasicExternalInputEstimatorTest extends OptimizerTestRoot {

    /**
     * tiny case.
     */
    @Test
    public void tiny() {
        Map<DataSize, Double> map = Collections.singletonMap(DataSize.TINY, 12.0);
        OperatorEstimator estimator = new BasicExternalInputEstimator(map);
        ExternalInput operator = of(DataSize.TINY);
        OperatorEstimate estimate = perform(context(), estimator, operator);

        assertThat(estimate.getSize(operator.getOperatorPort()), closeTo(12.0, 0.0));
    }

    /**
     * small case.
     */
    @Test
    public void small() {
        Map<DataSize, Double> map = Collections.singletonMap(DataSize.SMALL, 120.0);
        OperatorEstimator estimator = new BasicExternalInputEstimator(map);
        ExternalInput operator = of(DataSize.SMALL);
        OperatorEstimate estimate = perform(context(), estimator, operator);

        assertThat(estimate.getSize(operator.getOperatorPort()), closeTo(120.0, 0.0));
    }

    /**
     * large case.
     */
    @Test
    public void large() {
        Map<DataSize, Double> map = Collections.singletonMap(DataSize.LARGE, 1200.0);
        OperatorEstimator estimator = new BasicExternalInputEstimator(map);
        ExternalInput operator = of(DataSize.LARGE);
        OperatorEstimate estimate = perform(context(), estimator, operator);

        assertThat(estimate.getSize(operator.getOperatorPort()), closeTo(1200.0, 0.0));
    }

    /**
     * unknown case.
     */
    @Test
    public void unknown() {
        OperatorEstimator estimator = new BasicExternalInputEstimator();
        ExternalInput operator = of(DataSize.UNKNOWN);
        OperatorEstimate estimate = perform(context(), estimator, operator);

        assertThat(estimate.getSize(operator.getOperatorPort()), is(OperatorEstimate.UNKNOWN_SIZE));
    }

    private ExternalInput of(DataSize size) {
        return ExternalInput.newInstance("a", new ExternalInputInfo.Basic(
                clazz(size.name()),
                size.name(),
                classOf(String.class),
                size));
    }
}
