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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacteristics;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizers;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OptimizerTestRoot;

/**
 * Test for {@link CompositeOperatorCharacterizer}.
 */
public class CompositeOperatorCharacterizerTest extends OptimizerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        OperatorCharacterizer<? extends Mock> engine = CompositeOperatorCharacterizer.<Mock>builder()
                .withDefault(engine("ok"))
                .build();
        OperatorEstimator estimator = OperatorEstimator.NULL;
        Operator o0 = user(clazz("o0"));
        Map<Operator, Mock> map = OperatorCharacterizers.apply(context(), estimator, engine, ops(o0));
        assertThat(map.keySet(), hasSize(1));
        assertThat(map, hasEntry(is(o0), hasMark("ok")));
    }

    /**
     * core operators.
     */
    @Test
    public void core() {
        OperatorCharacterizer<? extends Mock> engine = CompositeOperatorCharacterizer.<Mock>builder()
                .withDefault(engine("miss"))
                .withCore(CoreOperatorKind.PROJECT, engine("ok"))
                .build();
        OperatorEstimator estimator = OperatorEstimator.NULL;
        Operator o0 = core(CoreOperatorKind.PROJECT);
        Operator o1 = core(CoreOperatorKind.EXTEND);
        Operator o2 = user(clazz("user"));

        Map<Operator, Mock> map = OperatorCharacterizers.apply(context(), estimator, engine, ops(o0, o1, o2));
        assertThat(map.keySet(), hasSize(3));
        assertThat(map, hasEntry(is(o0), hasMark("ok")));
        assertThat(map, hasEntry(is(o1), hasMark("miss")));
        assertThat(map, hasEntry(is(o2), hasMark("miss")));
    }

    /**
     * missing characterizer.
     */
    @Test(expected = DiagnosticException.class)
    public void missing() {
        OperatorCharacterizer<? extends Mock> engine = CompositeOperatorCharacterizer.<Mock>builder()
                .build();
        OperatorEstimator estimator = OperatorEstimator.NULL;
        Operator o0 = user(clazz("o0"));
        OperatorCharacterizers.apply(context(), estimator, engine, ops(o0));
    }

    private static List<Operator> ops(Operator... operators) {
        return Arrays.asList(operators);
    }

    static OperatorCharacterizer<Mock> engine(final String mark) {
        return new OperatorCharacterizer<Mock>() {
            @Override
            public Mock extract(Context context, Operator operator) {
                return new Mock(mark);
            }
        };
    }

    static Matcher<Mock> hasMark(String mark) {
        Matcher<String> matcher = (mark == null) ? nullValue(String.class) : is(mark);
        return new FeatureMatcher<Mock, String>(matcher, "has mark", "hask mark") {
            @Override
            protected String featureValueOf(Mock actual) {
                return actual.value;
            }
        };
    }

    @SuppressWarnings("javadoc")
    public static class Mock implements OperatorCharacteristics {

        final String value;

        /**
         * Creates a new instance.
         * @param value the characteristics
         */
        public Mock(String value) {
            this.value = value;
        }

        /**
         * Returns the value.
         * @return the value
         */
        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
