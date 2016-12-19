/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacteristics;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizers;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriters;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.adapter.OptimizerContextAdapter;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOptimizers;

/**
 * Testing utilities of optimizers for built-in operators.
 */
public abstract class BuiltInOptimizerTestRoot {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * Creates an {@link OptimizerContext}.
     * @param keyValuePairs the compiler option properties
     * @return the context
     */
    public OptimizerContext context(String... keyValuePairs) {
        assertThat(keyValuePairs.length % 2, is(0));
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            builder.withProperty(keyValuePairs[i + 0], keyValuePairs[i + 1]);
        }
        JobflowProcessor.Context context = new MockJobflowProcessorContext(
                builder.build(),
                getClass().getClassLoader(),
                temporary.getRoot());
        return new OptimizerContextAdapter(context, "testing");
    }

    /**
     * Applies {@link OperatorRewriter}.
     * @param context the current context
     * @param engine the target engine
     * @param graph the target graph
     */
    public void apply(OptimizerContext context, OperatorRewriter engine, OperatorGraph graph) {
        OperatorRewriters.apply(
                context,
                BasicOptimizers.getDefaultEstimator(getClass().getClassLoader()).build(),
                engine,
                graph);
    }

    /**
     * Applies {@link OperatorCharacterizer}.
     * @param <T> the characteristic type
     * @param context the current context
     * @param engine the target engine
     * @param operator the target operator
     * @return the result
     */
    public <T extends OperatorCharacteristics> T apply(
            OptimizerContext context, OperatorCharacterizer<T> engine, Operator operator) {
        Map<Operator, T> results = OperatorCharacterizers.apply(
                context,
                BasicOptimizers.getDefaultEstimator(getClass().getClassLoader()).build(),
                engine,
                Collections.singleton(operator));
        return results.get(operator);
    }

    /**
     * Connects straight operators.
     * @param line the operator line
     * @return the created graph
     */
    public OperatorGraph connect(Operator... line) {
        Operator last = line[0];
        for (int i = 1; i < line.length; i++) {
            Operator current = line[i];
            assert last.getOutputs().size() == 1;
            assert current.getInputs().size() == 1;
            last.getOutput(0).connect(current.getInput(0));
            last = current;
        }
        return new OperatorGraph(Arrays.asList(line));
    }

    /**
     * Returns a matcher whether the operator graph contains the target operator.
     * @param methodName the target method name
     * @return the matcher
     */
    public Matcher<OperatorGraph> hasOperator(String methodName) {
        return new BaseMatcher<OperatorGraph>() {
            @Override
            public boolean matches(Object item) {
                OperatorGraph graph = (OperatorGraph) item;
                for (Operator operator : graph.getOperators(false)) {
                    if (operator.getOperatorKind() != OperatorKind.USER) {
                        continue;
                    }
                    UserOperator user = (UserOperator) operator;
                    if (user.getMethod().getName().equals(methodName)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("contains ").appendValue(methodName);
            }
        };
    }

    /**
     * Returns a matcher whether the operator graph contains the target operator.
     * @param kind the target operator kind
     * @return the matcher
     */
    public Matcher<OperatorGraph> hasOperator(CoreOperatorKind kind) {
        return new BaseMatcher<OperatorGraph>() {
            @Override
            public boolean matches(Object item) {
                OperatorGraph graph = (OperatorGraph) item;
                for (Operator operator : graph.getOperators(false)) {
                    if (operator.getOperatorKind() != OperatorKind.CORE) {
                        continue;
                    }
                    CoreOperator core = (CoreOperator) operator;
                    if (core.getCoreOperatorKind() == kind) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("contains ").appendValue(kind);
            }
        };
    }
}
