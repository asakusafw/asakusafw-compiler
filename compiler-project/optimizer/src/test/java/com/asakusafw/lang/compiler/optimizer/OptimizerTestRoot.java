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
package com.asakusafw.lang.compiler.optimizer;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader;
import com.asakusafw.lang.compiler.api.testing.VoidResourceContainer;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;
import com.asakusafw.lang.compiler.optimizer.testing.MockOptimizerContext;

/**
 * Test utilities for optimizers.
 */
public abstract class OptimizerTestRoot {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * Performs estimator.
     * @param context the current context
     * @param estimator the target estimator
     * @param operator the target operator
     * @return estimator's result
     */
    public static OperatorEstimate perform(OptimizerContext context, OperatorEstimator estimator, Operator operator) {
        return perform(new OperatorEstimatorAdapter(context), estimator, operator);
    }

    /**
     * Performs estimator.
     * @param context the current context
     * @param estimator the target estimator
     * @param operator the target operator
     * @return estimator's result
     */
    public static OperatorEstimate perform(
            OperatorEstimator.Context context, OperatorEstimator estimator, Operator operator) {
        context.apply(estimator, Collections.singleton(operator));
        return context.estimate(operator);
    }

    /**
     * Performs characterizer.
     * @param <T> the characteristics type
     * @param context the current context
     * @param engine the target engine
     * @param operator the target operator
     * @return the result
     */
    public static <T extends OperatorCharacteristics> T perform(
            OptimizerContext context, OperatorCharacterizer<? extends T> engine, Operator operator) {
        Map<Operator, T> results = OperatorCharacterizers.apply(
                context, OperatorEstimator.NULL,
                engine, Collections.singleton(operator));
        return results.get(operator);
    }

    /**
     * Creates a core operator.
     * @param kind the kind
     * @return the operator
     */
    public static Operator core(CoreOperatorKind kind) {
        return CoreOperator.builder(kind)
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
    }

    /**
     * Creates a user operator.
     * @param annotationType the annotation type
     * @return the operator
     */
    public static Operator user(ClassDescription annotationType) {
        AnnotationDescription annotation = new AnnotationDescription(annotationType);
        MethodDescription method = new MethodDescription(annotationType, "testing");
        return UserOperator.builder(annotation, method, annotationType)
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
    }

    /**
     * Creates an input operator.
     * @param module the module name
     * @return the operator
     */
    public static Operator input(String module) {
        return ExternalInput.newInstance(module, new ExternalInputInfo.Basic(
                clazz(module),
                module,
                classOf(String.class),
                DataSize.UNKNOWN));
    }

    /**
     * Creates an output operator.
     * @param module the module name
     * @return the operator
     */
    public static Operator output(String module) {
        return ExternalOutput.newInstance(module, new ExternalOutputInfo.Basic(
                clazz(module),
                module,
                classOf(String.class)));
    }

    /**
     * Creates a class description.
     * @param name the class name
     * @return the description
     */
    public static ClassDescription clazz(String name) {
        return new ClassDescription(name);
    }

    /**
     * Creates an {@link OptimizerContext}.
     * @param keyValuePairs the compiler option properties
     * @return the context
     */
    public MockOptimizerContext context(String... keyValuePairs) {
        assertThat(keyValuePairs.length % 2, is(0));
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            builder.withProperty(keyValuePairs[i + 0], keyValuePairs[i + 1]);
        }
        ClassLoader loader = getClass().getClassLoader();
        return new MockOptimizerContext(
                builder.build(),
                loader,
                new MockDataModelLoader(loader),
                new VoidResourceContainer());
    }
}
