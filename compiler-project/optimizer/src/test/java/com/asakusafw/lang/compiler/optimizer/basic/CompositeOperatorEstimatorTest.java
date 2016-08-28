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
package com.asakusafw.lang.compiler.optimizer.basic;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.lang.annotation.Documented;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.optimizer.EngineBinding;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OptimizerTestRoot;

/**
 * Test for {@link CompositeOperatorEstimator}.
 */
public class CompositeOperatorEstimatorTest extends OptimizerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
            .withDefault(engine("ok"))
            .build();

        Operator operator = core(CoreOperatorKind.CHECKPOINT);
        OperatorEstimate estimate = perform(context(), estimator, operator);
        assertThat(estimate, hasMark("ok"));
    }

    /**
     * core operators.
     */
    @Test
    public void core() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withCore(CoreOperatorKind.PROJECT, engine("a"))
                .withCore(CoreOperatorKind.EXTEND, engine("b"))
                .build();

        Operator o0 = core(CoreOperatorKind.PROJECT);
        Operator o1 = core(CoreOperatorKind.EXTEND);
        Operator o2 = core(CoreOperatorKind.CHECKPOINT);

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);
        OperatorEstimate e2 = perform(context(), estimator, o2);

        assertThat(e0, hasMark("a"));
        assertThat(e1, hasMark("b"));
        assertThat(e2, hasMark(null));
    }

    /**
     * core operators specified by types.
     */
    @Test
    public void core_type() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withUser(clazz("com.asakusafw.vocabulary.operator.Project"), engine("a"))
                .withUser(clazz("com.asakusafw.vocabulary.operator.Extend"), engine("b"))
                .build();

        Operator o0 = core(CoreOperatorKind.PROJECT);
        Operator o1 = core(CoreOperatorKind.EXTEND);
        Operator o2 = core(CoreOperatorKind.CHECKPOINT);

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);
        OperatorEstimate e2 = perform(context(), estimator, o2);

        assertThat(e0, hasMark("a"));
        assertThat(e1, hasMark("b"));
        assertThat(e2, hasMark(null));
    }

    /**
     * user operators.
     */
    @Test
    public void user() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withUser(Deprecated.class, engine("a"))
                .withUser(Documented.class, engine("b"))
                .build();

        Operator o0 = user(classOf(Deprecated.class));
        Operator o1 = user(classOf(Documented.class));
        Operator o2 = user(clazz("Unknown"));

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);
        OperatorEstimate e2 = perform(context(), estimator, o2);

        assertThat(e0, hasMark("a"));
        assertThat(e1, hasMark("b"));
        assertThat(e2, hasMark(null));
    }

    /**
     * input operators.
     */
    @Test
    public void input() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withInput(engine("defaults"))
                .withInput("a", engine("A"))
                .withInput("b", engine("B"))
                .build();

        Operator o0 = input("a");
        Operator o1 = input("b");
        Operator o2 = input("c");

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);
        OperatorEstimate e2 = perform(context(), estimator, o2);

        assertThat(e0, hasMark("A"));
        assertThat(e1, hasMark("B"));
        assertThat(e2, hasMark("defaults"));
    }

    /**
     * output operators.
     */
    @Test
    public void output() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withOutput(engine("defaults"))
                .withOutput("a", engine("A"))
                .withOutput("b", engine("B"))
                .build();

        Operator o0 = output("a");
        Operator o1 = output("b");
        Operator o2 = output("c");

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);
        OperatorEstimate e2 = perform(context(), estimator, o2);

        assertThat(e0, hasMark("A"));
        assertThat(e1, hasMark("B"));
        assertThat(e2, hasMark("defaults"));
    }

    /**
     * custom operators.
     */
    @Test
    public void custom() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withCustom("mock", engine("a"))
                .build();

        Operator o0 = custom("mock");
        Operator o1 = custom("other");

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);

        assertThat(e0, hasMark("a"));
        assertThat(e1, hasMark(null));
    }

    /**
     * bindings.
     */
    @Test
    public void binding() {
        OperatorEstimator estimator = CompositeOperatorEstimator.builder()
                .withBinding(new MockBinding())
                .build();
        doCheck(estimator);
    }

    /**
     * loads bindings.
     * @throws Exception if failed
     */
    @Test
    public void load() throws Exception {
        File cp = temporary.newFolder();
        File service = new File(cp, "META-INF/services/" + MockBindingSpi.class.getName());
        FileEditor.put(service, MockBinding.class.getName());
        OperatorEstimator estimator;
        try (URLClassLoader loader = URLClassLoader.newInstance(new URL[] { cp.toURI().toURL() } )) {
            estimator = CompositeOperatorEstimator.builder()
                    .load(loader, MockBindingSpi.class)
                    .build();
        }
        doCheck(estimator);
    }

    private void doCheck(OperatorEstimator estimator) {
        Operator o0 = user(clazz("a"));
        Operator o1 = user(clazz("b"));
        Operator o2 = input("b");
        Operator o3 = input("c");
        Operator o4 = output("c");
        Operator o5 = output("d");
        Operator o6 = custom("d");
        Operator o7 = custom("e");

        OperatorEstimate e0 = perform(context(), estimator, o0);
        OperatorEstimate e1 = perform(context(), estimator, o1);
        OperatorEstimate e2 = perform(context(), estimator, o2);
        OperatorEstimate e3 = perform(context(), estimator, o3);
        OperatorEstimate e4 = perform(context(), estimator, o4);
        OperatorEstimate e5 = perform(context(), estimator, o5);
        OperatorEstimate e6 = perform(context(), estimator, o6);
        OperatorEstimate e7 = perform(context(), estimator, o7);

        assertThat(e0, hasMark("binding"));
        assertThat(e1, hasMark(null));
        assertThat(e2, hasMark("binding"));
        assertThat(e3, hasMark(null));
        assertThat(e4, hasMark("binding"));
        assertThat(e5, hasMark(null));
        assertThat(e6, hasMark("binding"));
        assertThat(e7, hasMark(null));
    }

    static OperatorEstimator engine(String mark) {
        return (context, operator) -> context.putAttribute(String.class, mark);
    }

    static Matcher<OperatorEstimate> hasMark(String mark) {
        Matcher<String> matcher = (mark == null) ? nullValue(String.class) : is(mark);
        return new FeatureMatcher<OperatorEstimate, String>(matcher, "has mark", "hask mark") {
            @Override
            protected String featureValueOf(OperatorEstimate actual) {
                return actual.getAttribute(String.class);
            }
        };
    }

    @SuppressWarnings("javadoc")
    public abstract static class MockBindingSpi extends EngineBinding.Abstract<OperatorEstimator> {
        // no members
    }

    @SuppressWarnings("javadoc")
    public static class MockBinding extends MockBindingSpi {

        @Override
        public Collection<ClassDescription> getTargetOperators() {
            return Arrays.asList(new ClassDescription("a"));
        }

        @Override
        public Collection<String> getTargetInputs() {
            return Arrays.asList("b");
        }

        @Override
        public Collection<String> getTargetOutputs() {
            return Arrays.asList("c");
        }

        @Override
        public Collection<String> getTargetCategories() {
            return Arrays.asList("d");
        }

        @Override
        public OperatorEstimator getEngine() {
            return engine("binding");
        }
    }
}
