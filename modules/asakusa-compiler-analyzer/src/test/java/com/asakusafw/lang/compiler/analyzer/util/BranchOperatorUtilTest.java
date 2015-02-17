package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.Branch;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link BranchOperatorUtil}.
 */
public class BranchOperatorUtilTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        UserOperator m0 = extract(Branch.class, Ops.class, "m0");
        assertThat(BranchOperatorUtil.isSupported(m0), is(true));
        Map<Enum<?>, OperatorOutput> o0 = rev(BranchOperatorUtil.getOutputMap(cl, m0));
        assertThat(o0.keySet(), containsInAnyOrder((Enum<?>[]) State.values()));
        assertThat(o0.get(State.A).getName(), is("a"));
        assertThat(o0.get(State.B).getName(), is("b"));
        assertThat(o0.get(State.C).getName(), is("c"));
        assertThat(o0.get(State.VERY_LONG_NAME_CONSTANT_VALUE).getName(), is("veryLongNameConstantValue"));
    }

    /**
     * check supports.
     * @throws Exception if failed
     */
    @Test
    public void support() throws Exception {
        UserOperator m0 = extract(Branch.class, Ops.class, "m0");
        UserOperator m1 = extract(MasterBranch.class, Ops.class, "m1");
        UserOperator m2 = extract(Update.class, Ops.class, "m2");
        CoreOperator m3 = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();

        assertThat(BranchOperatorUtil.isSupported(m0), is(true));
        assertThat(BranchOperatorUtil.isSupported(m1), is(true));
        assertThat(BranchOperatorUtil.isSupported(m2), is(false));
        assertThat(BranchOperatorUtil.isSupported(m3), is(false));
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        BranchOperatorUtil.getOutputMap(cl, CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build());
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_not_enum() throws Exception {
        BranchOperatorUtil.getOutputMap(cl, extract(Branch.class, Ops.class, "m3"));
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_conflict() throws Exception {
        UserOperator operator = OperatorExtractor.extract(Branch.class, Ops.class, "m0")
                .input("in", typeOf(String.class))
                .output("a", typeOf(String.class))
                .output("b", typeOf(String.class))
                .output("c", typeOf(String.class))
                .output("veryLongNameConstantValue", typeOf(String.class))
                .output("C", typeOf(String.class)) // conflict
                .build();
        BranchOperatorUtil.getOutputMap(cl, operator);
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_missing_constant() throws Exception {
        UserOperator operator = OperatorExtractor.extract(Branch.class, Ops.class, "m0")
                .input("in", typeOf(String.class))
                .output("a", typeOf(String.class))
                .output("b", typeOf(String.class))
                .output("c", typeOf(String.class))
                .output("d", typeOf(String.class))
                .output("veryLongNameConstantValue", typeOf(String.class))
                .build();
        BranchOperatorUtil.getOutputMap(cl, operator);
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_missing_output() throws Exception {
        UserOperator operator = OperatorExtractor.extract(Branch.class, Ops.class, "m0")
                .input("in", typeOf(String.class))
                .output("a", typeOf(String.class))
                .output("b", typeOf(String.class))
                .output("veryLongNameConstantValue", typeOf(String.class))
                .build();
        BranchOperatorUtil.getOutputMap(cl, operator);
    }

    private UserOperator extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName) {
        UserOperator.Builder builder = OperatorExtractor.extract(annotationType, operatorClass, methodName)
                .input("in", typeOf(String.class));
        for (State value : State.values()) {
            String name = PropertyName.of(value.name()).toMemberName();
            builder.output(name, typeOf(String.class));
        }
        return builder.build();
    }

    private Map<Enum<?>, OperatorOutput> rev(Map<OperatorOutput, Enum<?>> outputs) {
        Map<Enum<?>, OperatorOutput> results = new HashMap<>();
        for (Map.Entry<OperatorOutput, Enum<?>> entry : outputs.entrySet()) {
            assertThat(results.get(entry.getValue()), is(nullValue()));
            results.put(entry.getValue(), entry.getKey());
        }
        return results;
    }

    @SuppressWarnings("javadoc")
    public enum State {

        A,

        B,

        C,

        VERY_LONG_NAME_CONSTANT_VALUE,
    }

    @SuppressWarnings("javadoc")
    public static abstract class Ops {

        @Branch
        public abstract State m0();

        @MasterBranch
        public abstract State m1();

        @Update
        public abstract State m2();

        @Branch
        public abstract void m3();
    }
}
