package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Test for {@link OperatorUtil}.
 */
public class OperatorUtilTest {

    /**
     * check ports.
     */
    @Test
    public void check_ports() {
        Operator operator = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
            .input("p", typeOf(String.class))
            .output("p", typeOf(Integer.class))
            .build();

        // ok
        OperatorUtil.checkOperatorPorts(operator, 1, 1);

        try {
            OperatorUtil.checkOperatorPorts(operator, 2, 1);
            fail();
        } catch (IllegalStateException e) {
            // ok
        }

        try {
            OperatorUtil.checkOperatorPorts(operator, 1, 2);
            fail();
        } catch (IllegalStateException e) {
            // ok
        }
    }

    /**
     * collect data types.
     */
    @Test
    public void collect_data_types() {
        Set<Operator> operators = new LinkedHashSet<>();
        operators.add(CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
            .input("p", typeOf(String.class))
            .output("p", typeOf(Integer.class))
            .build());

        Set<TypeDescription> results = OperatorUtil.collectDataTypes(operators);
        assertThat(results, containsInAnyOrder((TypeDescription) typeOf(String.class), typeOf(Integer.class)));
    }
}
