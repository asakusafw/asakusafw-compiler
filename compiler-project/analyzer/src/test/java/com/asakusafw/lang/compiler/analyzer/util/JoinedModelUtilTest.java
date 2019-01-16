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
package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.vocabulary.model.Joined;
import com.asakusafw.vocabulary.model.Key;
import com.asakusafw.vocabulary.operator.MasterJoin;
import com.asakusafw.vocabulary.operator.Split;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link JoinedModelUtil}.
 */
public class JoinedModelUtilTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /**
     * simple case for master join operator.
     * @throws Exception if failed
     */
    @Test
    public void simple_master_join() throws Exception {
        Operator operator = newJoin();
        assertThat(JoinedModelUtil.isSupported(operator), is(true));

        List<PropertyMapping> mappings = JoinedModelUtil.getPropertyMappings(cl, operator);
        assertThat(mappings, hasSize(3));

        OperatorInput left = operator.getInput(0);
        OperatorInput right = operator.getInput(1);
        OperatorOutput joined = operator.getOutput(0);
        PropertyMapping mk = find("k", mappings);
        PropertyMapping mv0 = find("v0", mappings);
        PropertyMapping mv1 = find("v1", mappings);

        assertThat(mk.getSourcePort(), anyOf(is(left), is(right)));
        assertThat(mv0.getSourcePort(), is(left));
        assertThat(mv1.getSourcePort(), is(right));

        assertThat(mk.getSourceProperty(), anyOf(is(PropertyName.of("k0")), is(PropertyName.of("k1"))));
        assertThat(mv0.getSourceProperty(), is(PropertyName.of("v0")));
        assertThat(mv1.getSourceProperty(), is(PropertyName.of("v1")));

        assertThat(mk.getDestinationPort(), is(joined));
        assertThat(mv0.getDestinationPort(), is(joined));
        assertThat(mv1.getDestinationPort(), is(joined));
    }

    /**
     * simple case for split operator.
     * @throws Exception if failed
     */
    @Test
    public void simple_split() throws Exception {
        Operator operator = newSplit();
        assertThat(JoinedModelUtil.isSupported(operator), is(true));

        List<PropertyMapping> mappings = JoinedModelUtil.getPropertyMappings(cl, operator);
        assertThat(mappings, hasSize(4));

        OperatorInput joined = operator.getInput(0);
        OperatorOutput left = operator.getOutput(0);
        OperatorOutput right = operator.getOutput(1);

        PropertyMapping mk0 = find("k0", mappings);
        PropertyMapping mk1 = find("k1", mappings);
        PropertyMapping mv0 = find("v0", mappings);
        PropertyMapping mv1 = find("v1", mappings);

        assertThat(mk0.getSourcePort(), is(joined));
        assertThat(mk1.getSourcePort(), is(joined));
        assertThat(mv0.getSourcePort(), is(joined));
        assertThat(mv1.getSourcePort(), is(joined));

        assertThat(mk0.getDestinationPort(), is(left));
        assertThat(mk1.getDestinationPort(), is(right));
        assertThat(mv0.getDestinationPort(), is(left));
        assertThat(mv1.getDestinationPort(), is(right));

        assertThat(mk0.getDestinationProperty(), is(PropertyName.of("k0")));
        assertThat(mk0.getDestinationProperty(), is(PropertyName.of("k0")));
        assertThat(mv0.getDestinationProperty(), is(PropertyName.of("v0")));
        assertThat(mv1.getDestinationProperty(), is(PropertyName.of("v1")));
    }

    /**
     * supported.
     */
    @Test
    public void supported_model() {
        assertThat(JoinedModelUtil.isSupported(Left.class), is(false));
        assertThat(JoinedModelUtil.isSupported(Right.class), is(false));
        assertThat(JoinedModelUtil.isSupported(J0.class), is(true));
    }

    /**
     * supported.
     */
    @Test
    public void supported_operator() {
        Operator m0 = newJoin();
        Operator m1 = newSplit();
        Operator m2 = newUpdate();
        CoreOperator m3 = newCheckpoint();

        assertThat(JoinedModelUtil.isSupported(m0), is(true));
        assertThat(JoinedModelUtil.isSupported(m1), is(true));
        assertThat(JoinedModelUtil.isSupported(m2), is(false));
        assertThat(JoinedModelUtil.isSupported(m3), is(false));
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        JoinedModelUtil.getPropertyMappings(cl, newCheckpoint());
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_not_joined() throws Exception {
        JoinedModelUtil.getPropertyMappings(cl, newInvalid());
    }

    private PropertyMapping find(String destination, List<PropertyMapping> mappings) {
        PropertyName name = PropertyName.of(destination);
        for (PropertyMapping mapping : mappings) {
            if (mapping.getDestinationProperty().equals(name)) {
                return mapping;
            }
        }
        throw new AssertionError(destination);
    }

    private Operator newUpdate() {
        return OperatorExtractor.extract(Update.class, Ops.class, "update")
                .input("in", classOf(J0.class))
                .output("out", classOf(J0.class))
                .build();
    }

    private Operator newJoin() {
        return OperatorExtractor.extract(MasterJoin.class, Ops.class, "join")
                .input("left", classOf(Left.class))
                .input("right", classOf(Right.class))
                .output("joined", classOf(J0.class))
                .output("missed", classOf(Right.class))
                .build();
    }

    private Operator newSplit() {
        return OperatorExtractor.extract(Split.class, Ops.class, "split")
                .input("joined", classOf(J0.class))
                .output("left", classOf(Left.class))
                .output("right", classOf(Right.class))
                .build();
    }

    private Operator newInvalid() {
        return OperatorExtractor.extract(MasterJoin.class, Ops.class, "invalid")
                .input("left", classOf(Left.class))
                .input("right", classOf(Right.class))
                .output("joined", classOf(String.class))
                .output("missed", classOf(Right.class))
                .build();
    }

    private CoreOperator newCheckpoint() {
        return CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();
    }

    private static class Left {
        // no members
    }

    private static class Right {
        // no members
    }

    @Joined(terms = {
            @Joined.Term(source = Left.class, shuffle = @Key(group = {}), mappings = {
                @Joined.Mapping(source = "k0", destination = "k"),
                @Joined.Mapping(source = "v0", destination = "v0"),
            }),
            @Joined.Term(source = Right.class, shuffle = @Key(group = {}), mappings = {
                @Joined.Mapping(source = "k1", destination = "k"),
                @Joined.Mapping(source = "v1", destination = "v1"),
            })
    })
    private static class J0 {
        // no members
    }

    private abstract static class Ops {

        @Update
        public void update(J0 model) {
            return;
        }

        @MasterJoin
        public abstract J0 join(Left left, Right right);

        @Split
        public abstract void split(J0 joined, Left left, Right right);

        @MasterJoin
        public abstract String invalid(Left left, Right right);
    }
}
