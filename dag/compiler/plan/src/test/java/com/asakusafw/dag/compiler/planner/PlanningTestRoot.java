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
package com.asakusafw.dag.compiler.planner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.dag.compiler.model.plan.InputSpec;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputOption;
import com.asakusafw.dag.compiler.model.plan.OutputSpec;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * A common test base class for planning.
 */
public abstract class PlanningTestRoot {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * Returns a set of values.
     * @param <T> value type
     * @param values the elements
     * @return the set
     */
    @SafeVarargs
    public static <T> Set<T> set(T... values) {
        return Stream.of(values).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns a matcher whether the operator graph just has the specified operators.
     * @param id operator ID
     * @return the matcher
     */
    public static Matcher<? super Operator> isOperator(String id) {
        return new FeatureMatcher<Operator, String>(equalTo(id), "is operator", "operator") {
            @Override
            protected String featureValueOf(Operator actual) {
                return MockOperators.getId(actual);
            }
        };
    }

    /**
     * Returns a matcher whether the operator graph just has the specified operators.
     * @param ids operator IDs
     * @return the matcher
     */
    public static Matcher<Collection<? extends Operator>> hasOperators(String... ids) {
        return new FeatureMatcher<Collection<? extends Operator>, Set<String>>(
                equalTo(set(ids)), "has operators", "operators") {
            @Override
            protected Set<String> featureValueOf(Collection<? extends Operator> actual) {
                MockOperators mock = new MockOperators(actual);
                Set<String> results = new HashSet<>();
                for (Operator operator : mock.all()) {
                    results.add(mock.id(operator));
                }
                return results;
            }
        };
    }

    /**
     * Returns the unique owner sub-plan from the source operator.
     * @param detail the plan detail
     * @param source the source operator
     * @return the unique sub-plan which is owner of a copy of the target source
     */
    public static SubPlan ownerOf(PlanDetail detail, Operator source) {
        Set<SubPlan> candidates = ownersOf(detail, Collections.singleton(source));
        assertThat(candidates, hasSize(1));
        return candidates.iterator().next();
    }

    /**
     * Returns the unique owner sub-plan from the source operator.
     * @param detail the plan detail
     * @param sources the source operators
     * @return the unique sub-plan which is owner of a copy of the target source
     */
    public static Set<SubPlan> ownersOf(PlanDetail detail, Collection<? extends Operator> sources) {
        Set<Operator> copies = new HashSet<>();
        for (Operator source : sources) {
            copies.addAll(detail.getCopies(source));
        }
        assertThat(copies, is(not(empty())));
        Set<SubPlan> results = new HashSet<>();
        for (Operator copy : copies) {
            results.add(detail.getOwner(copy));
        }
        return results;
    }

    /**
     * Returns the unique owner sub-plan from the source operator.
     * @param detail the plan detail
     * @param inputs the source input operators
     * @param outputs the source outputs operators
     * @return the unique sub-plan which is owner of a copy of the target source
     */
    public static SubPlan ownerOf(
            PlanDetail detail,
            Collection<? extends Operator> inputs,
            Collection<? extends Operator> outputs) {
        Set<Operator> copyInputs = allCopiesOf(detail, inputs);
        Set<Operator> copyOutputs = allCopiesOf(detail, outputs);
        Set<SubPlan> candidates = new HashSet<>();
        for (Operator copy : copyInputs) {
            SubPlan owner = detail.getOwner(copy);
            if (copyInputs.containsAll(toOperators(owner.getInputs()))
                    && copyOutputs.containsAll(toOperators(owner.getOutputs()))) {
                candidates.add(owner);
            }
        }
        assertThat(candidates, hasSize(1));
        return candidates.iterator().next();
    }

    private static Set<Operator> allCopiesOf(PlanDetail detail, Collection<? extends Operator> sources) {
        return sources.stream().flatMap(o -> detail.getCopies(o).stream()).collect(Collectors.toSet());
    }

    /**
     * Returns the operators in the sub-plan ports.
     * @param ports the sub-plan ports
     * @return the operators
     */
    public static Set<MarkerOperator> toOperators(Collection<? extends SubPlan.Port> ports) {
        return ports.stream().map(SubPlan.Port::getOperator).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns the successors of the sub-plan.
     * @param vertex the sub-plan
     * @return the successors
     */
    public static Set<SubPlan> succ(SubPlan vertex) {
        return vertex.getOutputs().stream()
                .flatMap(p -> p.getOpposites().stream())
                .map(SubPlan.Input::getOwner)
                .collect(Collectors.toSet());
    }

    /**
     * Returns the predecessors of the sub-plan.
     * @param vertex the sub-plan
     * @return the predecessors
     */
    public static Set<SubPlan> pred(SubPlan vertex) {
        return vertex.getInputs().stream()
                .flatMap(p -> p.getOpposites().stream())
                .map(SubPlan.Output::getOwner)
                .collect(Collectors.toSet());
    }

    /**
     * Creates an {@link PlanningContext}.
     * @param keyValuePairs the compiler option properties
     * @return the context
     */
    public PlanningContext context(String... keyValuePairs) {
        assertThat(keyValuePairs.length % 2, is(0));
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            builder.withProperty(keyValuePairs[i + 0], keyValuePairs[i + 1]);
        }
        JobflowProcessor.Context context = new MockJobflowProcessorContext(
                builder.build(),
                getClass().getClassLoader(),
                temporary.getRoot());
        return DagPlanning.createContext(
                context,
                new JobflowInfo.Basic("testing", new ClassDescription("testing")));
    }

    /**
     * Restores the {@link MockOperators}.
     * @param detail the plan detail
     * @return the restored one
     */
    public static MockOperators restore(PlanDetail detail) {
        Set<Operator> managed = new HashSet<>();
        for (Operator operator : detail.getSources()) {
            if (MockOperators.getId(operator) != null) {
                managed.add(operator);
            }
        }
        return new MockOperators(detail.getSources().stream()
                .filter(o -> Objects.nonNull(MockOperators.getId(o)))
                .collect(Collectors.toSet()));
    }

    /**
     * Returns the singular input.
     * @param sub the target sub-plan
     * @return the singular input
     */
    public static SubPlan.Input input(SubPlan sub) {
        Set<? extends SubPlan.Input> inputs = sub.getInputs();
        assertThat(inputs, hasSize(1));
        return inputs.iterator().next();
    }

    /**
     * Returns the singular primary input.
     * @param sub the target sub-plan
     * @return the singular input
     */
    public static SubPlan.Input primary(SubPlan sub) {
        List<? extends SubPlan.Input> inputs = sub.getInputs().stream()
                .filter(p -> info(p).getInputOptions().contains(InputOption.PRIMARY))
                .collect(Collectors.toList());
        assertThat(inputs, hasSize(1));
        return inputs.get(0);
    }

    /**
     * Returns the singular secondary input.
     * @param sub the target sub-plan
     * @return the singular input
     */
    public static SubPlan.Input secondary(SubPlan sub) {
        List<? extends SubPlan.Input> inputs = sub.getInputs().stream()
                .filter(p -> info(p).getInputOptions().contains(InputOption.PRIMARY) == false)
                .collect(Collectors.toList());
        assertThat(inputs, hasSize(1));
        return inputs.get(0);
    }

    /**
     * Returns the singular output.
     * @param sub the target sub-plan
     * @return the singular output
     */
    public static SubPlan.Output output(SubPlan sub) {
        Set<? extends SubPlan.Output> outputs = sub.getOutputs();
        assertThat(outputs, hasSize(1));
        return outputs.iterator().next();
    }

    /**
     * Creates a {@link Group}.
     * @param values the expressions
     * @return the created object
     */
    public static Group group(String... values) {
        return Groups.parse(values);
    }

    /**
     * Returns the spec.
     * @param container the target element
     * @return the spec
     */
    public static VertexSpec info(SubPlan container) {
        VertexSpec info = container.getAttribute(VertexSpec.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }

    /**
     * Returns the spec.
     * @param container the target element
     * @return the spec
     */
    public static InputSpec info(SubPlan.Input container) {
        InputSpec info = container.getAttribute(InputSpec.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }

    /**
     * Returns the spec.
     * @param container the target element
     * @return the spec
     */
    public static OutputSpec info(SubPlan.Output container) {
        OutputSpec info = container.getAttribute(OutputSpec.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }
}