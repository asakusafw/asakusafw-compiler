/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.planning;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.testing.MockOperators;

/**
 * A common test base class for planning.
 */
public abstract class PlanningTestRoot {

    /**
     * Returns a set of values.
     * @param <T> value type
     * @param values the elements
     * @return the set
     */
    @SafeVarargs
    public static <T> Set<T> set(T... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
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
     * Returns a matcher whether the operator graph just has the specified operators.
     * @param ids operator IDs
     * @return the matcher
     */
    public static Matcher<OperatorGraph> hasOperatorGraph(String... ids) {
        return new FeatureMatcher<OperatorGraph, Set<String>>(equalTo(set(ids)), "has operators", "operators") {
            @Override
            protected Set<String> featureValueOf(OperatorGraph actual) {
                MockOperators mock = new MockOperators(actual.getOperators());
                Set<String> results = new HashSet<>();
                for (Operator operator : mock.all()) {
                    results.add(mock.id(operator));
                }
                return results;
            }
        };
    }

    /**
     * Returns a matcher whether the operator has a constraint.
     * @param constraint the constraint
     * @return the matcher
     */
    public static Matcher<Operator> hasConstraint(OperatorConstraint constraint) {
        return new FeatureMatcher<Operator, Set<OperatorConstraint>>(hasItem(constraint), "has constraint", "constraint") {
            @Override
            protected Set<OperatorConstraint> featureValueOf(Operator actual) {
                return actual.getConstraints();
            }
        };
    }

    /**
     * Returns a matcher whether the operator is the plan marker.
     * @param marker the marker type
     * @return the matcher
     */
    public static Matcher<Operator> hasMarker(PlanMarker marker) {
        return new FeatureMatcher<Operator, PlanMarker>(equalTo(marker), "has marker", "marker") {
            @Override
            protected PlanMarker featureValueOf(Operator actual) {
                return PlanMarkers.get(actual);
            }
        };
    }

    /**
     * Returns a predicate whether the operator has a constraint.
     * @param constraint the constraint
     * @return the predicate
     */
    public static Predicate<Operator> only(OperatorConstraint constraint) {
        return operator -> operator.getConstraints().contains(constraint);
    }

    /**
     * Returns only plan markers.
     * @param marker the target marker kind
     * @param operators the target operator
     * @return the filtered operators
     */
    public static Set<Operator> only(PlanMarker marker, Collection<? extends Operator> operators) {
        Set<Operator> results = new LinkedHashSet<>();
        for (Operator operator : operators) {
            if (PlanMarkers.get(operator) == marker) {
                results.add(operator);
            }
        }
        return results;
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
        Set<Operator> results = new HashSet<>();
        for (Operator source : sources) {
            results.addAll(detail.getCopies(source));
        }
        return results;
    }

    /**
     * Returns the unique copy operator from the source.
     * @param detail the plan detail
     * @param source the source operator
     * @return the unique copy of the source
     */
    public static Operator copyOf(PlanDetail detail, Operator source) {
        Set<Operator> copies = detail.getCopies(source);
        assertThat(copies, hasSize(1));
        return copies.iterator().next();
    }

    /**
     * Returns a copy operator of the source.
     * @param detail the plan detail
     * @param owner the owner of the target copy
     * @param source the source operator
     * @return the copy of the source
     */
    public static Operator copyOf(PlanDetail detail, SubPlan owner, Operator source) {
        Set<Operator> copies = detail.getCopies(source, owner);
        assertThat(copies, hasSize(1));
        return copies.iterator().next();
    }

    /**
     * Returns sources of the copied operators.
     * @param detail the detail
     * @param operators the copies
     * @return the related sources
     */
    public static Set<Operator> toSources(PlanDetail detail, Collection<? extends Operator> operators) {
        Set<Operator> results = new HashSet<>();
        for (Operator operator : operators) {
            Operator source = detail.getSource(operator);
            assertThat(operator.toString(), source, is(notNullValue()));
            results.add(source);
        }
        return results;
    }

    /**
     * Returns copies of the source operators.
     * @param detail the detail
     * @param operators the sources
     * @return the related copies
     */
    public static Set<Operator> toCopies(PlanDetail detail, Collection<? extends Operator> operators) {
        Set<Operator> results = new HashSet<>();
        for (Operator operator : operators) {
            results.addAll(detail.getCopies(operator));
        }
        return results;
    }

    /**
     * Returns copies of the source operators.
     * @param detail the detail
     * @param owner the owner
     * @param operators the sources
     * @return the related copies
     */
    public static Set<Operator> toCopies(PlanDetail detail, SubPlan owner, Collection<? extends Operator> operators) {
        Set<Operator> results = new HashSet<>();
        for (Operator operator : operators) {
            results.addAll(detail.getCopies(operator, owner));
        }
        return results;
    }

    /**
     * Returns the operators in the sub-plan ports.
     * @param ports the sub-plan ports
     * @return the operators
     */
    public static Set<MarkerOperator> toOperators(Collection<? extends SubPlan.Port> ports) {
        Set<MarkerOperator> results = new LinkedHashSet<>();
        for (SubPlan.Port port : ports) {
            results.add(port.getOperator());
        }
        return results;
    }

    /**
     * Returns the successors of the sub-plan.
     * @param vertex the sub-plan
     * @return the successors
     */
    public static Set<SubPlan> succ(SubPlan vertex) {
        Set<SubPlan> results = new HashSet<>();
        for (SubPlan.Port port : vertex.getOutputs()) {
            for (SubPlan.Port opposite : port.getOpposites()) {
                results.add(opposite.getOwner());
            }
        }
        return results;
    }

    /**
     * Returns the predecessors of the sub-plan.
     * @param vertex the sub-plan
     * @return the predecessors
     */
    public static Set<SubPlan> pred(SubPlan vertex) {
        Set<SubPlan> results = new HashSet<>();
        for (SubPlan.Port port : vertex.getInputs()) {
            for (SubPlan.Port opposite : port.getOpposites()) {
                results.add(opposite.getOwner());
            }
        }
        return results;
    }
}