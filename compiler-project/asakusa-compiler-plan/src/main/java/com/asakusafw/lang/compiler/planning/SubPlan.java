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
package com.asakusafw.lang.compiler.planning;

import java.util.Set;

import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Represents a logical execution unit of {@link Plan}.
 * <p>
 * Terminology:
 * </p>
 * <dl>
 * <dt> <em>member operator</em> </dt>
 * <dd>
 *   each operator which is in the sub-plan.
 * </dd>
 * <dt> <em>input operator</em> </dt>
 * <dd>
 *   each operator which comprises an input of the sub-plan.
 * </dd>
 * <dt> <em>output operator</em> </dt>
 * <dd>
 *   each operator which comprises an output of the sub-plan.
 * </dd>
 * <dt> <em>body operator</em> </dt>
 * <dd>
 *   each member operator other than <em>input operator</em> and <em>output operator</em>.
 * </dd>
 * </dl>
 *
 * <p>
 * Each {@link SubPlan} object must satisfy following invariants:
 * </p>
 * <ol>
 * <li> the sub-plan must have at least one input </li>
 * <li> the sub-plan must have at least one output </li>
 * <li> each sub-plan input must have a unique <em>input operator</em> </li>
 * <li> each sub-plan output must have a unique <em>output operator</em> </li>
 * <li> each <em>input operator</em> must be also a <em>member operator</em> </li>
 * <li> each <em>output operator</em> must be also a <em>member operator</em> </li>
 * <li> each <em>input operator</em> must not have any predecessors </li>
 * <li> each <em>output operator</em> must not have any successors </li>
 * <li>
 *   each <em>input operator</em> and <em>body operator</em> must be
 *   forward reachable to any <em>output operators</em>
 * </li>
 * <li>
 *   each <em>output operator</em> and <em>body operator</em> must be
 *   backward reachable to any <em>input operators</em>
 * </li>
 * <li>
 *   any successor and predecessor of each <em>member operator</em> must be also a <em>member operator</em>
 * </li>
 * </ol>
 */
public interface SubPlan extends AttributeContainer {

    /**
     * Returns the owner of this sub-plan.
     * @return the owner
     */
    Plan getOwner();

    /**
     * Returns the input ports of this sub-plan.
     * @return the input ports
     */
    Set<? extends Input> getInputs();

    /**
     * Returns the output ports of this sub-plan.
     * @return the output ports
     */
    Set<? extends Output> getOutputs();

    /**
     * Returns operators in this sub-plan.
     * @return the operators (includes input/output operators)
     */
    Set<? extends Operator> getOperators();

    /**
     * Returns the input port of this sub-plan for the operator.
     * @param operator the operator
     * @return the input port which original operator is the specified one
     */
    Input findInput(Operator operator);

    /**
     * Returns the output port of this sub-plan for the operator.
     * @param operator the operator
     * @return the output port which original operator is the specified one
     */
    Output findOutput(Operator operator);

    /**
     * Represents an input/output port of {@link SubPlan}.
     */
    public interface Port extends AttributeContainer {

        /**
         * Returns the owner of this port.
         * @return the owner
         */
        SubPlan getOwner();

        /**
         * Returns the connected opposite ports.
         * @return the opposite ports
         */
        Set<? extends Port> getOpposites();

        /**
         * Returns the operator which represents this port.
         * The returning operator will detached from other sub-plan.
         * @return the original operator
         */
        MarkerOperator getOperator();
    }

    /**
     * Represents an input port of {@link SubPlan}.
     */
    public interface Input extends Port {

        /**
         * Returns whether this is connected to the specified opposite or not.
         * @param opposite the opposite port
         * @return {@code true} if their are connected, otherwise {@code false}
         */
        boolean isConnected(Output opposite);

        /**
         * Returns the upstream outputs.
         * @return the upstream outputs
         */
        @Override
        Set<? extends Output> getOpposites();
    }

    /**
     * Represents an input port of {@link SubPlan}.
     */
    public interface Output extends Port {

        /**
         * Returns whether this is connected to the specified opposite or not.
         * @param opposite the opposite port
         * @return {@code true} if their are connected, otherwise {@code false}
         */
        boolean isConnected(Input opposite);

        /**
         * Returns the downstream inputs.
         * @return the downstream inputs
         */
        @Override
        Set<? extends Input> getOpposites();
    }
}
