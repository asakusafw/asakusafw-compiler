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
package com.asakusafw.lang.compiler.optimizer.adapter;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimators;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOperatorEstimate;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Adapter for {@link com.asakusafw.lang.compiler.optimizer.OperatorEstimator.Context}.
 */
public class OperatorEstimatorAdapter
        extends ForwardingOptimizerContext
        implements OperatorEstimator.Context {

    private final Map<Operator, OperatorEstimate> estimated = new HashMap<>();

    private Session session;

    /**
     * Creates a new instance.
     * @param delegate the delegation target
     */
    public OperatorEstimatorAdapter(OptimizerContext delegate) {
        super(delegate);
    }

    @Override
    public void apply(OperatorEstimator estimator, Collection<? extends Operator> operators) {
        Graph<Operator> dependencies = collectDependencies(operators);
        for (Operator operator : Graphs.sortPostOrder(dependencies)) {
            perform0(estimator, operator);
        }
    }

    private void perform0(OperatorEstimator estimator, Operator operator) {
        if (this.session != null) {
            throw new IllegalStateException(MessageFormat.format(
                    "other session is active: {0}",
                    this.session.target));
        }
        if (estimated.containsKey(operator)) {
            return;
        }
        this.session = new Session(operator);
        try {
            for (OperatorInput port : operator.getInputs()) {
                double size = OperatorEstimators.getSize(this, port);
                if (Double.isNaN(size) == false) {
                    this.session.estimate.putSize(port, size);
                }
            }
            estimator.perform(this, operator);
        } finally {
            put(operator, this.session.estimate);
            this.session = null;
        }
    }

    private Graph<Operator> collectDependencies(Collection<? extends Operator> operators) {
        Graph<Operator> result = Graphs.newInstance();
        Set<Operator> saw = new HashSet<>(estimated.keySet());
        LinkedList<Operator> work = new LinkedList<>(operators);
        while (work.isEmpty() == false) {
            Operator first = work.removeFirst();
            if (saw.contains(first)) {
                continue;
            }
            saw.add(first);
            result.addNode(first);
            for (Operator blocker : Operators.getPredecessors(first)) {
                work.addLast(blocker);
                result.addEdge(first, blocker);
            }
        }
        return result;
    }

    /**
     * Puts estimate for the operator.
     * @param operator the operator
     * @param estimate the estimate
     */
    public void put(Operator operator, OperatorEstimate estimate) {
        estimated.put(operator, estimate);
    }

    @Override
    public void putSize(OperatorOutput port, double size) {
        current(port.getOwner()).putSize(port, size);
    }

    @Override
    public <T> void putAttribute(Class<T> attributeType, T attributeValue) {
        if (this.session == null) {
            throw new IllegalStateException("session is not started"); //$NON-NLS-1$
        }
        this.session.estimate.putAttribute(attributeType, attributeValue);
    }

    @Override
    public <T> void putAttribute(OperatorInput port, Class<T> attributeType, T attributeValue) {
        current(port.getOwner()).putAttribute(port, attributeType, attributeValue);
    }

    @Override
    public <T> void putAttribute(OperatorOutput port, Class<T> attributeType, T attributeValue) {
        current(port.getOwner()).putAttribute(port, attributeType, attributeValue);
    }

    private BasicOperatorEstimate current(Operator operator) {
        if (this.session == null) {
            throw new IllegalStateException("session is not started"); //$NON-NLS-1$
        }
        if (operator != this.session.target) {
            throw new IllegalStateException("inconsistent session"); //$NON-NLS-1$
        }
        return this.session.estimate;
    }

    @Override
    public OperatorEstimate estimate(Operator operator) {
        OperatorEstimate result = estimated.get(operator);
        if (result == null) {
            return OperatorEstimate.UNKNOWN;
        }
        return result;
    }

    @Override
    public Map<Operator, OperatorEstimate> estimate(Operator... operators) {
        return estimate(Arrays.asList(operators));
    }

    @Override
    public Map<Operator, OperatorEstimate> estimate(Collection<? extends Operator> operators) {
        Map<Operator, OperatorEstimate> results = new HashMap<>();
        for (Operator operator : operators) {
            results.put(operator, estimate(operator));
        }
        return results;
    }

    private static class Session {

        final Operator target;

        final BasicOperatorEstimate estimate = new BasicOperatorEstimate();

        Session(Operator target) {
            this.target = target;
        }
    }
}
