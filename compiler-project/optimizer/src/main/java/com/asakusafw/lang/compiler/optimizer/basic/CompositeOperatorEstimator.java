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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;

/**
 * Composition of {@link OperatorEstimator}.
 */
public final class CompositeOperatorEstimator
        extends AbstractCompositeEngine<OperatorEstimator>
        implements OperatorEstimator {

    CompositeOperatorEstimator(
            OperatorEstimator defaultElement,
            Map<OperatorKind, OperatorEstimator> defaults,
            Map<String, OperatorEstimator> inputs,
            Map<String, OperatorEstimator> outputs,
            Map<CoreOperatorKind, OperatorEstimator> cores,
            Map<ClassDescription, OperatorEstimator> users,
            Map<String, OperatorEstimator> customs) {
        super(defaultElement == null ? OperatorEstimator.NULL : defaultElement,
                defaults, inputs, outputs, cores, users, customs);
    }

    /**
     * Creates a new builder for this class.
     * @return the created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void perform(Context context, Operator operator) {
        OperatorEstimator delegate = getElement(operator);
        assert delegate != null;
        delegate.perform(context, operator);
    }

    /**
     * Builder for {@link CompositeOperatorEstimator}.
     */
    public static class Builder extends AbstractBuilder<Builder, OperatorEstimator> {

        @Override
        protected OperatorEstimator doBuild(
                OperatorEstimator defaultElement,
                Map<OperatorKind, OperatorEstimator> kindElements,
                Map<String, OperatorEstimator> inputElements,
                Map<String, OperatorEstimator> outputElements,
                Map<CoreOperatorKind, OperatorEstimator> coreElements,
                Map<ClassDescription, OperatorEstimator> userElements,
                Map<String, OperatorEstimator> customElements) {
            return new CompositeOperatorEstimator(
                    defaultElement, kindElements,
                    inputElements, outputElements,
                    coreElements, userElements, customElements);
        }
    }
}
