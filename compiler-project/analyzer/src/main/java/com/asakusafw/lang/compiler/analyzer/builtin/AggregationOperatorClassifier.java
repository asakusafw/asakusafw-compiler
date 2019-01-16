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
package com.asakusafw.lang.compiler.analyzer.builtin;

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputType;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.operator.Fold;

/**
 * Provides {@link OperatorClass} for generic <em>aggregation like</em> operators.
 */
public class AggregationOperatorClassifier implements OperatorCharacterizer<OperatorClass> {

    static final Logger LOG = LoggerFactory.getLogger(AggregationOperatorClassifier.class);

    private static final String KEY_AGGREGATION_TYPE = "partialAggregation"; //$NON-NLS-1$

    private static final PartialAggregation[] OPTIONS_AGGREGATION = {
        PartialAggregation.TOTAL,
        PartialAggregation.PARTIAL,
    };

    /**
     * The compiler option key of whether the partial aggregation is enabled in default or not.
     */
    public static final String KEY_AGGREGATION = "operator.aggregation.default"; //$NON-NLS-1$

    /**
     * The default value of {@value #KEY_AGGREGATION}.
     */
    public static final PartialAggregation DEFAULT_AGGREGATEION = PartialAggregation.TOTAL;

    @Override
    public OperatorClass extract(Context context, Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.USER) {
            throw new IllegalArgumentException();
        }
        return extract0(context, (UserOperator) operator);
    }

    static OperatorClass extract0(Context context, UserOperator operator) {
        OperatorInput input = operator.getInput(Fold.ID_INPUT);
        OperatorClass.Builder builder = OperatorClass.builder(operator, InputType.GROUP);
        builder.with(input, InputAttribute.PRIMARY);
        builder.with(input, InputAttribute.AGGREATE);
        if (isPartialAggregation(context.getOptions(), operator)) {
            builder.with(input, InputAttribute.PARTIAL_REDUCTION);
        }
        return Util.resolve(builder, operator);
    }

    private static boolean isPartialAggregation(CompilerOptions options, UserOperator operator) {
        if (operator.getInputs().stream()
                .map(OperatorInput::getInputUnit)
                .anyMatch(Predicate.isEqual(InputUnit.WHOLE))) {
            // partial aggregation is disabled if data tables are required
            return false;
        }
        PartialAggregation value = Util.element(operator.getAnnotation(),
                KEY_AGGREGATION_TYPE, PartialAggregation.DEFAULT);
        if (value == PartialAggregation.DEFAULT) {
            value = Util.resolve(options, KEY_AGGREGATION, OPTIONS_AGGREGATION, DEFAULT_AGGREGATEION);
        }
        return value == PartialAggregation.PARTIAL;
    }
}
