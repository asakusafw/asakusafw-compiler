/**
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.util.List;

import com.asakusafw.lang.compiler.analyzer.util.GroupOperatorUtil;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputType;

/**
 * Provides {@link OperatorClass} for generic <em>co-group kind</em> operators.
 * @since 0.1.0
 * @version 0.3.0
 */
public class CoGroupKindOperatorClassifier implements OperatorCharacterizer<OperatorClass> {

    @Override
    public OperatorClass extract(Context context, Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.USER) {
            throw new IllegalArgumentException();
        }
        return extract0(context, (UserOperator) operator);
    }

    static OperatorClass extract0(Context context, UserOperator operator) {
        List<OperatorInput> inputs = operator.getInputs();
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException();
        }
        OperatorClass.Builder builder = OperatorClass.builder(operator, InputType.GROUP);
        for (OperatorInput input : inputs) {
            InputUnit unit = input.getInputUnit();
            if (unit == InputUnit.RECORD) {
                throw new IllegalStateException();
            } else if (unit == InputUnit.WHOLE) {
                continue;
            }
            builder.with(input, InputAttribute.PRIMARY);
            if (isSorted(input)) {
                builder.with(input, InputAttribute.SORTED);
            }
            switch (GroupOperatorUtil.getBufferType(input)) {
            case HEAP:
                break;
            case SPILL:
                builder.with(input, InputAttribute.ESCAPED);
                break;
            case VOLATILE:
                builder.with(input, InputAttribute.VOALTILE);
                break;
            default:
                throw new AssertionError(input);
            }
        }
        return Util.resolve(builder, operator);
    }

    private static boolean isSorted(OperatorInput input) {
        Group group = input.getGroup();
        return group != null && group.getOrdering().isEmpty() == false;
    }
}
