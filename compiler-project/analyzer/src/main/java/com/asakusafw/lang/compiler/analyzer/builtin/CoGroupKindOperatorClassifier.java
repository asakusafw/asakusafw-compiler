/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
import java.util.Optional;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputType;
import com.asakusafw.vocabulary.attribute.BufferType;
import com.asakusafw.vocabulary.flow.processor.InputBuffer;

/**
 * Provides {@link OperatorClass} for generic <em>co-group kind</em> operators.
 * @since 0.1.0
 * @version 0.3.0
 */
public class CoGroupKindOperatorClassifier implements OperatorCharacterizer<OperatorClass> {

    private static final String KEY_INPUT_BUFFER_TYPE = "inputBuffer"; //$NON-NLS-1$

    private static final InputBuffer DEFAULT_INPUT_BUFFER_TYPE = InputBuffer.EXPAND;

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
        BufferType defaultInputBufferType = isEscaped(operator.getAnnotation()) ? BufferType.STORED : BufferType.HEAP;
        for (OperatorInput input : inputs) {
            builder.with(input, InputAttribute.PRIMARY);
            if (isSorted(input)) {
                builder.with(input, InputAttribute.SORTED);
            }
            BufferType bufferType = Optional.ofNullable(input.getAttribute(BufferType.class))
                    .orElse(defaultInputBufferType);
            switch (bufferType) {
            case HEAP:
                break;
            case STORED:
                builder.with(input, InputAttribute.ESCAPED);
                break;
            case VOLATILE:
                builder.with(input, InputAttribute.VOALTILE);
                break;
            default:
                throw new AssertionError(bufferType);
            }
        }
        return builder.build();
    }

    private static boolean isSorted(OperatorInput input) {
        Group group = input.getGroup();
        return group != null && group.getOrdering().isEmpty() == false;
    }

    private static boolean isEscaped(AnnotationDescription annotation) {
        InputBuffer value = Util.element(annotation, KEY_INPUT_BUFFER_TYPE, DEFAULT_INPUT_BUFFER_TYPE);
        return value == InputBuffer.ESCAPE;
    }
}
