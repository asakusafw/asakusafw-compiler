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
package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.attribute.BufferType;
import com.asakusafw.vocabulary.flow.processor.InputBuffer;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.GroupSort;

/**
 * Utilities about operators which process input groups.
 * @since 0.4.1
 */
public final class GroupOperatorUtil {

    static final Logger LOG = LoggerFactory.getLogger(GroupOperatorUtil.class);

    private static final String NAME_INPUT_BUFFER = "inputBuffer"; //$NON-NLS-1$

    private static final EnumConstantDescription INPUT_BUFFER_DEFAULT = EnumConstantDescription.of(InputBuffer.EXPAND);

    private static final EnumConstantDescription INPUT_BUFFER_ESCAPE = EnumConstantDescription.of(InputBuffer.ESCAPE);

    private static final Set<ClassDescription> SUPPORTED;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(Descriptions.classOf(GroupSort.class));
        set.add(Descriptions.classOf(CoGroup.class));
        SUPPORTED = set;
    }

    private GroupOperatorUtil() {
        return;
    }

    /**
     * Returns whether the target operator is <em>grouping operator kind</em> or not.
     * @param operator the target operator
     * @return {@code true} if the target operator is <em>grouping operator kind</em>, otherwise {@code false}
     */
    public static boolean isSupported(Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.USER) {
            return false;
        }
        UserOperator op = (UserOperator) operator;
        AnnotationDescription annotation = op.getAnnotation();
        return SUPPORTED.contains(annotation.getDeclaringClass());
    }

    /**
     * Returns the buffer type of the given operator input.
     * The operator must be a <em>grouping operator kind</em>.
     * @param port the target input port
     * @return the corresponded buffer type
     * @throws IllegalArgumentException if the target operator is not supported
     */
    public static BufferType getBufferType(OperatorInput port) {
        if (isSupported(port.getOwner()) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be a kind of Logging: {0}",
                    port.getOwner()));
        }
        return Optional.ofNullable(port.getAttribute(BufferType.class))
                .orElseGet(() -> isEscape((UserOperator) port.getOwner()) ? BufferType.SPILL : BufferType.HEAP);
    }

    private static boolean isEscape(UserOperator operator) {
        AnnotationDescription annotation = operator.getAnnotation();
        ValueDescription inputBuffer = annotation.getElements().getOrDefault(NAME_INPUT_BUFFER, INPUT_BUFFER_DEFAULT);
        return inputBuffer.equals(INPUT_BUFFER_ESCAPE);
    }
}
