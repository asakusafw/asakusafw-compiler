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
package com.asakusafw.dag.compiler.model.plan;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Objects;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;

final class Util {

    private static final String LABEL_NOT_AVAILABLE = "N/A"; //$NON-NLS-1$

    private static final OperatorKind[] TYPICAL_ORDER = {
        OperatorKind.MARKER,
        OperatorKind.CORE,
        OperatorKind.USER,
        OperatorKind.FLOW,
        OperatorKind.INPUT,
        OperatorKind.OUTPUT,
        OperatorKind.CUSTOM,
    };

    private Util() {
        return;
    }

    static boolean isPrimary(SubPlan.Input port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        return marker != null && marker != PlanMarker.BROADCAST;
    }

    static Operator findMostTypical(Collection<? extends Operator> operators) {
        Operator current = null;
        for (Operator operator : operators) {
            if (current == null || isMoreTypical(operator, current)) {
                current = operator;
            }
        }
        return current;
    }

    static boolean isMoreTypical(Operator a, Operator b) {
        OperatorKind aKind = a.getOperatorKind();
        OperatorKind bKind = b.getOperatorKind();
        if (aKind == bKind) {
            return false;
        } else {
            for (OperatorKind target : TYPICAL_ORDER) {
                if (aKind == target || bKind == target) {
                    return bKind == target;
                }
            }
        }
        return false;
    }

    static String toLabel(Object value) {
        return Objects.toString(value, LABEL_NOT_AVAILABLE);
    }

    static String toOperatorLabel(Operator operator) {
        if (operator == null) {
            return LABEL_NOT_AVAILABLE;
        } else if (operator.getOperatorKind() == OperatorKind.USER) {
            UserOperator op = (UserOperator) operator;
            return MessageFormat.format(
                    "@{0}:{1}.{2}", //$NON-NLS-1$
                    op.getAnnotation().getDeclaringClass().getSimpleName(),
                    op.getMethod().getDeclaringClass().getSimpleName(),
                    op.getMethod().getName());
        } else {
            return operator.toString();
        }
    }
}
