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

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.analyzer.util.MasterJoinOperatorUtil;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputType;
import com.asakusafw.vocabulary.operator.MasterJoin;

/**
 * Provides {@link OperatorClass} for generic <em>master-join like</em> operators.
 */
public class MasterJoinOperatorClassifier implements OperatorCharacterizer<OperatorClass> {

    static final Logger LOG = LoggerFactory.getLogger(MasterJoinOperatorClassifier.class);

    /**
     * The compiler option key of the limit to select broadcast join in bytes.
     */
    public static final String KEY_BROADCAST_LIMIT = "operator.join.broadcast.limit"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_BROADCAST_LIMIT}.
     */
    public static final long DEFAULT_BROADCAST_LIMIT = 20 * 1024 * 1024;

    @Override
    public OperatorClass extract(Context context, Operator operator) {
        if (MasterJoinOperatorUtil.isSupported(operator) == false) {
            throw new IllegalArgumentException();
        }
        OperatorEstimate estimate = context.estimate(operator);
        return extract0(context, (UserOperator) operator, estimate);
    }

    static OperatorClass extract0(Context context, UserOperator operator, OperatorEstimate estimate) {
        OperatorInput masterInput = operator.getInput(MasterJoin.ID_INPUT_MASTER);
        double size = estimate.getSize(masterInput);
        if (Double.isNaN(size) == false) {
            long limit = getBroadcastJoinLimit(context.getOptions());
            if (size <= limit) {
                return extractAsBroadcast(context, operator);
            }
        }
        return extractAsDefault(context, operator);
    }

    static OperatorClass extractAsDefault(Context context, UserOperator operator) {
        return OperatorClass.builder(operator, InputType.GROUP)
                .with(operator.getInput(MasterJoin.ID_INPUT_MASTER), InputAttribute.PRIMARY)
                .with(operator.getInput(MasterJoin.ID_INPUT_TRANSACTION), InputAttribute.PRIMARY)
                .build();
    }

    static OperatorClass extractAsBroadcast(Context context, UserOperator operator) {
        return OperatorClass.builder(operator, InputType.RECORD)
                .with(operator.getInput(MasterJoin.ID_INPUT_TRANSACTION), InputAttribute.PRIMARY)
                .build();
    }

    private static long getBroadcastJoinLimit(CompilerOptions options) {
        String string = options.get(KEY_BROADCAST_LIMIT, null);
        if (string == null) {
            return DEFAULT_BROADCAST_LIMIT;
        }
        try {
            return Long.parseLong(string);
        } catch (NumberFormatException e) {
            LOG.warn(MessageFormat.format(
                    "broadcast limit value must be an integral number: {0}={1}",
                    KEY_BROADCAST_LIMIT,
                    string), e);
            return -1;
        }
    }
}
