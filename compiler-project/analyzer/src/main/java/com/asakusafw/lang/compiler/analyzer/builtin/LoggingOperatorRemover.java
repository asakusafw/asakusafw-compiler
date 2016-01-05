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

import com.asakusafw.lang.compiler.analyzer.util.LoggingOperatorUtil;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.vocabulary.operator.Logging;

/**
 * Removes {@link Logging} operators.
 */
public class LoggingOperatorRemover implements OperatorRewriter {

    static final Logger LOG = LoggerFactory.getLogger(LoggingOperatorRemover.class);

    /**
     * The compiler option key of whether the partial aggregation is enabled in default or not.
     */
    public static final String KEY_LOG_LEVEL = "operator.logging.level"; //$NON-NLS-1$

    /**
     * The default value of {@value #KEY_LOG_LEVEL}.
     */
    public static final Logging.Level DEFAULT_LOG_LEVEL = Logging.Level.INFO;

    @Override
    public void perform(Context context, OperatorGraph graph) {
        Logging.Level level = getLogLevel(context.getOptions());
        if (level == Logging.Level.DEBUG) {
            return;
        }
        LOG.debug("applying logging operator remover: flow={}, level={}", context.getFlowId(), level); //$NON-NLS-1$
        for (Operator operator : graph.getOperators(true)) {
            if (LoggingOperatorUtil.isSupported(operator) == false) {
                continue;
            }
            Logging.Level target;
            try {
                target = LoggingOperatorUtil.getLogLevel(context.getClassLoader(), operator);
            } catch (ReflectiveOperationException e) {
                LOG.warn(MessageFormat.format(
                        "failed to extract log level: {0}",
                        operator), e);
                continue;
            }
            if (isEnabled(level, target) == false) {
                LOG.debug("suppressing logging operator: operator={}, level={}", operator, target); //$NON-NLS-1$
                Operators.remove(operator);
                graph.remove(operator);
            }
        }
    }

    private static Logging.Level getLogLevel(CompilerOptions options) {
        return Util.resolve(options, KEY_LOG_LEVEL, Logging.Level.values(), DEFAULT_LOG_LEVEL);
    }

    private boolean isEnabled(Logging.Level level, Logging.Level target) {
        return level.compareTo(target) >= 0;
    }
}
