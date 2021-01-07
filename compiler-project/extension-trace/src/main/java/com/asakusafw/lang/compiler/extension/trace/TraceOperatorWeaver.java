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
package com.asakusafw.lang.compiler.extension.trace;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.runtime.trace.DefaultTraceOperator;
import com.asakusafw.trace.io.TraceSettingSerializer;
import com.asakusafw.trace.model.TraceSetting;
import com.asakusafw.trace.model.Tracepoint;
import com.asakusafw.trace.model.Tracepoint.PortKind;
import com.asakusafw.vocabulary.operator.Logging;
import com.asakusafw.vocabulary.operator.Trace;

/**
 * Injects {@link Trace} operators.
 * @since 0.3.1
 */
public class TraceOperatorWeaver implements OperatorRewriter {

    static final Logger LOG = LoggerFactory.getLogger(TraceOperatorWeaver.class);

    /**
     * The compiler option name of the serialized {@link TraceSetting} objects.
     * @see TraceSettingSerializer
     */
    public static final String KEY_COMPILER_OPTION = "trace.settings"; //$NON-NLS-1$

    static final AnnotationDescription TRACE_ANNOTATION;
    static {
        Map<String, ValueDescription> elements = new HashMap<>();
        for (Method method : Logging.class.getMethods()) {
            Object value = method.getDefaultValue();
            if (value != null) {
                elements.put(method.getName(), Descriptions.valueOf(value));
            }
        }
        TRACE_ANNOTATION = new AnnotationDescription(Descriptions.classOf(Logging.class), elements);
    }

    static final MethodDescription TRACE_HANDLER = new MethodDescription(
            Descriptions.classOf(DefaultTraceOperator.class),
            "trace", //$NON-NLS-1$
            Descriptions.classOf(Object.class), Descriptions.classOf(String.class));

    @Override
    public void perform(Context context, OperatorGraph graph) {
        String option = context.getOptions().get(KEY_COMPILER_OPTION, null);
        if (option == null) {
            LOG.debug("tracing facility is disabled");
            return;
        }
        Collection<? extends TraceSetting> settings = TraceSettingSerializer.deserialize(option);
        perform(graph, context.getClassLoader(), settings);
    }

    static void perform(OperatorGraph graph, ClassLoader classLoader, Collection<? extends TraceSetting> settings) {
        Map<Key, List<TraceSetting>> map = parse(settings, classLoader);
        for (Operator operator : graph.getOperators(true)) {
            if (operator.getOperatorKind() != OperatorKind.USER) {
                continue;
            }
            Key key = Key.of(((UserOperator) operator).getMethod());
            List<TraceSetting> scoped = map.get(key);
            if (scoped != null) {
                for (TraceSetting setting : scoped) {
                    Operator trace = apply(operator, setting);
                    graph.add(trace);
                }
            }
        }
    }

    private static Map<Key, List<TraceSetting>> parse(
            Collection<? extends TraceSetting> settings, ClassLoader classLoader) {
        Map<Key, List<TraceSetting>> results = new HashMap<>();
        for (TraceSetting setting : Util.normalize(classLoader, settings)) {
            Key key = Key.of(setting.getTracepoint());
            List<TraceSetting> list = results.get(key);
            if (list == null) {
                list = new ArrayList<>(1);
                results.put(key, list);
            }
            list.add(setting);
        }
        return results;
    }

    private static Operator apply(Operator operator, TraceSetting setting) {
        Tracepoint tracepoint = setting.getTracepoint();
        if (tracepoint.getPortKind() == PortKind.INPUT) {
            OperatorInput port = operator.findInput(tracepoint.getPortName());
            if (port == null) {
                LOG.warn(MessageFormat.format(
                        "failed to detect operator port: {0}",
                        tracepoint));
                return null;
            }
            return Operators.insert(createTrace(setting, port.getDataType()), port);
        } else {
            OperatorOutput port = operator.findOutput(tracepoint.getPortName());
            if (port == null) {
                LOG.warn(MessageFormat.format(
                        "failed to detect operator port: {0}",
                        tracepoint));
                return null;
            }
            return Operators.insert(createTrace(setting, port.getDataType()), port);
        }
    }

    private static Operator createTrace(TraceSetting setting, TypeDescription dataType) {
        return UserOperator.builder(TRACE_ANNOTATION, TRACE_HANDLER, TRACE_HANDLER.getDeclaringClass())
                .input("port", dataType)
                .output("port", dataType)
                .argument("header", Descriptions.valueOf(setting.getTracepoint().toString()))
                .constraint(OperatorConstraint.AT_LEAST_ONCE)
                .build();
    }

    private static final class Key {

        private final String declaring;

        private final PropertyName name;

        private Key(String declaring, PropertyName name) {
            this.declaring = declaring;
            this.name = name;
        }

        static Key of(MethodDescription method) {
            return new Key(method.getDeclaringClass().getBinaryName(), PropertyName.of(method.getName()));
        }

        static Key of(Tracepoint point) {
            return new Key(point.getOperatorClassName(), PropertyName.of(point.getOperatorMethodName()));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(declaring);
            result = prime * result + Objects.hashCode(name);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Key other = (Key) obj;
            if (!Objects.equals(declaring, other.declaring)) {
                return false;
            }
            if (!Objects.equals(name, other.name)) {
                return false;
            }
            return true;
        }
    }
}
