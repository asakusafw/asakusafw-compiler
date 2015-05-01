/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.lang.compiler.analyzer.util.TypeInfo;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.graph.FlowIn;
import com.asakusafw.vocabulary.flow.graph.FlowOut;

/**
 * Builds an {@link OperatorGraph} from generic {@link FlowDescription} class.
 * @see JobflowAnalyzer
 */
public class FlowPartBuilder {

    private final FlowPartDriver driver;

    private final List<Object> arguments = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param driver the internal driver
     */
    public FlowPartBuilder(FlowPartDriver driver) {
        this.driver = driver;
    }

    /**
     * Creates a new instance.
     * @param analyzer the element analyzer
     */
    public FlowPartBuilder(FlowGraphAnalyzer analyzer) {
        this(new FlowPartDriver(analyzer));
    }

    /**
     * Adds an external input to the flow description constructor.
     * @param name the port name
     * @param description the external input description
     * @return this
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public FlowPartBuilder addInput(String name, ImporterDescription description) {
        arguments.add(driver.addInput(name, description));
        return this;
    }

    /**
     * Adds an external output to the flow description constructor.
     * @param name the port name
     * @param description the external output description
     * @return this
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public FlowPartBuilder addOutput(String name, ExporterDescription description) {
        arguments.add(driver.addOutput(name, description));
        return this;
    }

    /**
     * Adds a generic value to the flow description constructor.
     * @param value the generic value
     * @return this
     */
    public FlowPartBuilder addValue(Object value) {
        arguments.add(value);
        return this;
    }

    /**
     * Builds a jobflow object.
     * @param flowId the target flow ID
     * @param flowClass the target flow-part class
     * @return the built jobflow object
     */
    public Jobflow build(String flowId, Class<? extends FlowDescription> flowClass) {
        OperatorGraph graph = build(flowClass);
        return new Jobflow(new JobflowInfo.Basic(flowId, Descriptions.classOf(flowClass)), graph);
    }

    /**
     * Builds an operator graph.
     * @param flowClass the target flow-part class
     * @return the built operator graph
     */
    public OperatorGraph build(Class<? extends FlowDescription> flowClass) {
        FlowDescription description = analyze(flowClass);
        return driver.build(description);
    }

    private FlowDescription analyze(Class<? extends FlowDescription> flowClass) {
        Constructor<? extends FlowDescription> ctor = getConstructor(flowClass);
        try {
            return ctor.newInstance(arguments.toArray(new Object[arguments.size()]));
        } catch (ReflectiveOperationException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while creating an instance of {0}",
                    flowClass.getName()), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Constructor<? extends FlowDescription> getConstructor(Class<? extends FlowDescription> flowClass) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        Constructor<?>[] ctors = flowClass.getConstructors();
        if (ctors.length == 0) {
            diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "public constructor is not declared: {0}",
                    flowClass.getName())));
        } else {
            for (Constructor<?> ctor : ctors) {
                Diagnostic diagnostic = checkApply(ctor);
                if (diagnostic == null) {
                    return (Constructor<? extends FlowDescription>) ctor;
                } else {
                    diagnostics.add(diagnostic);
                }
            }
        }
        throw new DiagnosticException(diagnostics);
    }

    private Diagnostic checkApply(Constructor<?> constructor) {
        Type[] types = constructor.getGenericParameterTypes();
        if (types.length != arguments.size()) {
            return new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "specified {0} arguments, but target constructor has {1} parameters: {2} <> {3}",
                    arguments.size(),
                    types.length,
                    stringnizeObjects(arguments.toArray()),
                    stringnizeTypes(types)));
        }
        for (int i = 0; i < types.length; i++) {
            Type type = types[i];
            Object arg = arguments.get(i);
            if (arg == null) {
                // null type
                continue;
            }
            Class<?> raw = TypeInfo.erase(type);
            if (raw.isAssignableFrom(arg.getClass()) == false) {
                return new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                        "cannnot apply #{0} argument: {1}<>{2}",
                        i,
                        stringnizeObject(arg),
                        stringnizeType(type)));
            }
            Type dataType;
            if (arg instanceof FlowIn<?>) {
                dataType = ((FlowIn<?>) arg).getDescription().getDataType();
            } else if (arg instanceof FlowOut<?>) {
                dataType = ((FlowOut<?>) arg).getDescription().getDataType();
            } else {
                dataType = null;
            }
            if (dataType == null || (dataType instanceof Class<?>) == false) {
                continue;
            }
            TypeInfo info = TypeInfo.of(type);
            List<Type> typeArguments = info.getTypeArguments();
            if (typeArguments.size() == 1) {
                Class<?> required = TypeInfo.erase(typeArguments.get(0));
                Class<?> target = (Class<?>) dataType;
                if (required.isAssignableFrom(target) == false) {
                    return new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                            "cannnot apply #{0} argument: {1}<>{2}",
                            i,
                            stringnizeObject(arg),
                            stringnizeType(type)));
                }
            }
        }
        return null;
    }

    private static String stringnizeObject(Object object) {
        Type dataType;
        if (object instanceof FlowIn<?>) {
            dataType = ((FlowIn<?>) object).getDescription().getDataType();
        } else if (object instanceof FlowOut<?>) {
            dataType = ((FlowOut<?>) object).getDescription().getDataType();
        } else if (object instanceof Class<?>) {
            dataType = (Type) object;
        } else {
            dataType = null;
        }
        if (dataType == null) {
            return object.getClass().getSimpleName();
        }
        Class<?> first = TypeInfo.erase(dataType);
        return String.format("%s<%s>", object.getClass().getSimpleName(), first.getSimpleName()); //$NON-NLS-1$
    }

    private static String stringnizeType(Type type) {
        Class<?> raw = TypeInfo.erase(type);
        if (raw == In.class || raw == Out.class || raw == Class.class) {
            TypeInfo info = TypeInfo.of(type);
            List<Type> typeArguments = info.getTypeArguments();
            if (typeArguments.size() == 1) {
                Class<?> first = TypeInfo.erase(typeArguments.get(0));
                return String.format("%s<%s>", raw.getSimpleName(), first.getSimpleName()); //$NON-NLS-1$
            }
        }
        return raw.getSimpleName();
    }

    private static List<String> stringnizeObjects(Object[] objects) {
        List<String> results = new ArrayList<>();
        for (int i = 0; i < objects.length; i++) {
            results.add(stringnizeObject(objects[i]));
        }
        return results;
    }

    private static List<String> stringnizeTypes(Type[] types) {
        List<String> results = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            results.add(stringnizeType(types[i]));
        }
        return results;
    }
}
