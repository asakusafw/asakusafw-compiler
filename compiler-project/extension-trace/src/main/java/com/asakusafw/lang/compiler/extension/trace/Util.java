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
package com.asakusafw.lang.compiler.extension.trace;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.trace.model.TraceSetting;
import com.asakusafw.trace.model.Tracepoint;
import com.asakusafw.trace.model.Tracepoint.PortKind;
import com.asakusafw.vocabulary.flow.FlowPart;
import com.asakusafw.vocabulary.flow.util.CoreOperatorFactory;
import com.asakusafw.vocabulary.flow.util.CoreOperators;
import com.asakusafw.vocabulary.operator.OperatorFactory;
import com.asakusafw.vocabulary.operator.OperatorInfo;

final class Util {

    static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private Util() {
        return;
    }

    static List<TraceSetting> normalize(ClassLoader loader, Collection<? extends TraceSetting> settings) {
        assert loader != null;
        assert settings != null;
        List<TraceSetting> results = new ArrayList<>();
        for (TraceSetting setting : settings) {
            Tracepoint orig = setting.getTracepoint();
            Class<?> operatorClass;
            try {
                operatorClass = loader.loadClass(orig.getOperatorClassName());
            } catch (ClassNotFoundException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "failed to load operator class: {0}",
                        orig.getOperatorClassName()), e);
            }
            Tracepoint normalized = createTracepoint(
                    operatorClass, orig.getOperatorMethodName(),
                    orig.getPortKind(), orig.getPortName());
            if (normalized != null) {
                results.add(new TraceSetting(normalized, setting.getMode(), setting.getAttributes()));
            }
        }
        return results;
    }

    static Tracepoint createTracepoint(Class<?> operatorClass, String methodName, PortKind portKind, String portName) {
        assert operatorClass != null;
        assert methodName != null;
        assert portKind != null;
        assert portName != null;
        Class<?> factoryClass = findFactoryClass(operatorClass);
        OperatorFactory factory = factoryClass.getAnnotation(OperatorFactory.class);
        assert factory != null;
        Method factoryMethod = findFactoryMethod(factoryClass, methodName);
        OperatorInfo info = factoryMethod.getAnnotation(OperatorInfo.class);
        assert info != null;
        if (info.kind() == FlowPart.class) {
            LOG.warn(MessageFormat.format(
                    "Currently, this compiler does not support tracing for flow-part inputs/outputs: {0}",
                    factory.value().getName()));
            return null;
        }
        if (portKind == PortKind.INPUT) {
            boolean found = false;
            for (OperatorInfo.Input port : info.input()) {
                if (port.name().equals(portName)) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
                List<String> list = new ArrayList<>();
                for (OperatorInfo.Input port : info.input()) {
                    list.add(port.name());
                }
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "unknown input port \"{2}\"; it must be one of {3} ({0}#{1}(...))",
                        factoryClass.getName(),
                        factoryMethod.getName(),
                        portName,
                        list));
            }
        } else {
            boolean found = false;
            for (OperatorInfo.Output port : info.output()) {
                if (port.name().equals(portName)) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
                List<String> list = new ArrayList<>();
                for (OperatorInfo.Output port : info.output()) {
                    list.add(port.name());
                }
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "unknown output port \"{2}\"; it must be one of {3} ({0}#{1}(...))",
                        factoryClass.getName(),
                        factoryMethod.getName(),
                        portName,
                        list));
            }
        }
        return new Tracepoint(factory.value().getName(), factoryMethod.getName(), portKind, portName);
    }

    private static Class<?> findFactoryClass(Class<?> operatorOrFactoryClass) {
        assert operatorOrFactoryClass != null;
        if (operatorOrFactoryClass == CoreOperators.class || operatorOrFactoryClass == CoreOperatorFactory.class) {
            throw new DiagnosticException(Diagnostic.Level.ERROR,
                    "trace does not support core operators");
        }
        if (operatorOrFactoryClass.isAnnotationPresent(OperatorFactory.class)) {
            return operatorOrFactoryClass;
        } else {
            ClassLoader classLoader = operatorOrFactoryClass.getClassLoader();
            if (classLoader == null) {
                classLoader = ClassLoader.getSystemClassLoader();
            }
            Class<?> factoryClass;
            String factoryClassName = operatorOrFactoryClass.getName() + "Factory"; //$NON-NLS-1$
            try {
                factoryClass =  classLoader.loadClass(factoryClassName);
            } catch (ClassNotFoundException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "{0} is not an operator class (missing operator factory class: {1})",
                        operatorOrFactoryClass.getName(),
                        factoryClassName));
            }
            if (factoryClass.isAnnotationPresent(OperatorFactory.class) == false) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "{0} is not an operator class (missing OperatorFactory annotation: {1})",
                        operatorOrFactoryClass.getName(),
                        factoryClassName));
            }
            return factoryClass;
        }
    }

    private static Method findFactoryMethod(Class<?> factoryClass, String methodName) {
        assert factoryClass != null;
        assert methodName != null;
        for (Method method : factoryClass.getMethods()) {
            if (method.getName().equalsIgnoreCase(methodName)) {
                if (method.isAnnotationPresent(OperatorInfo.class)) {
                    return method;
                }
            }
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "Missing operator factory method: {0}#{1}(...)",
                factoryClass.getName(),
                methodName));
    }
}
