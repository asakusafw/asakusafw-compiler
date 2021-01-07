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
package com.asakusafw.lang.compiler.model.description;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents an annotation.
 */
public class AnnotationDescription implements ValueDescription {

    private final ClassDescription declaringClass;

    private final Map<String, ValueDescription> elements;

    /**
     * Creates a new instance for marker annotation.
     * @param declaringClass the declaring annotation type
     */
    public AnnotationDescription(ClassDescription declaringClass) {
        this(declaringClass, Collections.emptyMap());
    }

    /**
     * Creates a new instance for single element annotation.
     * @param declaringClass the declaring annotation type
     * @param value value of the single element annotation
     */
    public AnnotationDescription(ClassDescription declaringClass, ValueDescription value) {
        this(declaringClass, Collections.singletonMap("value", value)); //$NON-NLS-1$
    }

    /**
     * Creates a new instance.
     * @param declaringClass the declaring annotation type
     * @param elements the annotation elements (includes default values)
     */
    public AnnotationDescription(
            ClassDescription declaringClass,
            Map<String, ? extends ValueDescription> elements) {
        this.declaringClass = declaringClass;
        this.elements = Collections.unmodifiableMap(new LinkedHashMap<>(elements));
    }

    /**
     * Creates a new instance.
     * @param annotation the annotation
     * @return the created instance
     */
    public static AnnotationDescription of(Annotation annotation) {
        try {
            Class<? extends Annotation> declaring = annotation.annotationType();
            if (declaring.isAnnotation() == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "must be an annotation: {0}", //$NON-NLS-1$
                        annotation));
            }
            Map<String, ValueDescription> elements = new LinkedHashMap<>();
            for (Method method : declaring.getMethods()) {
                int modifiers = method.getModifiers();
                if (Modifier.isStatic(modifiers)) {
                    continue;
                }
                if (method.getDeclaringClass() != declaring || method.isSynthetic()) {
                    continue;
                }
                if (method.getParameterTypes().length != 0) {
                    continue;
                }
                Object value = method.invoke(annotation);
                elements.put(method.getName(), Descriptions.valueOf(value));
            }
            return new AnnotationDescription(Descriptions.classOf(declaring), elements);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "failed to analyze annotation: {0}", //$NON-NLS-1$
                    annotation), e);
        }
    }

    @Override
    public ValueKind getValueKind() {
        return ValueKind.ANNOTATION;
    }

    @Override
    public ClassDescription getValueType() {
        return getDeclaringClass();
    }

    /**
     * Returns the declaring annotation type.
     * @return the declaring annotation type
     */
    public ClassDescription getDeclaringClass() {
        return declaringClass;
    }

    /**
     * Returns the annotation elements.
     * @return the annotation elements
     */
    public Map<String, ValueDescription> getElements() {
        return elements;
    }

    @Override
    public Annotation resolve(ClassLoader classLoader) throws ReflectiveOperationException {
        Class<?> type = declaringClass.resolve(classLoader);
        Map<String, Object> resolved = new HashMap<>();
        for (Map.Entry<String, ValueDescription> entry : elements.entrySet()) {
            resolved.put(entry.getKey(), entry.getValue().resolve(classLoader));
        }
        return (Annotation) Proxy.newProxyInstance(
                classLoader,
                new Class<?>[] { type },
                new ElementHandler(type, resolved));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + declaringClass.hashCode();
        result = prime * result + elements.hashCode();
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
        AnnotationDescription other = (AnnotationDescription) obj;
        if (!declaringClass.equals(other.declaringClass)) {
            return false;
        }
        if (!elements.equals(other.elements)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Annotation({0})", //$NON-NLS-1$
                declaringClass.getClassName());
    }

    private static final class ElementHandler implements InvocationHandler {

        private final Class<?> annotationType;

        private final Map<String, Object> elements;

        ElementHandler(Class<?> annotationType, Map<String, Object> elements) {
            this.annotationType = annotationType;
            this.elements = elements;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (args == null || args.length == 0) {
                Object value = elements.get(name);
                if (value != null) {
                    return value;
                }
                switch (name) {
                case "annotationType": //$NON-NLS-1$
                    return annotationType;
                case "hashCode": //$NON-NLS-1$
                    return System.identityHashCode(proxy);
                case "toString": //$NON-NLS-1$
                    return annotationType.getName();
                default:
                    break;
                }
            } else if (args.length == 1) {
                switch (name) {
                case "equals": //$NON-NLS-1$
                    if (method.getParameterTypes()[0] == Object.class) {
                        return proxy == args[0];
                    }
                    break;
                default:
                    break;
                }
            }
            throw new UnsupportedOperationException(method.toString());
        }
    }
}
