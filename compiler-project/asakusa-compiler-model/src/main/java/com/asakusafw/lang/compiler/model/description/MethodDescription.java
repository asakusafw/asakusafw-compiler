package com.asakusafw.lang.compiler.model.description;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a method.
 */
public class MethodDescription implements Description {

    private final ClassDescription declaringClass;

    private final String name;

    private final List<ReifiableTypeDescription> parameterTypes;

    /**
     * Creates a new instance.
     * @param declaringClass the declaring class
     * @param name the method name
     * @param parameterTypes the erased parameter types
     */
    public MethodDescription(
            ClassDescription declaringClass,
            String name,
            List<? extends ReifiableTypeDescription> parameterTypes) {
        this.declaringClass = declaringClass;
        this.name = name;
        this.parameterTypes = Collections.unmodifiableList(new ArrayList<>(parameterTypes));
    }

    /**
     * Creates a new instance.
     * @param method the reflective object
     * @return the created instance
     */
    public static MethodDescription of(Method method) {
        ClassDescription declaringClass = Descriptions.classOf(method.getDeclaringClass());
        String name = method.getName();
        List<ReifiableTypeDescription> parameterTypes = new ArrayList<>();
        for (Class<?> aClass : method.getParameterTypes()) {
            parameterTypes.add(ReifiableTypeDescription.of(aClass));
        }
        return new MethodDescription(declaringClass, name, parameterTypes);
    }

    /**
     * Returns the declaring class.
     * @return the declaring class
     */
    public ClassDescription getDeclaringClass() {
        return declaringClass;
    }

    /**
     * Returns the method name.
     * @return the method name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the erased parameter types
     * @return the erased parameter types
     */
    public List<ReifiableTypeDescription> getParameterTypes() {
        return parameterTypes;
    }

    /**
     * Resolves this method and returns its reflective object.
     * @param classLoader the class loader
     * @return the resolved object
     * @throws NoSuchMethodException the target method is not found
     */
    public Method resolve(ClassLoader classLoader) throws NoSuchMethodException {
        try {
            Class<?> decl = declaringClass.resolve(classLoader);
            List<Class<?>> params = new ArrayList<>();
            for (ReifiableTypeDescription t : parameterTypes) {
                params.add(t.resolve(classLoader));
            }
            NoSuchMethodException exception;
            try {
                // first, we search for public/inherited methods
                return decl.getMethod(name, params.toArray(new Class[params.size()]));
            } catch (NoSuchMethodException e) {
                exception = e;
            }
            try {
                // then, we search for non-public methods
                return decl.getDeclaredMethod(name, params.toArray(new Class[params.size()]));
            } catch (NoSuchMethodException e) {
                // raise the first exception instead of the second one for non-public methods
                throw exception;
            }
        } catch (ClassNotFoundException e) {
            throw new NoSuchMethodException(toString());
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + declaringClass.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + parameterTypes.hashCode();
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
        MethodDescription other = (MethodDescription) obj;
        if (!declaringClass.equals(other.declaringClass)) {
            return false;
        }
        if (!name.equals(other.name)) {
            return false;
        }
        if (!parameterTypes.equals(other.parameterTypes)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Method({0}#{1})", //$NON-NLS-1$
                declaringClass.getName(),
                name);
    }
}
