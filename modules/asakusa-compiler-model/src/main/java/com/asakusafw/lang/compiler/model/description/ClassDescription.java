package com.asakusafw.lang.compiler.model.description;

import java.text.MessageFormat;

/**
 * Represents a class.
 */
public class ClassDescription extends ReifiableTypeDescription {

    private final String name;

    /**
     * Creates a new instance.
     * @param name the class name
     */
    public ClassDescription(String name) {
        this.name = name;
    }

    /**
     * Returns an instance.
     * @param aClass the reflective object
     * @return the related instance
     */
    public static ClassDescription of(Class<?> aClass) {
        if (aClass.isArray() || aClass.isPrimitive()) {
            throw new IllegalArgumentException("must be class or interface type");
        }
        return new ClassDescription(aClass.getName());
    }

    @Override
    public TypeKind getTypeKind() {
        return TypeKind.CLASS;
    }

    /**
     * Returns the class name.
     * @return the class name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the class simple name.
     * @return the class simple name
     */
    public String getSimpleName() {
        int index = Math.max(name.lastIndexOf('.'), name.lastIndexOf('$'));
        if (index < 0) {
            return name;
        }
        return name.substring(index + 1);
    }

    /**
     * Returns package name of the class.
     * @return the package name
     */
    public String getPackageName() {
        int index = name.lastIndexOf('.');
        if (index <= 0) {
            return null;
        }
        return name.substring(0, index);
    }

    @Override
    public Class<?> resolve(ClassLoader classLoader) throws ClassNotFoundException {
        return classLoader.loadClass(name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
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
        ClassDescription other = (ClassDescription) obj;
        if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Class({0})", //$NON-NLS-1$
                name);
    }
}
