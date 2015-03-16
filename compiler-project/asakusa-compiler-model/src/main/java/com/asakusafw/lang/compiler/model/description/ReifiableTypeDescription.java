package com.asakusafw.lang.compiler.model.description;

/**
 * Represents a reifiable type (can be represented in {@link Class}).
 */
public abstract class ReifiableTypeDescription implements TypeDescription, ValueDescription {

    @Override
    public final ValueKind getValueKind() {
        return ValueKind.TYPE;
    }

    @Override
    public final ReifiableTypeDescription getValueType() {
        return of(Class.class);
    }

    /**
     * Returns an instance.
     * @param aClass the reflective object
     * @return the related instance
     */
    public static ReifiableTypeDescription of(Class<?> aClass) {
        if (aClass.isPrimitive()) {
            return BasicTypeDescription.of(aClass);
        } else if (aClass.isArray()) {
            return ArrayTypeDescription.of(aClass);
        } else {
            return ClassDescription.of(aClass);
        }
    }

    /**
     * Resolves this reference and returns the related reflective object.
     * @param classLoader the class loader
     * @return the resolved object
     * @throws ClassNotFoundException if the target class is not found in the class loader
     */
    @Override
    public abstract Class<?> resolve(ClassLoader classLoader) throws ClassNotFoundException;
}
