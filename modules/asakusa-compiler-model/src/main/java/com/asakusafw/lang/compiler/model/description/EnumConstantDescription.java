package com.asakusafw.lang.compiler.model.description;

import java.text.MessageFormat;

/**
 * Represents an enum constant.
 */
public class EnumConstantDescription implements ValueDescription {

    private final ClassDescription declaringClass;

    private final String name;

    /**
     * Creates a new instance.
     * @param declaringClass the declaring class
     * @param name the constant name
     */
    public EnumConstantDescription(ClassDescription declaringClass, String name) {
        this.declaringClass = declaringClass;
        this.name = name;
    }

    /**
     * Creates a new instance.
     * @param value the enum constant
     * @return the created instance
     */
    public static EnumConstantDescription of(Enum<?> value) {
        ClassDescription declaring = Descriptions.classOf(value.getDeclaringClass());
        String name = value.name();
        return new EnumConstantDescription(declaring, name);
    }

    @Override
    public ValueKind getValueKind() {
        return ValueKind.ENUM_CONSTANT;
    }

    @Override
    public ReifiableTypeDescription getValueType() {
        return getDeclaringClass();
    }

    /**
     * Returns the declaring enum type.
     * @return the declaring enum type
     */
    public ClassDescription getDeclaringClass() {
        return declaringClass;
    }

    /**
     * Returns the constant name.
     * @return the constant name
     */
    public String getName() {
        return name;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Object resolve(ClassLoader classLoader) throws ReflectiveOperationException {
        Class declaring = declaringClass.resolve(classLoader);
        try {
            return Enum.valueOf(declaring, name);
        } catch (IllegalArgumentException e) {
            throw new ReflectiveOperationException(MessageFormat.format(
                    "enum constant is not found: {0}#{1}", //$NON-NLS-1$
                    declaringClass.getName(),
                    name), e);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + declaringClass.hashCode();
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
        EnumConstantDescription other = (EnumConstantDescription) obj;
        if (!declaringClass.equals(other.declaringClass)) {
            return false;
        }
        if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "EnumConstant({0}#{1})", //$NON-NLS-1$
                declaringClass.getName(),
                name);
    }
}
