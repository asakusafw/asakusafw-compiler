package com.asakusafw.lang.compiler.model.description;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an immediate value.
 */
public class ImmediateDescription implements ValueDescription {

    private static final Map<Class<?>, BasicTypeDescription> BOXED;
    static {
        Map<Class<?>, BasicTypeDescription> map = new HashMap<>();
        for (BasicTypeDescription.BasicTypeKind kind : BasicTypeDescription.BasicTypeKind.values()) {
            map.put(kind.getWrapperType(), new BasicTypeDescription(kind));
        }
        BOXED = map;
    }

    private final ReifiableTypeDescription valueType;

    private final Object value;

    /**
     * Creates a new instance.
     * @param valueType the value type
     * @param value the value
     */
    public ImmediateDescription(ReifiableTypeDescription valueType, Object value) {
        this.valueType = valueType;
        this.value = value;
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(Object value) {
        if (value == null) {
            throw new IllegalArgumentException();
        }
        Class<?> aClass = value.getClass();
        ReifiableTypeDescription type;
        if (BOXED.containsKey(aClass)) {
            type = BOXED.get(aClass);
        } else if (aClass == String.class) {
            type = Descriptions.classOf(String.class);
        } else {
            throw new IllegalArgumentException(MessageFormat.format(
                    "immediate value must be either boxed value or String: {0}", //$NON-NLS-1$
                    aClass.getName()));
        }
        return new ImmediateDescription(type, value);
    }

    /**
     * Returns whether the target type is boxed type or not.
     * @param aClass the target type
     * @return {@code true} if the target type is boxed, otherwise {@code false}
     */
    public static final boolean isBoxed(Class<?> aClass) {
        return BOXED.containsKey(aClass);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(boolean value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(boolean.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(byte value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(byte.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(short value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(short.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(int value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(int.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(long value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(long.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(float value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(float.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(double value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(double.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(char value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(char.class), value);
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static final ImmediateDescription of(String value) {
        return new ImmediateDescription(ReifiableTypeDescription.of(String.class), value);
    }

    @Override
    public ValueKind getValueKind() {
        return ValueKind.IMMEDIATE;
    }

    @Override
    public ReifiableTypeDescription getValueType() {
        return valueType;
    }

    /**
     * Returns the value.
     * @return the value
     */
    public Object getValue() {
        return value;
    }

    @Override
    public Object resolve(ClassLoader classLoader) throws ReflectiveOperationException {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        ImmediateDescription other = (ImmediateDescription) obj;
        if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}