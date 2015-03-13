package com.asakusafw.lang.compiler.model.description;

import java.text.MessageFormat;

/**
 * Represents an unknown object.
 * Unknown values cannot be {@link #resolve(ClassLoader) resolved}.
 */
public class UnknownValueDescription implements ValueDescription {

    private final ReifiableTypeDescription valueType;

    private final String label;

    /**
     * Creates a new instance.
     * @param valueType the original value type
     * @param label a value label
     */
    public UnknownValueDescription(ReifiableTypeDescription valueType, String label) {
        this.valueType = valueType;
        this.label = label;
    }

    /**
     * Creates a new instance.
     * @param value the value
     * @return the created instance
     */
    public static UnknownValueDescription of(Object value) {
        Class<?> type = value.getClass();
        return new UnknownValueDescription(ReifiableTypeDescription.of(type), String.valueOf(value));
    }

    @Override
    public ValueKind getValueKind() {
        return ValueKind.UNKNOWN;
    }

    @Override
    public ReifiableTypeDescription getValueType() {
        return valueType;
    }

    /**
     * Returns the label for this description.
     * @return the label for this description
     */
    public String getLabel() {
        return label;
    }

    @Override
    public Object resolve(ClassLoader classLoader) throws ReflectiveOperationException {
        throw new ReflectiveOperationException(MessageFormat.format(
                "cannot resolve unknown value: {0}", //$NON-NLS-1$
                this));
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Unknown(type={0}, label={1})", //$NON-NLS-1$
                valueType,
                label);
    }
}
