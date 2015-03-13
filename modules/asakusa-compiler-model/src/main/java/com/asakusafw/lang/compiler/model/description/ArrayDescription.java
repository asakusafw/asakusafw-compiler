package com.asakusafw.lang.compiler.model.description;

import java.lang.reflect.Array;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents an array of values.
 */
public class ArrayDescription implements ValueDescription {

    private final ArrayTypeDescription arrayType;

    private final List<ValueDescription> elements;

    /**
     * Creates a new instance.
     * @param arrayType the array type
     * @param elements the array elements
     */
    public ArrayDescription(
            ArrayTypeDescription arrayType,
            List<? extends ValueDescription> elements) {
        this.arrayType = arrayType;
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    /**
     * Creates a new instance.
     * @param array the array object
     * @return the created instance
     */
    public static ArrayDescription of(Object array) {
        Class<?> arrayType = array.getClass();
        if (arrayType.isArray() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "must be an array: {0}", //$NON-NLS-1$
                    array));
        }
        List<ValueDescription> elements = new ArrayList<>();
        for (int i = 0, n = Array.getLength(array); i < n; i++) {
            ValueDescription element = Descriptions.valueOf(Array.get(array, i));
            elements.add(element);
        }
        return new ArrayDescription(ArrayTypeDescription.of(arrayType), elements);
    }

    @Override
    public ValueKind getValueKind() {
        return ValueKind.ARRAY;
    }

    @Override
    public ReifiableTypeDescription getValueType() {
        return arrayType;
    }

    /**
     * Returns the array elements.
     * @return the array elements
     */
    public List<ValueDescription> getElements() {
        return elements;
    }

    @Override
    public Object resolve(ClassLoader classLoader) throws ReflectiveOperationException {
        Class<?> type = arrayType.getComponentType().resolve(classLoader);
        Object array = Array.newInstance(type, elements.size());
        for (int i = 0, n = elements.size(); i < n; i++) {
            Array.set(array, i, elements.get(i).resolve(classLoader));
        }
        return array;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + arrayType.hashCode();
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
        ArrayDescription other = (ArrayDescription) obj;
        if (!arrayType.equals(other.arrayType)) {
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
                "Array(type={0}, size={1})", //$NON-NLS-1$
                arrayType,
                elements.size());
    }
}
