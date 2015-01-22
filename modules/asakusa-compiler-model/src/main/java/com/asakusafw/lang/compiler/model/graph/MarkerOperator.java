package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents a marker operator.
 * @see OperatorAttribute
 */
public final class MarkerOperator extends Operator {

    final Map<Class<?>, Object> attributes = new HashMap<>();

    private MarkerOperator() {
        return;
    }

    @Override
    public MarkerOperator copy() {
        MarkerOperator operator = new MarkerOperator();
        for (Map.Entry<Class<?>, Object> entry : attributes.entrySet()) {
            Class<?> key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof OperatorAttribute) {
                value = ((OperatorAttribute) value).copy();
                assert key.isInstance(value);
            }
            operator.attributes.put(key, value);
        }
        return copyAttributesTo(operator);
    }

    /**
     * Creates a new operator builder.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder(new MarkerOperator());
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.MARKER;
    }

    /**
     * Returns the all attribute types which this operator has.
     * @return the all attribute types
     */
    public Set<Class<?>> getAttributeTypes() {
        return Collections.unmodifiableSet(attributes.keySet());
    }

    /**
     * Returns an attribute.
     * @param <T> the attribute type
     * @param attributeType the attribute type
     * @return the attribute value, or {@code null} if the operator has no such an attribute
     */
    public <T> T getAttribute(Class<T> attributeType) {
        Object value = attributes.get(attributeType);
        if (value == null) {
            return null;
        } else {
            return attributeType.cast(value);
        }
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "MarkerOperator({0}attributes)", //$NON-NLS-1$
                attributes.size());
    }

    /**
     * A builder for {@link MarkerOperator}.
     */
    public static final class Builder extends AbstractBuilder<MarkerOperator, Builder> {

        Builder(MarkerOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }

        /**
         * Adds an attribute to the building operator.
         * When clients {@link MarkerOperator#copy() copy operators},
         * only attributes implementing {@link OperatorAttribute}
         * are also copied using {@link OperatorAttribute#copy()} method.
         * @param attributeType attribute type
         * @param attributeValue attribute value
         * @param <T> attribute type
         * @return this
         */
        public <T> Builder attribute(Class<T> attributeType, T attributeValue) {
            MarkerOperator owner = getOwner();
            if (attributeValue == null) {
                owner.attributes.remove(attributeType);
            } else {
                owner.attributes.put(attributeType, attributeValue);
            }
            return this;
        }
    }
}
