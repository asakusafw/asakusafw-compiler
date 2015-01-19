package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.List;

import com.asakusafw.lang.compiler.model.PropertyName;

/**
 * Dataset grouping information.
 * FIXME accept various expressions
 */
public class Group {

    private final List<PropertyName> grouping;

    private final List<Ordering> ordering;

    /**
     * Creates a new instance.
     * @param grouping the grouping properties
     * @param ordering the ordering properties
     */
    public Group(List<PropertyName> grouping, List<Ordering> ordering) {
        this.grouping = grouping;
        this.ordering = ordering;
    }

    /**
     * Returns the grouping properties.
     * @return the grouping properties
     */
    public List<PropertyName> getGrouping() {
        return grouping;
    }

    /**
     * Returns the sort expression in each group.
     * @return the sort expression
     */
    public List<Ordering> getOrdering() {
        return ordering;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Group(grouping={0}, sort={1})", //$NON-NLS-1$
                grouping,
                ordering);
    }

    /**
     * Represents an ordering atom.
     */
    public static final class Ordering {

        private final PropertyName propertyName;

        private final Direction direction;

        /**
         * Creates new instance.
         * @param propertyName the property name
         * @param direction the ordering direction
         */
        public Ordering(PropertyName propertyName, Direction direction) {
            this.propertyName = propertyName;
            this.direction = direction;
        }

        /**
         * Returns the property name.
         * @return the property name
         */
        public PropertyName getPropertyName() {
            return propertyName;
        }

        /**
         * Returns the ordering direction.
         * @return the ordering direction
         */
        public Direction getDirection() {
            return direction;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "{1}{2}", //$NON-NLS-1$
                    propertyName,
                    direction);
        }
    }

    /**
     * Represents a kind of ordering direction.
     */
    public static enum Direction {

        /**
         * Ascendant order.
         */
        ASCENDANT("+"), //$NON-NLS-1$

        /**
         * Descendant order.
         */
        DESCENDANT("-"), //$NON-NLS-1$
        ;

        private final String operator;

        private Direction(String symbol) {
            this.operator = symbol;
        }

        /**
         * Returns the direction operator.
         * @return the direction operator
         */
        public String getOperator() {
            return operator;
        }

        @Override
        public String toString() {
            return getOperator();
        }
    }
}
