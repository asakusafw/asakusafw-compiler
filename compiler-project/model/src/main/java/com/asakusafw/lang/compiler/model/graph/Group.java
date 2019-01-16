/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.lang.compiler.model.PropertyName;

/**
 * Dataset grouping information.
 * FIXME accept various expressions
 * @see Groups
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + grouping.hashCode();
        result = prime * result + ordering.hashCode();
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
        Group other = (Group) obj;
        if (!grouping.equals(other.grouping)) {
            return false;
        }
        if (!ordering.equals(other.ordering)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        List<String> elements = new ArrayList<>();
        for (PropertyName name : grouping) {
            elements.add(String.format("=%s", name)); //$NON-NLS-1$
        }
        for (Ordering order : ordering) {
            elements.add(order.toString());
        }
        return MessageFormat.format(
                "Group{0}", //$NON-NLS-1$
                elements);
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
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + direction.hashCode();
            result = prime * result + propertyName.hashCode();
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
            Ordering other = (Ordering) obj;
            if (direction != other.direction) {
                return false;
            }
            if (!propertyName.equals(other.propertyName)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "{1}{0}", //$NON-NLS-1$
                    propertyName,
                    direction);
        }
    }

    /**
     * Represents a kind of ordering direction.
     */
    public enum Direction {

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

        Direction(String symbol) {
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
