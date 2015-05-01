/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacteristics;

/**
 * Represents a class of {@link Operator}.
 */
public class OperatorClass implements OperatorCharacteristics {

    private final Operator operator;

    private final InputType primaryInputType;

    private final Set<OperatorAttribute> attributes;

    private final Map<OperatorInput, Set<InputAttribute>> inputs;

    private final Map<OperatorOutput, Set<OutputAttribute>> outputs;

    OperatorClass(
            Operator operator,
            InputType primaryInputType,
            Set<OperatorAttribute> attributes,
            Map<OperatorInput, Set<InputAttribute>> inputs,
            Map<OperatorOutput, Set<OutputAttribute>> outputs) {
        this.operator = operator;
        this.primaryInputType = primaryInputType;
        this.attributes = attributes;
        this.inputs = inputs;
        this.outputs = outputs;
    }

    /**
     * Creates a new builder for this class.
     * @param operator the target operator
     * @param type the primary input type
     * @return the builder
     */
    public static Builder builder(Operator operator, InputType type) {
        return new Builder(operator, type);
    }

    /**
     * Returns the target operator.
     * @return the target operator
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Returns the primary input type for the target operator.
     * @return the primary input type
     */
    public InputType getPrimaryInputType() {
        return primaryInputType;
    }

    /**
     * Returns the all operator inputs.
     * @return the all operator inputs
     */
    public Set<OperatorInput> getInputs() {
        return inputs.keySet();
    }

    /**
     * Returns the all operator outputs.
     * @return the all operator outputs
     */
    public Set<OperatorOutput> getOutputs() {
        return outputs.keySet();
    }

    /**
     * Returns the attributes for the operator.
     * @return the operator attributes
     */
    public Set<OperatorAttribute> getAttributes() {
        return attributes;
    }

    /**
     * Returns the attributes for the target input.
     * @param port the target port
     * @return the corresponded attributes
     */
    public Set<InputAttribute> getAttributes(OperatorInput port) {
        Set<InputAttribute> attrs = inputs.get(port);
        if (attrs == null) {
            throw new IllegalArgumentException();
        }
        return attrs;
    }

    /**
     * Returns the attributes for the target output.
     * @param port the target port
     * @return the corresponded attributes
     */
    public Set<OutputAttribute> getAttributes(OperatorOutput port) {
        Set<OutputAttribute> attrs = outputs.get(port);
        if (attrs == null) {
            throw new IllegalArgumentException();
        }
        return attrs;
    }

    /**
     * Returns the primary operator inputs.
     * @return the primary operator inputs
     * @see #getPrimaryInputType()
     */
    public Set<OperatorInput> getPrimaryInputs() {
        return filter(inputs, InputAttribute.PRIMARY, true);
    }

    /**
     * Returns the secondary operator inputs ({@literal (a.k.a. side-data inputs)}).
     * @return the secondary operator inputs
     */
    public Set<OperatorInput> getSecondaryInputs() {
        return filter(inputs, InputAttribute.PRIMARY, false);
    }

    private <K, V> Set<K> filter(Map<K, ? extends Set<? extends V>> map, V attribute, boolean truth) {
        Set<K> results = new LinkedHashSet<>();
        for (Map.Entry<K, ? extends Set<? extends V>> entry : map.entrySet()) {
            if (entry.getValue().contains(attribute) == truth) {
                results.add(entry.getKey());
            }
        }
        return results;
    }

    /**
     * Represents a kind of input type.
     */
    public enum InputType {

        /**
         * There are no inputs, or the operator ignores the inputs.
         */
        NOTHING,

        /**
         * Processes individual records.
         */
        RECORD,

        /**
         * Processes individual groups.
         */
        GROUP,
        ;
    }

    /**
     * Represents an attribute for {@link Operator}.
     */
    public enum OperatorAttribute {

        // no special members
    }

    /**
     * Represents an attribute for {@link OperatorInput}.
     */
    public enum InputAttribute {

        /**
         * The input is primary.
         */
        PRIMARY,

        /**
         * Each input group must be sorted.
         */
        SORTED,

        /**
         * Each input group will be aggregated.
         */
        AGGREATE,

        /**
         * Each input group accepts partial reduction.
         */
        PARTIAL_REDUCTION,
    }

    /**
     * Represents an attribute for {@link OperatorOutput}.
     */
    public enum OutputAttribute {

        // no special members
    }

    /**
     * A builder for {@link OperatorClass}.
     */
    public static class Builder {

        private final Operator operator;

        private final InputType primaryInputType;

        private final EnumSet<OperatorAttribute> attributes = EnumSet.noneOf(OperatorAttribute.class);

        private final Map<OperatorInput, EnumSet<InputAttribute>> inputs;

        private final Map<OperatorOutput, EnumSet<OutputAttribute>> outputs;

        Builder(Operator operator, InputType type) {
            this.operator = operator;
            this.primaryInputType = type;
            this.inputs = prepare(operator.getInputs(), InputAttribute.class);
            this.outputs = prepare(operator.getOutputs(), OutputAttribute.class);
        }

        /**
         * Puts an attribute for the operator.
         * @param attribute the attribute
         * @return this
         */
        public Builder with(OperatorAttribute attribute) {
            attributes.add(attribute);
            return this;
        }

        /**
         * Puts an attribute for the input.
         * @param port the target port
         * @param attribute the attribute
         * @return this
         */
        public Builder with(OperatorInput port, InputAttribute attribute) {
            EnumSet<InputAttribute> attrs = inputs.get(port);
            if (attrs == null) {
                throw new IllegalArgumentException();
            }
            attrs.add(attribute);
            return this;
        }

        /**
         * Puts an attribute for the output.
         * @param port the target port
         * @param attribute the attribute
         * @return this
         */
        public Builder with(OperatorOutput port, OutputAttribute attribute) {
            EnumSet<OutputAttribute> attrs = outputs.get(port);
            if (attrs == null) {
                throw new IllegalArgumentException();
            }
            attrs.add(attribute);
            return this;
        }

        /**
         * Builds a new instance.
         * @return the built instance
         */
        public OperatorClass build() {
            return new OperatorClass(
                    operator,
                    primaryInputType,
                    freeze(attributes),
                    freeze(inputs),
                    freeze(outputs));
        }

        private static <K, V extends Enum<V>> Map<K, EnumSet<V>> prepare(List<K> keys, Class<V> valueType) {
            Map<K, EnumSet<V>> results = new LinkedHashMap<>();
            for (K key : keys) {
                results.put(key, EnumSet.noneOf(valueType));
            }
            return results;
        }

        private static <K, V extends Enum<V>> Map<K, Set<V>> freeze(Map<K, EnumSet<V>> map) {
            if (map.isEmpty()) {
                return Collections.emptyMap();
            } else if (map.size() == 1) {
                Map.Entry<K, EnumSet<V>> entry = map.entrySet().iterator().next();
                return Collections.singletonMap(entry.getKey(), freeze(entry.getValue()));
            } else {
                Map<K, Set<V>> results = new LinkedHashMap<>();
                for (Map.Entry<K, EnumSet<V>> entry : map.entrySet()) {
                    results.put(entry.getKey(), freeze(entry.getValue()));
                }
                return results;
            }
        }

        private static <V extends Enum<V>> Set<V> freeze(EnumSet<V> set) {
            return Collections.unmodifiableSet(set);
        }
    }
}
