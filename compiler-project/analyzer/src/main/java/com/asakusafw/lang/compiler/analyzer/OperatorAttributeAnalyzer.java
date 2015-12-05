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
package com.asakusafw.lang.compiler.analyzer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.asakusafw.lang.compiler.analyzer.model.OperatorSource;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.AbstractBuilder;

/**
 * Analyzes extra operator attributes from the original declarations.
 * @since 0.3.0
 */
public interface OperatorAttributeAnalyzer {

    /**
     * A null implementation of {@link OperatorAttributeAnalyzer} that always returns an empty map.
     */
    OperatorAttributeAnalyzer NULL = new OperatorAttributeAnalyzer() {

        @Override
        public AttributeMap analyze(OperatorSource source) {
            return new AttributeMap();
        }
    };

    /**
     * Extracts attributes from the target operator source.
     * @param source the target operator source
     * @return the extracted attribute map
     * @throws DiagnosticException if the operator source is something wrong
     */
    AttributeMap analyze(OperatorSource source) throws DiagnosticException;

    /**
     * Represents a map of attributes.
     */
    public static class AttributeMap {

        private final Map<Class<?>, Object> entity = new LinkedHashMap<>();

        /**
         * Returns an attribute.
         * @param <T> the attribute type
         * @param type the attribute type
         * @return the attribute value, or {@code null} if there is no such an attribute
         */
        public <T> T get(Class<T> type) {
            return type.cast(entity.get(type));
        }

        /**
         * Puts an attribute.
         * @param <T> the attribute type
         * @param type the attribute type
         * @param value the attribute value
         * @return this
         */
        public <T> AttributeMap put(Class<T> type, T value) {
            Objects.requireNonNull(type);
            Objects.requireNonNull(value);
            entity.put(type, value);
            return this;
        }

        void mergeTo(Operator.AbstractBuilder<?, ?> builder) {
            Objects.requireNonNull(builder);
            for (Map.Entry<Class<?>, Object> entry : entity.entrySet()) {
                mergeTo(builder, entry.getKey(), entry.getValue());
            }
        }

        private <T> void mergeTo(AbstractBuilder<?, ?> builder, Class<T> type, Object value) {
            builder.attribute(type, type.cast(value));
        }
    }
}
