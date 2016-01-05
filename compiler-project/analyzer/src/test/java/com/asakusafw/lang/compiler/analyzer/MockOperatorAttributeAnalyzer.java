/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.analyzer.model.OperatorSource;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.google.common.base.Objects;

/**
 * Mock {@link OperatorAttributeAnalyzer}.
 */
public class MockOperatorAttributeAnalyzer implements OperatorAttributeAnalyzer {

    @Override
    public AttributeMap analyze(OperatorSource source) throws DiagnosticException {
        return new AttributeMap().put(MockAttribute.class, new MockAttribute("OK"));
    }

    /**
     * The mock attribute.
     */
    public static class MockAttribute {

        private final String value;

        /**
         * Creates a new instance.
         * @param value the value
         */
        public MockAttribute(String value) {
            this.value = value;
        }

        /**
         * Returns the value.
         * @return the value
         */
        public String getValue() {
            return value;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);
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
            MockAttribute other = (MockAttribute) obj;
            return Objects.equal(value, other.value);
        }
    }
}
