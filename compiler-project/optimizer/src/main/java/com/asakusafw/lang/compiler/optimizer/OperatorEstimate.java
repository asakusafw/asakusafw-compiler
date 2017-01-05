/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.optimizer;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;

/**
 * Estimated information of {@link Operator}s.
 */
public interface OperatorEstimate {

    /**
     * Represents that data size is not sure.
     */
    double UNKNOWN_SIZE = Double.NaN;

    /**
     * Represents unknown estimate.
     */
    OperatorEstimate UNKNOWN = new OperatorEstimate() {
        @Override
        public double getSize(OperatorOutput port) {
            return UNKNOWN_SIZE;
        }
        @Override
        public double getSize(OperatorInput port) {
            return UNKNOWN_SIZE;
        }
        @Override
        public <T> T getAttribute(Class<T> attributeType) {
            return null;
        }
        @Override
        public <T> T getAttribute(OperatorOutput port, Class<T> attributeType) {
            return null;
        }
        @Override
        public <T> T getAttribute(OperatorInput port, Class<T> attributeType) {
            return null;
        }
    };

    /**
     * Returns the estimated size for the target input.
     * @param port the target port
     * @return the estimated size in bytes, or {@code NaN} if it is not sure
     */
    double getSize(OperatorInput port);

    /**
     * Returns the estimated size for the target output.
     * @param port the target port
     * @return the estimated size in bytes, or {@code NaN} if it is not sure
     */
    double getSize(OperatorOutput port);

    /**
     * Returns an attribute of the operator.
     * @param <T> the attribute type
     * @param attributeType the attribute type
     * @return the related attribute value, or {@code null} if there is no such an attribute
     */
    <T> T getAttribute(Class<T> attributeType);

    /**
     * Returns an attribute of the target input port.
     * @param <T> the attribute type
     * @param port the target port
     * @param attributeType the attribute type
     * @return the related attribute value, or {@code null} if there is no such an attribute
     */
    <T> T getAttribute(OperatorInput port, Class<T> attributeType);

    /**
     * Returns an attribute of the target output port.
     * @param <T> the attribute type
     * @param port the target port
     * @param attributeType the attribute type
     * @return the related attribute value, or {@code null} if there is no such an attribute
     */
    <T> T getAttribute(OperatorOutput port, Class<T> attributeType);
}
