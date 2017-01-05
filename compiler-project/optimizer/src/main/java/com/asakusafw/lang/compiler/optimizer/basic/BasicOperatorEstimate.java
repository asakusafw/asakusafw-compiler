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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.HashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;

/**
 * Estimated information of {@link Operator}s.
 */
public class BasicOperatorEstimate implements OperatorEstimate {

    private final Map<Class<?>, Object> attributes = new HashMap<>();

    private final Map<OperatorInput, PortInfo> inputs = new HashMap<>();

    private final Map<OperatorOutput, PortInfo> outputs = new HashMap<>();

    @Override
    public double getSize(OperatorInput port) {
        return getSize(inputs, port);
    }

    @Override
    public double getSize(OperatorOutput port) {
        return getSize(outputs, port);
    }

    @Override
    public <T> T getAttribute(Class<T> attributeType) {
        return getAttribute(attributes, attributeType);
    }

    @Override
    public <T> T getAttribute(OperatorInput port, Class<T> attributeType) {
        return getAttribute(inputs, port, attributeType);
    }

    @Override
    public <T> T getAttribute(OperatorOutput port, Class<T> attributeType) {
        return getAttribute(outputs, port, attributeType);
    }

    private static <T> double getSize(Map<T, PortInfo> ports, T port) {
        PortInfo info = ports.get(port);
        if (info == null) {
            return UNKNOWN_SIZE;
        }
        return info.size;
    }

    private static <T, A> A getAttribute(Map<T, PortInfo> ports, T port, Class<A> attributeType) {
        PortInfo info = ports.get(port);
        if (info == null) {
            return null;
        }
        return getAttribute(info.attributes, attributeType);
    }

    private static <A> A getAttribute(Map<Class<?>, ?> attributes, Class<A> attributeType) {
        Object attribute = attributes.get(attributeType);
        if (attribute == null) {
            return null;
        }
        return attributeType.cast(attribute);
    }

    /**
     * Sets the input estimated size.
     * @param port the target port
     * @param size the estimated size
     */
    public void putSize(OperatorInput port, double size) {
        putSize(inputs, port, size);
    }

    /**
     * Sets the output estimated size.
     * @param port the target port
     * @param size the estimated size
     */
    public void putSize(OperatorOutput port, double size) {
        putSize(outputs, port, size);
    }

    /**
     * Sets the input attribute.
     * @param <T> the attribute type
     * @param type the attribute type
     * @param value the attribute value
     */
    public <T> void putAttribute(Class<T> type, T value) {
        attributes.put(type, value);
    }

    /**
     * Sets the input attribute.
     * @param <T> the attribute type
     * @param port the target port
     * @param type the attribute type
     * @param value the attribute value
     */
    public <T> void putAttribute(OperatorInput port, Class<T> type, T value) {
        putAttribute(inputs, port, type, value);
    }

    /**
     * Sets the output attribute.
     * @param <T> the attribute type
     * @param port the target port
     * @param type the attribute type
     * @param value the attribute value
     */
    public <T> void putAttribute(OperatorOutput port, Class<T> type, T value) {
        putAttribute(outputs, port, type, value);
    }

    private static <T> void putSize(Map<T, PortInfo> ports, T port, double size) {
        prepare(ports, port).size = size;
    }

    private static <T, A> void putAttribute(Map<T, PortInfo> ports, T port, Class<A> type, A value) {
        prepare(ports, port).attributes.put(type, value);
    }

    private static <T> PortInfo prepare(Map<T, PortInfo> ports, T port) {
        PortInfo info = ports.get(port);
        if (info == null) {
            info = new PortInfo();
            ports.put(port, info);
        }
        return info;
    }

    static class PortInfo {

        double size = OperatorEstimate.UNKNOWN_SIZE;

        final Map<Class<?>, Object> attributes = new HashMap<>();
    }
}
