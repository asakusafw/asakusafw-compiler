/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;

/**
 * Represents a mapping between properties.
 */
public class PropertyMapping {

    private final OperatorInput sourcePort;

    private final PropertyName sourceProperty;

    private final OperatorOutput destinationPort;

    private final PropertyName destinationProperty;

    /**
     * Creates a new instance.
     * @param sourcePort the source port
     * @param sourceProperty the source property
     * @param destinationPort the destination port
     * @param destinationProperty the destination property
     */
    public PropertyMapping(
            OperatorInput sourcePort,
            PropertyName sourceProperty,
            OperatorOutput destinationPort,
            PropertyName destinationProperty) {
        this.sourcePort = sourcePort;
        this.sourceProperty = sourceProperty;
        this.destinationPort = destinationPort;
        this.destinationProperty = destinationProperty;
    }

    /**
     * Returns the source port.
     * @return the source port
     */
    public OperatorInput getSourcePort() {
        return sourcePort;
    }

    /**
     * Returns the source property.
     * @return the source property
     */
    public PropertyName getSourceProperty() {
        return sourceProperty;
    }

    /**
     * Returns the destination port.
     * @return the destination port
     */
    public OperatorOutput getDestinationPort() {
        return destinationPort;
    }

    /**
     * Returns the destination property.
     * @return the destination property
     */
    public PropertyName getDestinationProperty() {
        return destinationProperty;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "PropertyMapping({0}[{1}]->{2}[{3}])", //$NON-NLS-1$
                sourcePort,
                sourceProperty,
                destinationPort,
                destinationProperty);
    }
}
