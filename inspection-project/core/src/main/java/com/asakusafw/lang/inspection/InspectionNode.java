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
package com.asakusafw.lang.inspection;

import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents a node for inspection.
 */
public class InspectionNode {

    private final String id;

    private final String title;

    private final Map<String, Port> inputs = new LinkedHashMap<>();

    private final Map<String, Port> outputs = new LinkedHashMap<>();

    private final Map<String, String> properties = new LinkedHashMap<>();

    private final Map<String, InspectionNode> elements = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @param id the node ID
     * @param title the title of the node
     */
    public InspectionNode(String id, String title) {
        this.id = id;
        this.title = title;
    }

    /**
     * Returns the ID of this node.
     * @return the node ID
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the title of this node.
     * @return the node title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Returns the input ports.
     * @return the input ports
     */
    public Map<String, Port> getInputs() {
        return inputs;
    }

    /**
     * Returns the output ports.
     * @return the output ports
     */
    public Map<String, Port> getOutputs() {
        return outputs;
    }

    /**
     * Returns the node properties.
     * @return the node properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Returns the child elements of this node.
     * @return the child elements
     */
    public Map<String, InspectionNode> getElements() {
        return elements;
    }

    /**
     * Adds an input port.
     * @param port the input port
     * @return this
     */
    public InspectionNode withInput(Port port) {
        inputs.put(port.getId(), port);
        return this;
    }

    /**
     * Adds an output port.
     * @param port the output port
     * @return this
     */
    public InspectionNode withOutput(Port port) {
        outputs.put(port.getId(), port);
        return this;
    }

    /**
     * Adds an element node.
     * @param element the element node
     * @return this
     */
    public InspectionNode withElement(InspectionNode element) {
        elements.put(element.getId(), element);
        return this;
    }

    /**
     * Adds a property.
     * @param key the property key
     * @param value the property value
     * @return this
     */
    public InspectionNode withProperty(String key, String value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "{1}@{0}{2}", //$NON-NLS-1$
                getId(),
                getTitle(),
                getProperties());
    }

    /**
     * Represents a port of {@link InspectionNode}.
     */
    public static class Port {

        private final String id;

        private final Map<String, String> properties = new LinkedHashMap<>();

        private final Set<PortReference> opposites = new LinkedHashSet<>();

        /**
         * Creates a new instance.
         * @param id the port ID
         */
        public Port(String id) {
            this.id = id;
        }

        /**
         * Returns the ID of this port.
         * @return the port ID
         */
        public String getId() {
            return id;
        }

        /**
         * Returns the properties.
         * @return the properties
         */
        public Map<String, String> getProperties() {
            return properties;
        }

        /**
         * Returns the references of the opposite ports.
         * @return the opposite ports
         */
        public Set<PortReference> getOpposites() {
            return opposites;
        }

        /**
         * Adds a property.
         * @param key the property key
         * @param value the property value
         * @return this
         */
        public Port withProperty(String key, String value) {
            properties.put(key, value);
            return this;
        }

        /**
         * Adds an opposite port reference.
         * @param reference the target port
         * @return this
         */
        public Port withOpposite(PortReference reference) {
            opposites.add(reference);
            return this;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "{0}{1}", //$NON-NLS-1$
                    getId(),
                    getProperties());
        }
    }

    /**
     * Represents a reference of port.
     */
    public static class PortReference {

        private final String nodeId;

        private final String portId;

        /**
         * Creates a new instance.
         * @param nodeId the target node ID
         * @param portId the target port ID
         */
        public PortReference(String nodeId, String portId) {
            this.nodeId = nodeId;
            this.portId = portId;
        }

        /**
         * Returns the target node ID.
         * @return the node ID
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * Returns the target port ID.
         * @return the port ID
         */
        public String getPortId() {
            return portId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + nodeId.hashCode();
            result = prime * result + portId.hashCode();
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
            PortReference other = (PortReference) obj;
            if (!nodeId.equals(other.nodeId)) {
                return false;
            }
            if (!portId.equals(other.portId)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "{0}.{1}", //$NON-NLS-1$
                    getNodeId(), getPortId());
        }
    }
}
