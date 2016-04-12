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
package com.asakusafw.lang.inspection.gui;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.swing.tree.TreeNode;

import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Tree node model for {@link InspectionNode}.
 */
public class DetailTreeNode extends AbstractTreeNode<InspectionNode> {

    /**
     * Creates a new instance.
     * @param node the target node
     */
    public DetailTreeNode(InspectionNode node) {
        super(null, node);
    }

    @Override
    public String toString() {
        InspectionNode current = getUserObject();
        return String.format("%s (%s)", current.getId(), current.getTitle()); //$NON-NLS-1$
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    protected List<? extends TreeNode> prepareElements() {
        InspectionNode node = getUserObject();
        List<TreeNode> elements = new ArrayList<>();
        elements.add(PropertyNode.of(this, node.getProperties()));
        elements.add(buildPorts(node.getInputs().values(), Direction.INPUT));
        elements.add(buildPorts(node.getOutputs().values(), Direction.OUTPUT));
        return elements;
    }

    private TreeNode buildPorts(Collection<InspectionNode.Port> ports, Direction direction) {
        ContainerNode container = new ContainerNode(this, direction);
        for (InspectionNode.Port port : ports) {
            container.add(new PortNode(container, this, port, direction));
        }
        return container;
    }

    private static class ContainerNode extends AbstractTreeNode<Object> {

        private final List<TreeNode> elements = new ArrayList<>();

        ContainerNode(TreeNode parent, Object userObject) {
            super(parent, userObject);
        }

        public void add(TreeNode element) {
            elements.add(element);
        }

        @Override
        public boolean isLeaf() {
            return false;
        }

        @Override
        protected List<? extends TreeNode> prepareElements() {
            return elements;
        }
    }

    /**
     * Represents a port.
     */
    public static class PortNode extends AbstractTreeNode<InspectionNode.Port> {

        private final DetailTreeNode owner;

        private final Direction direction;

        PortNode(TreeNode parent, DetailTreeNode owner, InspectionNode.Port port, Direction direction) {
            super(parent, port);
            this.owner = owner;
            this.direction = direction;
        }

        /**
         * Returns the owner of this port.
         * @return the owner
         */
        public DetailTreeNode getOwner() {
            return owner;
        }

        /**
         * Returns the direction of this port.
         * @return the direction
         */
        public Direction getDirection() {
            return direction;
        }

        @Override
        public String toString() {
            return getUserObject().getId();
        }

        @Override
        protected List<? extends TreeNode> prepareElements() {
            List<TreeNode> elements = new ArrayList<>();
            elements.add(PropertyNode.of(this, getUserObject().getProperties()));
            elements.add(buildReferences());
            return elements;
        }

        private TreeNode buildReferences() {
            ContainerNode container = new ContainerNode(this, "opposites");
            for (InspectionNode.PortReference ref : getUserObject().getOpposites()) {
                container.add(new PortReferenceNode(container, this, ref));
            }
            return container;
        }
    }

    /**
     * Represents a port reference.
     */
    public static class PortReferenceNode extends AbstractTreeNode<InspectionNode.PortReference> {

        private final PortNode owner;

        PortReferenceNode(TreeNode parent, PortNode owner, InspectionNode.PortReference reference) {
            super(parent, reference);
            this.owner = owner;
        }

        /**
         * Returns the owner of this port.
         * @return the owner
         */
        public PortNode getOwner() {
            return owner;
        }

        @Override
        public PortNode getParent() {
            return owner;
        }

        @Override
        protected List<? extends TreeNode> prepareElements() {
            return Collections.emptyList();
        }
    }

    /**
     * Represents a property.
     */
    public static class PropertyNode extends AbstractTreeNode<Map.Entry<String, String>> {

        PropertyNode(TreeNode parent, Map.Entry<String, String> entry) {
            super(parent, entry);
        }

        static TreeNode of(TreeNode parent, Map<String, String> properties) {
            ContainerNode container = new ContainerNode(parent, "properties"); //$NON-NLS-1$
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                container.add(new PropertyNode(parent, entry));
            }
            return container;
        }

        @Override
        public String toString() {
            Map.Entry<String, String> entry = getUserObject();
            return MessageFormat.format("{0}: {1}", entry.getKey(), entry.getValue()); //$NON-NLS-1$
        }

        @Override
        protected List<? extends TreeNode> prepareElements() {
            return Collections.emptyList();
        }
    }

    /**
     * The port direction.
     */
    public enum Direction {

        /**
         * input ports.
         */
        INPUT,

        /**
         * output ports.
         */
        OUTPUT,
        ;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }
}
