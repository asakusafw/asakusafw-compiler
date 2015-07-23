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
package com.asakusafw.lang.inspection.gui;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

import javax.swing.tree.TreeNode;

/**
 * An abstract implementation of {@link TreeNode}.
 * @param <T> the user object type
 */
public abstract class AbstractTreeNode<T> implements TreeNode {

    private final TreeNode parent;

    private final T userObject;

    private List<? extends TreeNode> elements;

    /**
     * Creates a new instance.
     * @param parent the parent node (nullable)
     * @param userObject the user object for this node
     */
    public AbstractTreeNode(TreeNode parent, T userObject) {
        this.parent = parent;
        this.userObject = userObject;
    }

    /**
     * Returns the user object.
     * @return the user object
     */
    public T getUserObject() {
        return userObject;
    }

    @Override
    public String toString() {
        return String.valueOf(userObject);
    }

    /**
     * Prepares the child elements of this node.
     * @return the children of this
     */
    protected abstract List<? extends TreeNode> prepareElements();

    /**
     * Returns child elements of this node.
     * @return the children
     */
    public synchronized List<? extends TreeNode> getElements() {
        if (elements == null) {
            elements = prepareElements();
        }
        return elements;
    }

    @Override
    public TreeNode getParent() {
        return parent;
    }

    @Override
    public TreeNode getChildAt(int childIndex) {
        return getElements().get(childIndex);
    }

    @Override
    public int getChildCount() {
        return getElements().size();
    }

    @Override
    public int getIndex(TreeNode node) {
        return getElements().indexOf(node);
    }

    @Override
    public boolean getAllowsChildren() {
        return isLeaf() == false;
    }

    @Override
    public boolean isLeaf() {
        return getElements().isEmpty();
    }

    @Override
    public Enumeration<? extends TreeNode> children() {
        return Collections.enumeration(getElements());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getClass().hashCode();
        result = prime * result + Objects.hashCode(userObject.hashCode());
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
        AbstractTreeNode<?> other = (AbstractTreeNode<?>) obj;
        if (!Objects.equals(userObject, other.userObject)) {
            return false;
        }
        return true;
    }
}
