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
package com.asakusafw.lang.inspection.gui;

import java.util.ArrayList;
import java.util.List;

import javax.swing.tree.TreeNode;

import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Tree node model for {@link InspectionNode}.
 */
public class InspectionTreeNode extends AbstractTreeNode<InspectionNode> {

    private final InspectionTreeNode parent;

    /**
     * Creates a new instance.
     * @param root the root node
     */
    public InspectionTreeNode(InspectionNode root) {
        this(null, root);
    }

    private InspectionTreeNode(InspectionTreeNode parent, InspectionNode current) {
        super(parent, current);
        this.parent = parent;
    }

    @Override
    public String toString() {
        InspectionNode current = getUserObject();
        return String.format("%s (%s)", current.getId(), current.getTitle()); //$NON-NLS-1$
    }

    @Override
    public TreeNode getParent() {
        return parent;
    }

    @Override
    protected List<InspectionTreeNode> prepareElements() {
        InspectionNode current = getUserObject();
        List<InspectionTreeNode> children = new ArrayList<>(current.getElements().size());
        for (InspectionNode node : current.getElements().values()) {
            children.add(new InspectionTreeNode(this, node));
        }
        return children;
    }
}
