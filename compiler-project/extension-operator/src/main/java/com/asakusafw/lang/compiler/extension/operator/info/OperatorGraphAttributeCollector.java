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
package com.asakusafw.lang.compiler.extension.operator.info;

import com.asakusafw.info.graph.Node;
import com.asakusafw.info.operator.OperatorGraphAttribute;
import com.asakusafw.lang.compiler.info.AttributeCollector;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Collects {@link OperatorGraphAttribute}.
 * @since 0.4.2
 */
public class OperatorGraphAttributeCollector implements AttributeCollector {

    @Override
    public void process(Context context, Jobflow jobflow) {
        Node root = new Node();
        new OperatorGraphConverter().process(jobflow.getOperatorGraph(), root);
        context.putAttribute(new OperatorGraphAttribute(root));
    }
}
