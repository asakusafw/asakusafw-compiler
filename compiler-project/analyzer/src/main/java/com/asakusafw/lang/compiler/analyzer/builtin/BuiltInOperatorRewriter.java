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
package com.asakusafw.lang.compiler.analyzer.builtin;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.basic.CompositeOperatorRewriter;

/**
 * An implementation of {@link OperatorRewriter} for built-in operators.
 */
public class BuiltInOperatorRewriter implements OperatorRewriter {

    private static final OperatorRewriter DELEGATE = CompositeOperatorRewriter.builder()
            .add(new LoggingOperatorRemover())
            .add(new CheckpointOperatorRemover())
            .build();

    @Override
    public void perform(Context context, OperatorGraph graph) {
        DELEGATE.perform(context, graph);
    }
}
