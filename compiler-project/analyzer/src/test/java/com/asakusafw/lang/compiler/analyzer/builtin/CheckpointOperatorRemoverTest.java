/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;

/**
 * Test for {@link CheckpointOperatorRemover}.
 */
public class CheckpointOperatorRemoverTest extends BuiltInOptimizerTestRoot {

    private final OperatorRewriter optimizer = new CheckpointOperatorRemover();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        OperatorGraph graph = graph();
        apply(context(), optimizer, graph);
        assertThat(graph, hasOperator(CoreOperatorKind.CHECKPOINT));
    }

    /**
     * enabled.
     */
    @Test
    public void enable() {
        OperatorGraph graph = graph();
        OptimizerContext context = context(CheckpointOperatorRemover.KEY_ENABLE, "true");
        apply(context, optimizer, graph);
        assertThat(graph, not(hasOperator(CoreOperatorKind.CHECKPOINT)));
    }

    private OperatorGraph graph() {
        ReifiableTypeDescription type = typeOf(String.class);
        return connect(new Operator[] {
                ExternalInput.newInstance("in", type),
                CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                    .input("p", type)
                    .output("p", type)
                    .build(),
                ExternalOutput.newInstance("out", type),
        });
    }
}
