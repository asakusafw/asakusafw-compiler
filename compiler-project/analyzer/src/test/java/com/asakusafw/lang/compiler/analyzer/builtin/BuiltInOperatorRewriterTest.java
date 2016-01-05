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
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.vocabulary.operator.Logging;

/**
 * Test for {@link BuiltInOperatorRewriter}.
 */
public class BuiltInOperatorRewriterTest extends BuiltInOptimizerTestRoot {

    private final OperatorRewriter optimizer = new BuiltInOperatorRewriter();

    /**
     * {@link CheckpointOperatorRemover} is enabled.
     */
    @Test
    public void checkpoint_operator_remover() {
        ReifiableTypeDescription type = typeOf(String.class);
        OperatorGraph graph = connect(new Operator[] {
                ExternalInput.newInstance("in", type),
                CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                    .input("p", type)
                    .output("p", type)
                    .build(),
                ExternalOutput.newInstance("out", type),
        });
        OptimizerContext context = context(CheckpointOperatorRemover.KEY_ENABLE, "true");
        apply(context, optimizer, graph);
        assertThat(graph, not(hasOperator(CoreOperatorKind.CHECKPOINT)));
    }

    /**
     * {@link LoggingOperatorRemover} is enabled.
     */
    @Test
    public void logging_operator_remover() {
        ReifiableTypeDescription type = typeOf(String.class);
        OperatorGraph graph = connect(new Operator[] {
                ExternalInput.newInstance("in", type),
                OperatorExtractor.extract(Logging.class, Ops.class, "logging")
                    .input("p", type)
                    .output("p", type)
                    .build(),
                ExternalOutput.newInstance("out", type),
        });
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("debug")));
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @Logging(Logging.Level.DEBUG)
        public abstract void logging(String value);
    }
}
