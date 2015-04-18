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
package com.asakusafw.lang.compiler.analyzer.builtin;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.vocabulary.operator.Logging;

/**
 * Test for {@link LoggingOperatorRemover}.
 */
public class LoggingOperatorRemoverTest extends BuiltInOptimizerTestRoot {

    /**
     * test for default level.
     */
    @Test
    public void default_level() {
        OperatorRewriter optimizer = new LoggingOperatorRemover();
        OperatorGraph graph = graph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("debug")));
        assertThat(graph, hasOperator("info"));
        assertThat(graph, hasOperator("warn"));
        assertThat(graph, hasOperator("error"));
    }

    /**
     * test for default level.
     */
    @Test
    public void special_level() {
        OperatorRewriter optimizer = new LoggingOperatorRemover();
        OperatorGraph graph = graph();
        apply(context(LoggingOperatorRemover.KEY_LOG_LEVEL, "warn"), optimizer, graph);
        assertThat(graph, not(hasOperator("debug")));
        assertThat(graph, not(hasOperator("info")));
        assertThat(graph, hasOperator("warn"));
        assertThat(graph, hasOperator("error"));
    }

    private OperatorGraph graph() {
        ReifiableTypeDescription type = typeOf(String.class);
        return connect(new Operator[] {
                ExternalInput.newInstance("in", type),
                extract("debug", type),
                extract("info", type),
                extract("warn", type),
                extract("error", type),
                ExternalOutput.newInstance("out", type),
        });
    }

    private Operator extract(String name, TypeDescription type) {
        return OperatorExtractor.extract(Logging.class, Ops.class, name)
                .input("p", type)
                .output("p", type)
                .build();
    }

    private OperatorGraph connect(Operator... line) {
        Operator last = line[0];
        for (int i = 1; i < line.length; i++) {
            Operator current = line[i];
            assert last.getOutputs().size() == 1;
            assert current.getInputs().size() == 1;
            last.getOutputs().get(0).connect(current.getInputs().get(0));
            last = current;
        }
        return new OperatorGraph(Arrays.asList(line));
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @Logging(Logging.Level.DEBUG)
        public abstract void debug(String value);

        @Logging(Logging.Level.INFO)
        public abstract void info(String value);

        @Logging(Logging.Level.WARN)
        public abstract void warn(String value);

        @Logging(Logging.Level.ERROR)
        public abstract void error(String value);
    }
}
