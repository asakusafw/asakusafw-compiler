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
package com.asakusafw.lang.compiler.analyzer.builtin;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;
import com.asakusafw.vocabulary.operator.MasterSelection;

/**
 * Test for {@link EmptyMasterJoinRemover}.
 */
public class EmptyMasterJoinRemoverTest extends BuiltInOptimizerTestRoot {

    private final OperatorRewriter optimizer = new EmptyMasterJoinRemover();

    /**
     * connected.
     */
    @Test
    public void connected() {
        OperatorGraph graph = join().toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, hasOperator("join"));
    }

    /**
     * w/ empty master.
     */
    @Test
    public void empty_master() {
        MockOperators mock = join();
        mock.getInput("join.master").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("join")));

        mock.assertConnected("transaction", "missed");
        assertThat(mock.getInput("joined").hasOpposites(), is(false));
    }

    /**
     * w/ empty tx.
     */
    @Test
    public void empty_tx() {
        MockOperators mock = join();
        mock.getInput("join.transaction").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("join")));

        assertThat(mock.getInput("joined").hasOpposites(), is(false));
        assertThat(mock.getInput("missed").hasOpposites(), is(false));
    }

    /**
     * w/ empty both.
     */
    @Test
    public void empty_both() {
        MockOperators mock = join();
        mock.getInput("join.master").disconnectAll();
        mock.getInput("join.transaction").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("join")));

        assertThat(mock.getInput("joined").hasOpposites(), is(false));
        assertThat(mock.getInput("missed").hasOpposites(), is(false));
    }

    /**
     * w/ empty master for branch.
     */
    @Test
    public void branch_empty_master() {
        MockOperators mock = branch();
        mock.getInput("join.master").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, hasOperator("branch"));
    }

    /**
     * w/ empty tx for branch.
     */
    @Test
    public void branch_empty_tx() {
        MockOperators mock = branch();
        mock.getInput("join.transaction").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("branch")));

        assertThat(mock.getInput("joined").hasOpposites(), is(false));
        assertThat(mock.getInput("missed").hasOpposites(), is(false));
    }

    /**
     * w/ empty both for branch.
     */
    @Test
    public void branch_empty_both() {
        MockOperators mock = branch();
        mock.getInput("join.master").disconnectAll();
        mock.getInput("join.transaction").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("branch")));

        assertThat(mock.getInput("joined").hasOpposites(), is(false));
        assertThat(mock.getInput("missed").hasOpposites(), is(false));
    }

    /**
     * w/ empty master for selection.
     */
    @Test
    public void selection_empty_master() {
        MockOperators mock = selection();
        mock.getInput("join.master").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, hasOperator("selection"));
    }

    /**
     * w/ empty tx for selection.
     */
    @Test
    public void selection_empty_tx() {
        MockOperators mock = selection();
        mock.getInput("join.transaction").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("selection")));

        assertThat(mock.getInput("joined").hasOpposites(), is(false));
        assertThat(mock.getInput("missed").hasOpposites(), is(false));
    }

    /**
     * w/ empty both for selection.
     */
    @Test
    public void selection_empty_both() {
        MockOperators mock = selection();
        mock.getInput("join.master").disconnectAll();
        mock.getInput("join.transaction").disconnectAll();
        OperatorGraph graph = mock.toGraph();
        apply(context(), optimizer, graph);
        assertThat(graph, not(hasOperator("selection")));

        assertThat(mock.getInput("joined").hasOpposites(), is(false));
        assertThat(mock.getInput("missed").hasOpposites(), is(false));
    }

    private OptimizerContext context() {
        return context(new String[] {
                EmptyMasterJoinRemover.KEY_REMOVE_EMPTY_MASTER, "true",
                EmptyMasterJoinRemover.KEY_REMOVE_EMPTY_TRANACTION, "true",
        });
    }

    private MockOperators join() {
        return new MockOperators()
                .marker("master")
                .marker("transaction")
                .bless("join", OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "join")
                        .input("master", typeOf(String.class))
                        .input("transaction", typeOf(String.class))
                        .output("updated", typeOf(String.class))
                        .output("missed", typeOf(String.class)))
                .marker("joined")
                .marker("missed")
                .connect("master", "join.master")
                .connect("transaction", "join.transaction")
                .connect("join.updated", "joined")
                .connect("join.missed", "missed");
    }

    private MockOperators branch() {
        return new MockOperators()
                .marker("master")
                .marker("transaction")
                .bless("join", OperatorExtractor.extract(MasterBranch.class, Ops.class, "branch")
                        .input("master", typeOf(String.class))
                        .input("transaction", typeOf(String.class))
                        .output("updated", typeOf(String.class))
                        .output("missed", typeOf(String.class)))
                .marker("joined")
                .marker("missed")
                .connect("master", "join.master")
                .connect("transaction", "join.transaction")
                .connect("join.updated", "joined")
                .connect("join.missed", "missed");
    }

    private MockOperators selection() {
        return new MockOperators()
                .marker("master")
                .marker("transaction")
                .bless("join", OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "selection")
                        .input("master", typeOf(String.class))
                        .input("transaction", typeOf(String.class))
                        .output("updated", typeOf(String.class))
                        .output("missed", typeOf(String.class)))
                .marker("joined")
                .marker("missed")
                .connect("master", "join.master")
                .connect("transaction", "join.transaction")
                .connect("join.updated", "joined")
                .connect("join.missed", "missed");
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @MasterJoinUpdate
        public abstract void join();

        @MasterBranch
        public abstract void branch();

        @MasterJoinUpdate(selection = "selector")
        public abstract void selection();

        @MasterSelection
        public abstract void selector();
    }
}
