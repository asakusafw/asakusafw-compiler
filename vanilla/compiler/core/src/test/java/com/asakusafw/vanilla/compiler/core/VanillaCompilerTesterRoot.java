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
package com.asakusafw.vanilla.compiler.core;

import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.lang.compiler.tester.CompilerTester;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.lang.compiler.tester.executor.JobflowExecutor;
import com.asakusafw.lang.compiler.tester.util.OperatorGraphBuilder;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.vocabulary.flow.FlowDescription;

/**
 * Testing utilities about Asakusa Vanilla compiler.
 */
public class VanillaCompilerTesterRoot {

    /**
     * Compiles the target operator graph and runs it.
     * @param profile the compiler profile
     * @param executor the executor
     * @param action the source provider
     */
    public void run(
            CompilerProfile profile,
            JobflowExecutor executor,
            Action<OperatorGraphBuilder, Exception> action) {
        try (CompilerTester tester = profile.build()) {
            OperatorGraphBuilder graph = new OperatorGraphBuilder(tester.getCompilerContext());
            action.perform(graph);
            Jobflow jobflow = new Jobflow(
                    new JobflowInfo.Basic("testing", Descriptions.classOf(FlowDescription.class)),
                    graph.build());
            JobflowArtifact artifact = tester.compile(jobflow);
            executor.execute(tester.getTesterContext(), artifact);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
