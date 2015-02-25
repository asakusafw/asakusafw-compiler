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
package com.asakusafw.lang.compiler.cli.mock;

import com.asakusafw.lang.compiler.analyzer.adapter.BatchAdapter;
import com.asakusafw.lang.compiler.analyzer.adapter.JobflowAdapter;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * Mock {@link ClassAnalyzer}.
 */
public class DummyClassAnalyzer implements ClassAnalyzer {

    @Override
    public Batch analyzeBatch(Context context, Class<?> batchClass) {
        BatchInfo info = BatchAdapter.analyzeInfo(batchClass);
        Batch result = new Batch(info);
        result.addElement(newJobflow(new JobflowInfo.Basic("testing", Descriptions.classOf(batchClass))));
        return result;
    }

    @Override
    public Jobflow analyzeJobflow(Context context, Class<?> jobflowClass) {
        JobflowInfo info = JobflowAdapter.analyzeInfo(jobflowClass);
        return newJobflow(info);
    }

    private Jobflow newJobflow(JobflowInfo info) {
        ExternalInput in = ExternalInput.newInstance("in", new ExternalInputInfo.Basic(
                Descriptions.classOf(DummyImporterDescription.class),
                "simple",
                Descriptions.classOf(String.class),
                ExternalInputInfo.DataSize.UNKNOWN));
        ExternalOutput out = ExternalOutput.newInstance("out", new ExternalOutputInfo.Basic(
                Descriptions.classOf(DummyImporterDescription.class),
                "simple",
                Descriptions.classOf(String.class)));
        in.getOperatorPort().connect(out.getOperatorPort());
        return new Jobflow(info, new OperatorGraph().add(in).add(out));
    }
}
