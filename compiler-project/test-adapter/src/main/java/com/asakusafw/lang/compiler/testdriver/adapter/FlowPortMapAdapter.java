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
package com.asakusafw.lang.compiler.testdriver.adapter;

import com.asakusafw.lang.compiler.analyzer.FlowDescriptionAnalyzer;
import com.asakusafw.lang.compiler.internalio.InternalExporterDescription;
import com.asakusafw.lang.compiler.internalio.InternalImporterDescription;
import com.asakusafw.testdriver.compiler.FlowPortMap;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;

class FlowPortMapAdapter implements FlowPortMap {

    private final FlowDescriptionAnalyzer analyzer = new FlowDescriptionAnalyzer();

    public FlowPortMapAdapter() {
        return;
    }

    @Override
    public <T> In<T> addInput(String name, Class<T> dataType) {
        String path = Util.createInputPath(name);
        return analyzer.addInput(name, new InternalImporterDescription.Basic(dataType, path));
    }

    @Override
    public <T> Out<T> addOutput(String name, Class<T> dataType) {
        String path = Util.createOutputPath(name);
        return analyzer.addOutput(name, new InternalExporterDescription.Basic(dataType, path));
    }

    public FlowGraph resolve(FlowDescription flow) {
        return analyzer.analyze(flow);
    }
}