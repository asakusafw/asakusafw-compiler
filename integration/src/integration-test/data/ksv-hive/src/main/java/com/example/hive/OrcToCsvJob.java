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
package com.example.hive;

import static com.asakusafw.vocabulary.flow.util.CoreOperators.*;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.Source;
import com.example.hive.KsvHiveOperatorFactory;
import com.example.modelgen.dmdl.model.KsvHive;

/**
 * Partitioned sort using {@link KsvHive}.
 */
@JobFlow(name = "OrcToCsv")
public class OrcToCsvJob extends FlowDescription {

    final In<KsvHive> in;

    final Out<KsvHive> out;

    /**
     * Creates a new instance.
     * @param in the input
     * @param out the output
     */
    public OrcToCsvJob(
            @Import(name = "ksv", description = OrcInputDescription.class) In<KsvHive> in,
            @Export(name = "ksv", description = CsvOutputDescription.class) Out<KsvHive> out) {
        this.in = in;
        this.out = out;
    }

    @Override
    protected void describe() {
        out.add(in);
    }
}
