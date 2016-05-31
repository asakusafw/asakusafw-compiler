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
package com.asakusafw.lang.compiler.extension.hive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.directio.hive.info.InputInfo;
import com.asakusafw.directio.hive.info.OutputInfo;
import com.asakusafw.directio.hive.info.TableInfo;
import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.Location;

/**
 * Aggregates results of {@link HiveSchemaCollectorProcessor}.
 * @since 0.3.1
 */
public class HiveSchemaAggregatorProcessor implements BatchProcessor {

    static final Logger LOG = LoggerFactory.getLogger(HiveSchemaAggregatorProcessor.class);

    private static final Location PATH_BASE = Location.of("etc/hive-schema"); //$NON-NLS-1$

    /**
     * The output path of input schema information.
     */
    public static final Location PATH_INPUT = PATH_BASE.append("input.json"); //$NON-NLS-1$

    /**
     * The output path of output schema information.
     */
    public static final Location PATH_OUTPUT = PATH_BASE.append("output.json"); //$NON-NLS-1$

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        Util.checkDependencies(context.getClassLoader());
        LOG.debug("aggregating hive schema information: {}", source.getBatchId());
        List<InputInfo> inputs = new ArrayList<>();
        List<OutputInfo> outputs = new ArrayList<>();
        for (JobflowReference jobflow : source.getJobflows()) {
            inputs.addAll(collect(context, jobflow, InputInfo.class, HiveSchemaCollectorProcessor.PATH_INPUT));
            outputs.addAll(collect(context, jobflow, OutputInfo.class, HiveSchemaCollectorProcessor.PATH_OUTPUT));
        }
        inputs = Util.normalize(inputs);
        outputs = Util.normalize(outputs);
        LOG.debug("generating Hive input table schema: {} entries", inputs.size());
        try (OutputStream stream = context.addResourceFile(PATH_INPUT)) {
            Persistent.write(InputInfo.class, inputs, stream);
        }
        LOG.debug("generating Hive output table schema: {} entries", outputs.size());
        try (OutputStream stream = context.addResourceFile(PATH_OUTPUT)) {
            Persistent.write(OutputInfo.class, outputs, stream);
        }
    }

    private <T extends TableInfo.Provider> List<T> collect(
            Context context, JobflowReference jobflow,
            Class<T> type, Location location) throws IOException {
        LOG.debug("collecting hive schema information: {}!{}", jobflow.getFlowId(), location);
        try (InputStream input = context.findResourceFile(jobflow, location)) {
            if (input == null) {
                LOG.debug("missing hive schema information: {} ({})", jobflow.getFlowId(), location);
                return null;
            }
            return Persistent.read(type, input);
        }
    }
}
