/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import com.asakusafw.info.hive.HiveIoAttribute;
import com.asakusafw.info.hive.HiveInputInfo;
import com.asakusafw.info.hive.HiveOutputInfo;
import com.asakusafw.info.hive.TableInfo;
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
        if (Util.isAvailable(context.getClassLoader()) == false) {
            return;
        }
        LOG.debug("aggregating hive schema information: {}", source.getBatchId());
        List<HiveInputInfo> inputs = new ArrayList<>();
        List<HiveOutputInfo> outputs = new ArrayList<>();
        for (JobflowReference jobflow : source.getJobflows()) {
            HiveIoAttribute attr = load(location -> context.findResourceFile(jobflow, location));
            inputs.addAll(attr.getInputs());
            outputs.addAll(attr.getOutputs());
        }
        inputs = Util.normalize(inputs);
        outputs = Util.normalize(outputs);
        LOG.debug("generating Hive input table schema: {} entries", inputs.size());
        try (OutputStream stream = context.addResourceFile(PATH_INPUT)) {
            Persistent.write(HiveInputInfo.class, inputs, stream);
        }
        LOG.debug("generating Hive output table schema: {} entries", outputs.size());
        try (OutputStream stream = context.addResourceFile(PATH_OUTPUT)) {
            Persistent.write(HiveOutputInfo.class, outputs, stream);
        }
    }

    /**
     * Collects the saved Hive I/O information.
     * @param provider the resource provider
     * @return Hive I/O information
     * @throws IOException if I/O error was occurred while loading information
     */
    public static HiveIoAttribute load(ResourceProvider provider) throws IOException {
        List<HiveInputInfo> inputs = collect(provider,
                HiveInputInfo.class, HiveSchemaCollectorProcessor.PATH_INPUT);
        List<HiveOutputInfo> outputs = collect(provider,
                HiveOutputInfo.class, HiveSchemaCollectorProcessor.PATH_OUTPUT);
        return new HiveIoAttribute(inputs, outputs);
    }

    private static <T extends TableInfo.Provider> List<T> collect(
            ResourceProvider provider,
            Class<T> type, Location location) throws IOException {
        try (InputStream input = provider.find(location)) {
            if (input == null) {
                return null;
            }
            return Persistent.read(type, input);
        }
    }

    /**
     * Provides resources.
     * @since 0.5.0
     */
    @FunctionalInterface
    public interface ResourceProvider {

        /**
         * Returns a resource on the given location.
         * @param location the target location
         * @return the resource contents, or {@code null} if it is not found
         * @throws IOException if failed to open the resource
         */
        InputStream find(Location location) throws IOException;
    }
}
