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
package com.asakusafw.lang.compiler.extension.info;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.BatchCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.api.AttributeCollector;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Collects batch information and generating scripts from it.
 * @since 0.4.1
 */
public class BatchInfoParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(BatchInfoParticipant.class);

    /**
     * The script output path.
     */
    public static final String PATH = "etc/batch-info.json"; //$NON-NLS-1$

    /**
     * Computes and returns the path to the batch information script output.
     * @param outputDir compilation output path
     * @return the script output path
     */
    public static File getScriptOutput(File outputDir) {
        return new File(outputDir, PATH);
    }

    @Override
    public void afterBatch(Context context, Batch batch, BatchReference reference) {
        BatchInfo info = analyzeBatch(context, batch, reference);
        write(context, info, Location.of(PATH));
    }

    static BatchInfo analyzeBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
        AttributeCollectorContextAdapter adapter = new AttributeCollectorContextAdapter(context);
        List<AttributeCollector> collectors = collectors(context.getProject().getClassLoader());
        List<JobflowInfo> jobflows = new ArrayList<>();
        for (BatchElement element : batch.getElements()) {
            adapter.reset(element.getJobflow());
            collectors.forEach(c -> c.process(adapter, element.getJobflow()));
            Optional.ofNullable(reference.find(element.getJobflow().getFlowId()))
                .ifPresent(it -> collectors.forEach(c -> c.process(adapter, it)));

            jobflows.add(new JobflowInfo(
                    element.getJobflow().getFlowId(),
                    element.getJobflow().getDescriptionClass().getBinaryName(),
                    element.getBlockerElements().stream()
                        .map(e -> e.getJobflow().getFlowId())
                        .collect(Collectors.toList()),
                    adapter.getAttributes()));
        }
        adapter.reset(null);
        collectors.forEach(c -> c.process(adapter, batch));
        collectors.forEach(c -> c.process(adapter, reference));
        return new BatchInfo(
                batch.getBatchId(),
                batch.getDescriptionClass().getBinaryName(),
                batch.getComment(),
                jobflows,
                adapter.getAttributes());
    }

    private static List<AttributeCollector> collectors(ClassLoader classLoader) {
        List<AttributeCollector> results = new ArrayList<>();
        try {
            for (AttributeCollector collector : ServiceLoader.load(AttributeCollector.class, classLoader)) {
                results.add(collector);
            }
        } catch (ServiceConfigurationError e) {
            LOG.warn("error occurred while initializing AttributeCollector plug-ins", e);
        }
        return results;
    }

    static void write(Context context, BatchInfo info, Location location) {
        ObjectMapper mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        try (OutputStream output = context.getOutput().addResource(location)) {
            mapper.writerFor(BatchInfo.class).writeValue(output, info);
        } catch (IOException | RuntimeException e) {
            LOG.warn(MessageFormat.format(
                            "error occurred while writing batch info: {0}",
                            location), e);
        }
    }
}
