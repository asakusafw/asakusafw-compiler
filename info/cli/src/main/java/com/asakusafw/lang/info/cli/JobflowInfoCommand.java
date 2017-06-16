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
package com.asakusafw.lang.info.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;

import io.airlift.airline.Option;

/**
 * An abstract implementation of jobflow processing commands.
 * @since 0.4.2
 */
public abstract class JobflowInfoCommand extends InfoCommand {

    @Option(
            name = { "--jobflow", "-j", },
            title = "flow-id",
            description = "target flow ID",
            arity = 1,
            required = false)
    String flowId;

    @Override
    protected final void process(PrintWriter writer, BatchInfo info) throws IOException {
        List<JobflowInfo> candidates = new ArrayList<>(info.getJobflows());
        if (candidates.isEmpty()) {
            throw new IOException(MessageFormat.format(
                    "there are no available jobflows in batch: {0}",
                    info.getId()));
        }
        if (flowId != null) {
            candidates = candidates.stream()
                    .filter(it -> Objects.equals(it.getId(), flowId))
                    .collect(Collectors.toList());
            if (candidates.isEmpty()) {
                throw new IOException(MessageFormat.format(
                        "there is no jobflow with flow ID \"{0}\", must be one of: '{'{1}'}'",
                        info.getId(),
                        info.getJobflows().stream()
                            .map(JobflowInfo::getId)
                            .sorted()
                            .collect(Collectors.joining(", "))));
            }
        }
        process(writer, info, candidates);
    }

    /**
     * Processes the batch information.
     * @param writer the output printer
     * @param batch the batch information
     * @param jobflows the process targets, never empty
     * @throws IOException if I/O error was occurred while processing the file
     */
    protected abstract void process(
            PrintWriter writer,
            BatchInfo batch,
            List<JobflowInfo> jobflows) throws IOException;
}
