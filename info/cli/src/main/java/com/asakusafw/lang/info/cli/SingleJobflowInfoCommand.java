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
import java.util.List;
import java.util.stream.Collectors;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;

/**
 * An abstract implementation of single jobflow processing commands.
 * @since 0.4.2
 */
public abstract class SingleJobflowInfoCommand extends JobflowInfoCommand {

    @Override
    protected final void process(
            PrintWriter writer,
            BatchInfo batch,
            List<JobflowInfo> jobflows) throws IOException {
        if (jobflows.size() != 1) {
            throw new IOException(MessageFormat.format(
                    "target jobflow is ambiguous, please specify \"--jobflow <flow-ID>\": '{'{0}'}'",
                    jobflows.stream()
                        .map(JobflowInfo::getId)
                        .collect(Collectors.joining(", "))));
        }
        process(writer, batch, jobflows.get(0));
    }

    /**
     * Processes the batch information.
     * @param writer the output printer
     * @param batch the batch information
     * @param jobflow the process target jobflow
     * @throws IOException if I/O error was occurred while processing the file
     */
    protected abstract void process(
            PrintWriter writer,
            BatchInfo batch,
            JobflowInfo jobflow) throws IOException;
}
