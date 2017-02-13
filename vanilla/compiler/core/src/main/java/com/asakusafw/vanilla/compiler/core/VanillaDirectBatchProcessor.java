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
package com.asakusafw.vanilla.compiler.core;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.vanilla.compiler.common.VanillaTask;

/**
 * Generates launch scripts of direct Vanilla jobflows.
 * @since 0.4.1
 */
public class VanillaDirectBatchProcessor implements BatchProcessor {

    static final Logger LOG = LoggerFactory.getLogger(VanillaDirectBatchProcessor.class);

    private static final int ARG_APPLICATION_INDEX = 4;

    private static final String SCRIPT_TEMPLATE_FILE = "direct.sh.template";

    private static final String[] SCRIPT_TEMPLATE;
    static {
        try (Scanner scanner = new Scanner(
                VanillaDirectBatchProcessor.class.getResourceAsStream(SCRIPT_TEMPLATE_FILE),
                StandardCharsets.US_ASCII.name())) {
            List<String> lines = new ArrayList<>();
            while (scanner.hasNextLine()) {
                lines.add(scanner.nextLine());
            }
            SCRIPT_TEMPLATE = lines.toArray(new String[lines.size()]);
        }
    }

    private static final Pattern SCRIPT_PLACEHOLDER = Pattern.compile("\\{{3}(\\w+)\\}{3}"); //$NON-NLS-1$

    /**
     * Returns the generating script location.
     * @param flowId the flow ID
     * @return the script location
     */
    public static Location getScriptLocation(String flowId) {
        Arguments.requireNonNull(flowId);
        return Location.of(String.format("bin/%s.sh", flowId)); //$NON-NLS-1$

    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        for (JobflowReference jobflow : source.getJobflows()) {
            CommandTaskReference command = find(jobflow)
                    .filter(task -> task.getCommand().equals(VanillaTask.PATH_COMMAND))
                    .filter(task -> task.getArguments().size() > ARG_APPLICATION_INDEX)
                    .orElse(null);
            if (command == null) {
                LOG.debug("{}:{} does not support Vanilla direct launching", source.getBatchId(), jobflow.getFlowId()); //$NON-NLS-1$
                continue;
            }
            Map<String, String> variables = new LinkedHashMap<>();
            variables.put("BATCH_ID", escape(source.getBatchId()));
            variables.put("FLOW_ID", escape(jobflow.getFlowId()));
            variables.put("APPLICATION", escape(command.getArguments().get(ARG_APPLICATION_INDEX).getImage()));
            Location location = getScriptLocation(jobflow.getFlowId());
            LOG.debug("generating direct vanilla script: {} ({})", location, variables);
            try (Writer writer = new PrintWriter(
                    new OutputStreamWriter(context.addResourceFile(location), StandardCharsets.UTF_8))) {
                write(writer, variables);
            }
        }
    }

    private static Optional<CommandTaskReference> find(JobflowReference jobflow) {
        List<TaskReference> tasks = Arrays.stream(TaskReference.Phase.values())
                .flatMap(phase -> jobflow.getTasks(phase).stream())
                .collect(Collectors.toList());
        if (tasks.size() != 1) {
            return Optional.empty();
        }
        return Optionals.of(tasks.get(0))
                .filter(task -> task instanceof CommandTaskReference)
                .map(CommandTaskReference.class::cast);
    }

    private static void write(Appendable writer, Map<String, String> variables) throws IOException {
        for (String line : SCRIPT_TEMPLATE) {
            Matcher matcher = SCRIPT_PLACEHOLDER.matcher(line);
            int start = 0;
            while (matcher.find(start)) {
                writer.append(line, start, matcher.start());
                String name = matcher.group(1);
                String value = variables.get(name);
                if (value == null) {
                    throw new IllegalStateException(MessageFormat.format(
                            "[Internal Error] unknown placeholder: {0}",
                            name));
                }
                writer.append(value);
                start = matcher.end();
            }
            writer.append(line, start, line.length());
            writer.append('\n'); // LF-only
        }
    }

    private static String escape(String value) {
        if (value.indexOf('\'') >= 0) {
            throw new IllegalStateException(value);
        }
        return String.format("'%s'", value); //$NON-NLS-1$
    }
}
