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
package com.asakusafw.dag.compiler.flow;

import java.io.IOException;
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

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Generates direct launcher scripts.
 * @since 0.4.1
 */
public final class DirectLauncherScriptGenerator {

    static final Logger LOG = LoggerFactory.getLogger(DirectLauncherScriptGenerator.class);

    private static final String SCRIPT_TEMPLATE_FILE = "direct.sh.template";

    private static final String[] SCRIPT_TEMPLATE;
    static {
        try (Scanner scanner = new Scanner(
                DirectLauncherScriptGenerator.class.getResourceAsStream(SCRIPT_TEMPLATE_FILE),
                StandardCharsets.US_ASCII.name())) {
            List<String> lines = new ArrayList<>();
            while (scanner.hasNextLine()) {
                lines.add(scanner.nextLine());
            }
            SCRIPT_TEMPLATE = lines.toArray(new String[lines.size()]);
        }
    }

    private static final Pattern SCRIPT_PLACEHOLDER = Pattern.compile("\\{{3}(\\w+)\\}{3}"); //$NON-NLS-1$

    private DirectLauncherScriptGenerator() {
        return;
    }

    /**
     * Returns the generating script location.
     * @param flowId the flow ID
     * @return the script location
     */
    public static Location getScriptLocation(String flowId) {
        Arguments.requireNonNull(flowId);
        return Location.of(String.format("bin/%s.sh", flowId)); //$NON-NLS-1$

    }

    /**
     * Returns a stand-alone task in the given jobflow.
     * @param jobflow the target jobflow
     * @return the stand-alone task, or empty if it does not exist
     */
    public static Optional<CommandTaskReference> findTask(JobflowReference jobflow) {
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

    /**
     * Writes a launcher script into the given writer.
     * @param writer the target writer
     * @param script the script location (relative from the installation root)
     * @param batchId the batch ID
     * @param flowId the flow ID
     * @param application the application class
     * @throws IOException if I/O error was occurred
     */
    public static void write(
            Appendable writer,
            Location script,
            String batchId, String flowId,
            ClassDescription application) throws IOException {
        Map<String, String> variables = new LinkedHashMap<>();
        variables.put("SCRIPT", escape(script.toPath('/')));
        variables.put("BATCH_ID", escape(batchId));
        variables.put("FLOW_ID", escape(flowId));
        variables.put("APPLICATION", escape(application.getBinaryName()));
        write(writer, variables);
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
