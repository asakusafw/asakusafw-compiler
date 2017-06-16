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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.windgate.WindGateInputInfo;
import com.asakusafw.lang.info.windgate.WindGateIoAttribute;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for printing list of WindGate inputs.
 * @since 0.4.2
 */
@Command(
        name = "windgate-input",
        description = "Displays WindGate input uses",
        hidden = false
)
public class ListWindGateInputCommand extends JobflowInfoCommand {

    @Option(
            name = { "--verbose", "-v", },
            title = "verbose mode",
            description = "verbose mode",
            arity = 0,
            required = false)
    boolean showVerbose = false;

    @Override
    protected void process(PrintWriter writer, BatchInfo batch, List<JobflowInfo> jobflows) throws IOException {
        jobflows.stream()
            .flatMap(jobflow -> jobflow.getAttributes().stream())
            .filter(it -> it instanceof WindGateIoAttribute)
            .map(it -> (WindGateIoAttribute) it)
            .flatMap(it -> it.getInputs().stream())
            .sorted(Comparator.comparing(WindGateInputInfo::getName))
            .forEachOrdered(info -> {
                Map<String, Object> members = new LinkedHashMap<>();
                members.put("profile-name", info.getProfileName());
                members.put("resource-name", info.getResourceName());
                members.putAll(info.getConfiguration());
                writer.printf("%s:%n", info.getDescriptionClass());
                ListUtil.printBlock(writer, 4, members);
            });
    }
}
