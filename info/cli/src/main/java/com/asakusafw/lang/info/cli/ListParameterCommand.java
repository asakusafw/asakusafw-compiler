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
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.ParameterListAttribute;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for printing list of batch parameters.
 * @since 0.4.2
 */
@Command(
        name = "parameter",
        description = "Displays list of batch parameters",
        hidden = false
)
public class ListParameterCommand extends InfoCommand {

    @Option(
            name = { "--verbose", "-v", },
            title = "verbose mode",
            description = "verbose mode",
            arity = 0,
            required = false)
    boolean showVerbose = false;

    @Override
    protected void process(PrintWriter writer, BatchInfo info) throws IOException {
        ParameterListAttribute attr = info.findAttribute(ParameterListAttribute.class)
                .orElseThrow(() -> new IOException(MessageFormat.format(
                        "there are no batch parameter information in {0} ({1})",
                        info.getId(),
                        ListUtil.normalize(info.getDescriptionClass()))));
        if (showVerbose) {
            attr.getElements().forEach(it -> {
                Map<String, Object> members = new LinkedHashMap<>();
                members.put("comment", it.getComment());
                members.put("pattern", it.getPattern());
                members.put("mandatory", it.isMandatory());
                writer.printf("%s:%n", it.getName());
                ListUtil.printBlock(writer, 4, members);
            });
        } else {
            attr.getElements().forEach(writer::println);
        }
    }
}