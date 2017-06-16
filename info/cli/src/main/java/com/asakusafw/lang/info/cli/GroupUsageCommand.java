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

import java.io.PrintWriter;
import java.util.Collections;

import javax.inject.Inject;

import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.model.GlobalMetadata;

/**
 * A command for printing help of group.
 * @since 0.4.2
 */
public class GroupUsageCommand extends BaseCommand {

    private final String groupName;

    @Inject
    GlobalMetadata metadata;

    @Option(
            name = { "--verbose", "-v", },
            title = "verbose mode",
            description = "verbose mode",
            arity = 0,
            required = false)
    boolean showVerbose = false;

    /**
     * Creates a new instance.
     * @param groupName the group name
     */
    protected GroupUsageCommand(String groupName) {
        this.groupName = groupName;
    }

    @Override
    protected Status process(PrintWriter writer) {
        if (showVerbose) {
            StringBuilder buf = new StringBuilder();
            Help.help(metadata, Collections.singletonList(groupName), buf);
            writer.print(buf);
        } else {
            writer.printf("usage: %s %s <sub-command> [<args>]%n", metadata.getName(), groupName);
            writer.println();
            writer.println("The available sub-commands are:");
            metadata.getCommandGroups().stream()
                .filter(it -> it.getName().equals(groupName))
                .findAny()
                .get()
                .getCommands()
                .forEach(it -> writer.printf("    %s - %s%n", it.getName(), it.getDescription()));
            writer.println();
            writer.printf("See '%s help %s <sub-command>' for more information on a specific sub-command.%n",
                    metadata.getName(),
                    groupName);
        }
        return Status.SUCCESS;
    }
}
