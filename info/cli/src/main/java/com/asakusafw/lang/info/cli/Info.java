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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;

/**
 * CLI entry for information models.
 * @since 0.4.2
 */
public final class Info {

    static final Logger LOG = LoggerFactory.getLogger(Info.class);

    private Info() {
        return;
    }

    /**
     * Program entry.
     * @param args command line tokens
     */
    public static void main(String... args) {
        int status = exec(args);
        if (status != 0) {
            System.exit(status);
        }
    }

    static int exec(String... args) {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("java -jar asakusa-info.jar")
                .withDefaultCommand(Help.class)
                .withCommand(Help.class);

        builder.withGroup("list")
            .withDescription("Displays information list")
            .withDefaultCommand(ListUsageCommand.class)
            .withCommand(ListBatchCommand.class)
            .withCommand(ListParameterCommand.class)
            .withCommand(ListJobflowCommand.class)
            .withCommand(ListOperatorCommand.class)
            .withCommand(ListPlanCommand.class)
            .withCommand(ListDirectFileInputCommand.class)
            .withCommand(ListDirectFileOutputCommand.class)
            .withCommand(ListWindGateInputCommand.class)
            .withCommand(ListWindGateOutputCommand.class);

        builder.withGroup("draw")
            .withDescription("Generates Graphviz DOT scripts")
            .withDefaultCommand(DrawUsageCommand.class)
            .withCommand(DrawJobflowCommand.class)
            .withCommand(DrawOperatorCommand.class)
            .withCommand(DrawPlanCommand.class);

        Cli<Runnable> cli = builder.build();
        Runnable command;
        try {
            command = cli.parse(args);
        } catch (ParseException e) {
            LOG.error("Cannot recognize command, please type \"help\" to show command information: {}",
                    Arrays.toString(args),
                    e);
            return 1;
        }
        command.run();
        if (command instanceof BaseCommand) {
            switch (((BaseCommand) command).getStatus()) {
            case PREPARE_ERROR:
                return 1;
            case PROCESS_ERROR:
                return 2;
            case SUCCESS:
                return 0;
            default:
                return -1;
            }
        } else {
            return 0;
        }
    }
}
