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

import static com.asakusafw.lang.info.cli.DrawUtil.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Optional;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.value.ClassInfo;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for generating DOT script about jobflow graphs.
 * @since 0.4.2
 */
@Command(
        name = "jobflow",
        description = "Generates jobflow graph as Graphviz DOT script",
        hidden = false
)
public class DrawJobflowCommand extends InfoCommand {

    @Option(
            name = { "--show-type", },
            title = "display jobflow type",
            description = "display jobflow type",
            arity = 0,
            required = false)
    boolean showJobflowType = false;

    @Option(
            name = { "--show-all", "-a", },
            title = "display all information",
            description = "display all information",
            arity = 0,
            required = false)
    boolean showAll = false;

    @Override
    protected void process(PrintWriter writer, BatchInfo info) throws IOException {
        writer.println("digraph {");
        writer.printf("label=%s;%n", literal(Optional.ofNullable(info.getDescriptionClass())
                .map(ClassInfo::of)
                .map(ClassInfo::getSimpleName)
                .orElse(info.getId())));
        info.getJobflows().forEach(it -> writer.printf(
                "%s [label=%s];%n",
                literal(it.getId()),
                literal(analyzeJobflow(it))));
        info.getJobflows().forEach(
                downstream -> downstream.getBlockerIds().forEach(
                        upstreamId -> writer.printf("%s -> %s",
                                literal(upstreamId),
                                literal(downstream.getId()))));
        writer.println("}");
    }

    private String analyzeJobflow(JobflowInfo it) {
        if (showAll || showJobflowType) {
            return String.join("\n",
                    it.getId(),
                    Optional.ofNullable(it.getDescriptionClass())
                        .map(ClassInfo::of)
                        .map(ClassInfo::getSimpleName)
                        .orElse("N/A"));
        } else {
            return it.getId();
        }
    }
}
