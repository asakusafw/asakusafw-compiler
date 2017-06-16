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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.operator.FlowOperatorSpec;
import com.asakusafw.lang.info.operator.OperatorGraphAttribute;
import com.asakusafw.lang.info.operator.UserOperatorSpec;
import com.asakusafw.lang.info.operator.view.OperatorGraphView;
import com.asakusafw.lang.info.operator.view.OperatorView;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for printing operators.
 * @since 0.4.2
 */
@Command(
        name = "operator",
        description = "Displays operator uses",
        hidden = false
)
public class ListOperatorCommand extends JobflowInfoCommand {

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
            .filter(it -> it instanceof OperatorGraphAttribute)
            .map(it -> new OperatorGraphView((OperatorGraphAttribute) it))
            .flatMap(this::extract)
            .filter(it -> it.getSpec() instanceof UserOperatorSpec)
            .map(it -> new UserOp(it, showVerbose))
            .distinct()
            .sorted(Comparator
                    .comparing((UserOp it) -> it.spec().getDeclaringClass().getName())
                    .thenComparing((UserOp it) -> it.spec().getMethodName()))
            .forEach(it -> {
                if (showVerbose) {
                    writer.printf("%s#%s(@%s){%s}%n",
                            it.spec().getDeclaringClass().getClassName(),
                            it.spec().getMethodName(),
                            it.spec().getAnnotation().getDeclaringClass().getSimpleName(),
                            it.entity.getParameters().stream()
                                .map(p -> String.format("%s:%s=%s",
                                        p.getName(),
                                        p.getType().getSimpleName(),
                                        p.getValue().getObject()))
                                .collect(Collectors.joining(", ")));
                } else {
                    writer.printf("%s#%s(@%s)%n",
                            it.spec().getDeclaringClass().getSimpleName(),
                            it.spec().getMethodName(),
                            it.spec().getAnnotation().getDeclaringClass().getSimpleName());
                }
            });
    }

    private Stream<OperatorView> extract(OperatorGraphView graph) {
        return graph.getOperators().stream()
                .flatMap(it -> {
                    if (it.getSpec() instanceof FlowOperatorSpec) {
                        return extract(it.getElementGraph());
                    } else {
                        return Stream.of(it);
                    }
                });
    }

    private static class UserOp {

        final OperatorView entity;

        final boolean verbose;

        UserOp(OperatorView entity, boolean verbose) {
            this.entity = entity;
            this.verbose = verbose;
        }

        UserOperatorSpec spec() {
            return (UserOperatorSpec) entity.getSpec();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(entity);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            UserOp other = (UserOp) obj;
            if (verbose) {
                return Objects.equals(entity.getSpec(), other.entity.getSpec())
                        && Objects.equals(entity.getParameters(), entity.getParameters());
            } else {
                return Objects.equals(entity.getSpec(), other.entity.getSpec());
            }
        }
    }
}
