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
package com.asakusafw.lang.compiler.extension.workflow;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.BlockingReference;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.hadoop.HadoopCommandRequired;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.workflow.model.BatchInfo;
import com.asakusafw.workflow.model.CommandToken;
import com.asakusafw.workflow.model.DeleteTaskInfo;
import com.asakusafw.workflow.model.TaskInfo;
import com.asakusafw.workflow.model.basic.AbstractGraphElement;
import com.asakusafw.workflow.model.basic.BasicBatchInfo;
import com.asakusafw.workflow.model.basic.BasicCommandTaskInfo;
import com.asakusafw.workflow.model.basic.BasicDeleteTaskInfo;
import com.asakusafw.workflow.model.basic.BasicHadoopTaskInfo;
import com.asakusafw.workflow.model.basic.BasicJobflowInfo;
import com.asakusafw.workflow.model.basic.BasicTaskInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An implementation of {@link BatchProcessor} for generating workflow information scripts.
 * @since 0.10.0
 */
public class WorkflowBatchProcessor implements BatchProcessor {

    static final Logger LOG = LoggerFactory.getLogger(WorkflowBatchProcessor.class);

    /**
     * The script output path.
     */
    public static final String PATH = "etc/workflow.json"; //$NON-NLS-1$

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        LOG.debug("analyzing batch structure for workflow information");
        BatchInfo workflow = new ReferenceResolver(context.getOptions()).resolve(source);
        write(context, workflow, Location.of(PATH));
    }

    static void write(Context context, BatchInfo info, Location location) {
        ObjectMapper mapper = new ObjectMapper();
        try (OutputStream output = context.addResourceFile(location)) {
            mapper.writerFor(BatchInfo.class).writeValue(output, info);
        } catch (IOException | RuntimeException e) {
            LOG.warn(MessageFormat.format(
                            "error occurred while writing workflow information: {0}",
                            location), e);
        }
    }

    private static class ReferenceResolver {

        private static final Map<TaskReference.Phase, TaskInfo.Phase> PHASE_MAPPING;
        static {
            Map<TaskReference.Phase, TaskInfo.Phase> map = new EnumMap<>(TaskReference.Phase.class);
            for (TaskReference.Phase value : TaskReference.Phase.values()) {
                map.put(value, TaskInfo.Phase.valueOf(value.name()));
            }
            PHASE_MAPPING = map;
        }

        private final CompilerOptions options;

        ReferenceResolver(CompilerOptions options) {
            this.options = options;
        }

        BatchInfo resolve(BatchReference source) {
            BasicBatchInfo result = new BasicBatchInfo(source.getBatchId());
            Map<JobflowReference, BasicJobflowInfo> mapping = new LinkedHashMap<>();
            for (JobflowReference jobflow : source.getJobflows()) {
                BasicJobflowInfo resolved = resolve(jobflow);
                result.addElement(resolved);
                mapping.put(jobflow, resolved);
            }
            resolveDependencies(mapping);
            return result;
        }

        private BasicJobflowInfo resolve(JobflowReference reference) {
            BasicJobflowInfo result = new BasicJobflowInfo(reference.getFlowId());
            for (Map.Entry<TaskReference.Phase, TaskInfo.Phase> phaseMapping : PHASE_MAPPING.entrySet()) {
                Map<TaskReference, BasicTaskInfo> mapping = new LinkedHashMap<>();
                for (TaskReference task : reference.getTasks(phaseMapping.getKey())) {
                    BasicTaskInfo mirror = resolve(task);
                    result.addTask(phaseMapping.getValue(), mirror);
                    mapping.put(task, mirror);
                }
                resolveDependencies(mapping);
            }
            if (isHadoopCommandRequired(reference)) {
                result.addTask(TaskInfo.Phase.CLEANUP, new BasicDeleteTaskInfo(
                        DeleteTaskInfo.PathKind.HADOOP_FILE_SYSTEM,
                        options.getRuntimeWorkingDirectory()));
            }
            return result;
        }

        private static BasicTaskInfo resolve(TaskReference reference) {
            if (reference instanceof CommandTaskReference) {
                CommandTaskReference t = (CommandTaskReference) reference;
                Location cmd = t.getCommand();
                List<CommandToken> args = resolveArguments(t.getArguments());
                return new BasicCommandTaskInfo(t.getModuleName(), t.getProfileName(), cmd.toPath(), args);
            } else if (reference instanceof HadoopTaskReference) {
                HadoopTaskReference t = (HadoopTaskReference) reference;
                return new BasicHadoopTaskInfo(t.getModuleName(), t.getMainClass().getBinaryName());
            } else {
                throw new UnsupportedOperationException(MessageFormat.format(
                        "unsupported task type: {0}",
                        reference));
            }
        }

        private static <R extends BlockingReference<? extends R>, E extends AbstractGraphElement<? super E>>
        void resolveDependencies(Map<R, E> mapping) {
            mapping.forEach((reference, resolved) -> reference.getBlockers().stream()
                    .map(mapping::get)
                    .forEach(resolved::addBlocker));
        }

        private static boolean isHadoopCommandRequired(JobflowReference jobflow) {
            for (TaskReference.Phase phase : TaskReference.Phase.values()) {
                for (TaskReference task : jobflow.getTasks(phase)) {
                    if (task instanceof HadoopTaskReference) {
                        return true;
                    }
                    if (HadoopCommandRequired.get(task)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static List<CommandToken> resolveArguments(
                List<com.asakusafw.lang.compiler.api.reference.CommandToken> arguments) {
            List<CommandToken> results = new ArrayList<>();
            for (com.asakusafw.lang.compiler.api.reference.CommandToken token : arguments) {
                results.add(resolveCommandToken(token));
            }
            return results;
        }

        private static CommandToken resolveCommandToken(com.asakusafw.lang.compiler.api.reference.CommandToken token) {
            switch (token.getTokenKind()) {
            case TEXT:
                return CommandToken.of(token.getImage());
            case BATCH_ID:
                return CommandToken.BATCH_ID;
            case FLOW_ID:
                return CommandToken.FLOW_ID;
            case BATCH_ARGUMENTS:
                return CommandToken.BATCH_ARGUMENTS;
            case EXECUTION_ID:
                return CommandToken.EXECUTION_ID;
            default:
                throw new AssertionError(token.getTokenKind());
            }
        }
    }
}
