/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.yaess;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.hadoop.HadoopCommandRequired;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.yaess.core.BatchScript;
import com.asakusafw.yaess.core.CommandScript;
import com.asakusafw.yaess.core.ExecutionPhase;
import com.asakusafw.yaess.core.ExecutionScript;
import com.asakusafw.yaess.core.FlowScript;
import com.asakusafw.yaess.core.HadoopScript;

/**
 * An implementation of {@link BatchProcessor} for generating YAESS scripts.
 */
public class YaessBatchProcessor implements BatchProcessor {

    static final Logger LOG = LoggerFactory.getLogger(YaessBatchProcessor.class);

    /**
     * The script output path.
     */
    public static final String PATH = "etc/yaess-script.properties"; //$NON-NLS-1$

    /**
     * Computes and returns the path to the YAESS script output.
     * @param outputDir compilation output path
     * @return the script output path
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public static File getScriptOutput(File outputDir) {
        return new File(outputDir, PATH);
    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        LOG.debug("anayzing batch structure for YAESS"); //$NON-NLS-1$
        List<FlowScript> scripts = processJobflowList(source);

        LOG.debug("building YAESS batch script"); //$NON-NLS-1$
        Properties properties = new Properties();
        properties.setProperty(BatchScript.KEY_ID, source.getBatchId());
        properties.setProperty(BatchScript.KEY_VERSION, BatchScript.VERSION);
        properties.setProperty(BatchScript.KEY_VERIFICATION_CODE, context.getOptions().getBuildId());

        for (FlowScript script : scripts) {
            LOG.trace("building YAESS flow script: {}", script.getId()); //$NON-NLS-1$
            script.storeTo(properties);
        }

        LOG.debug("exporting YAESS batch script"); //$NON-NLS-1$
        try (OutputStream output = context.addResourceFile(Location.of(PATH))) {
            properties.store(output, MessageFormat.format(
                    "YAESS batch script for \"{0}\", version {1}", //$NON-NLS-1$
                    source.getBatchId(),
                    BatchScript.VERSION));
        }
        LOG.debug("exported YAESS batch script"); //$NON-NLS-1$
    }

    private List<FlowScript> processJobflowList(BatchReference batch) {
        List<FlowScript> results = new ArrayList<>();
        for (JobflowReference jobflow : batch.getJobflows()) {
            FlowScript script = processJobflow(batch, jobflow);
            results.add(script);
        }
        return results;
    }

    private FlowScript processJobflow(BatchReference batch, JobflowReference jobflow) {
        Set<String> blockerIds = new LinkedHashSet<>();
        for (JobflowReference blocker : jobflow.getBlockers()) {
            blockerIds.add(blocker.getFlowId());
        }
        Map<ExecutionPhase, List<ExecutionScript>> scripts = new EnumMap<>(ExecutionPhase.class);
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            ExecutionPhase execution = convert(phase);
            scripts.put(execution, processPhase(batch, jobflow, phase));
        }
        Set<ExecutionScript.Kind> enables = EnumSet.allOf(ExecutionScript.Kind.class);
        if (isHadoopCommandRequired(jobflow) == false) {
            LOG.debug("jobflow \"{}.{}\" does not require Hadoop command", //$NON-NLS-1$
                    batch.getBatchId(), jobflow.getFlowId());
            enables.remove(ExecutionScript.Kind.HADOOP);
        }
        return new FlowScript(jobflow.getFlowId(), blockerIds, scripts, enables);
    }

    private List<ExecutionScript> processPhase(
            BatchReference batch, JobflowReference jobflow,
            TaskReference.Phase phase) {
        List<TaskReference> tasks = new ArrayList<>(jobflow.getTasks(phase));
        Map<TaskReference, String> idMap = createStageIdMap(tasks);
        List<ExecutionScript> results = new ArrayList<>();
        for (TaskReference task : tasks) {
            ExecutionScript script = processStage(batch, jobflow, phase, task, idMap);
            results.add(script);
        }
        return results;
    }

    private Map<TaskReference, String> createStageIdMap(List<TaskReference> tasks) {
        Map<String, Set<TaskReference>> moduleMap = new HashMap<>();
        Map<TaskReference, String> results = new HashMap<>();
        for (TaskReference task : tasks) {
            String name = task.getModuleName();
            Set<TaskReference> group = moduleMap.get(name);
            if (group == null) {
                group = new HashSet<>();
                moduleMap.put(name, group);
            }
            int index = group.size();
            group.add(task);
            results.put(task, String.format("%s%04d", name, index)); //$NON-NLS-1$
        }
        return results;
    }

    private ExecutionScript processStage(
            BatchReference batch, JobflowReference jobflow,
            TaskReference.Phase phase, TaskReference task,
            Map<TaskReference, String> idMap) {
        if (task instanceof CommandTaskReference) {
            return processCommandStage(batch, jobflow, phase, (CommandTaskReference) task, idMap);
        } else if (task instanceof HadoopTaskReference) {
            return processHadoopStage(batch, jobflow, phase, (HadoopTaskReference) task, idMap);
        } else {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "unsupported task type: {0} ({1})",
                    task.getClass().getName(),
                    task));
        }
    }

    private CommandScript processCommandStage(
            BatchReference batch, JobflowReference jobflow,
            TaskReference.Phase phase, CommandTaskReference task,
            Map<TaskReference, String> idMap) {
        String stageId = idMap.get(task);
        assert stageId != null;
        Set<String> blockerIds = toStageIds(task.getBlockers(), idMap);
        List<String> command = resolveCommandLine(batch, jobflow, phase, task);
        Map<String, String> envs = Collections.emptyMap();
        Set<String> extensions = task.getExtensions();
        return new CommandScript(
                stageId, blockerIds,
                task.getProfileName(), task.getModuleName(),
                command, envs, extensions);
    }

    private List<String> resolveCommandLine(
            BatchReference batch, JobflowReference jobflow,
            TaskReference.Phase phase, CommandTaskReference task) {
        List<String> results = new ArrayList<>();
        results.add(String.format("%s/%s", //$NON-NLS-1$
                ExecutionScript.PLACEHOLDER_HOME,
                task.getCommand().toPath()));
        for (CommandToken argument : task.getArguments()) {
            switch (argument.getTokenKind()) {
            case TEXT:
                results.add(argument.getImage());
                break;
            case BATCH_ID:
                results.add(batch.getBatchId());
                break;
            case FLOW_ID:
                results.add(jobflow.getFlowId());
                break;
            case EXECUTION_ID:
                results.add(ExecutionScript.PLACEHOLDER_EXECUTION_ID);
                break;
            case BATCH_ARGUMENTS:
                results.add(ExecutionScript.PLACEHOLDER_ARGUMENTS);
                break;
            default:
                throw new AssertionError(argument);
            }
        }
        return results;
    }

    private HadoopScript processHadoopStage(
            BatchReference batch, JobflowReference jobflow,
            TaskReference.Phase phase, HadoopTaskReference task,
            Map<TaskReference, String> idMap) {
        String stageId = idMap.get(task);
        assert stageId != null;
        Set<String> blockerIds = toStageIds(task.getBlockers(), idMap);
        String className = task.getMainClass().getBinaryName();
        Map<String, String> props = Collections.emptyMap();
        Map<String, String> envs = Collections.emptyMap();
        Set<String> extensions = task.getExtensions();
        return new HadoopScript(stageId, blockerIds, className, props, envs, extensions);
    }

    private boolean isHadoopCommandRequired(JobflowReference jobflow) {
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

    private Set<String> toStageIds(Collection<? extends TaskReference> tasks, Map<TaskReference, String> idMap) {
        Set<String> results = new HashSet<>();
        for (TaskReference task : tasks) {
            String id = idMap.get(task);
            assert id != null;
            results.add(id);
        }
        return results;
    }

    private ExecutionPhase convert(TaskReference.Phase phase) {
        return ExecutionPhase.valueOf(phase.name());
    }
}
