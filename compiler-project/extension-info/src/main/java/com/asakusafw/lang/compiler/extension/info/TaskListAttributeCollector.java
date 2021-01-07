/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.info;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.asakusafw.info.task.TaskInfo;
import com.asakusafw.info.task.TaskListAttribute;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.info.AttributeCollector;

/**
 * Collects {@link TaskListAttribute}.
 * @since 0.4.2
 */
public class TaskListAttributeCollector implements AttributeCollector {

    static final String PROFILE_HADOOP = "hadoop";

    @Override
    public void process(Context context, JobflowReference jobflow) {
        List<TaskInfo> tasks = new ArrayList<>();
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            List<TaskReference> all = new ArrayList<>(jobflow.getTasks(phase));
            all.forEach(it -> tasks.add(convert(all, phase, it)));
        }
        context.putAttribute(new TaskListAttribute(tasks));
    }

    private static TaskInfo convert(List<TaskReference> all, TaskReference.Phase phase, TaskReference task) {
        return new TaskInfo(
                toId(all.indexOf(task)),
                convert(phase),
                task.getModuleName(),
                Optional.of(task)
                    .filter(it -> it instanceof CommandTaskReference)
                    .map(it -> (CommandTaskReference) it)
                    .map(CommandTaskReference::getProfileName)
                    .orElse(PROFILE_HADOOP),
                task.getBlockers().stream()
                    .map(all::indexOf)
                    .filter(it -> it != null)
                    .sorted()
                    .map(String::valueOf)
                    .collect(Collectors.toList()));
    }

    private static TaskInfo.Phase convert(TaskReference.Phase phase) {
        switch (phase) {
        case INITIALIZE:
            return TaskInfo.Phase.INITIALIZE;
        case IMPORT:
            return TaskInfo.Phase.IMPORT;
        case PROLOGUE:
            return TaskInfo.Phase.PROLOGUE;
        case MAIN:
            return TaskInfo.Phase.MAIN;
        case EPILOGUE:
            return TaskInfo.Phase.EPILOGUE;
        case EXPORT:
            return TaskInfo.Phase.EXPORT;
        case FINALIZE:
            return TaskInfo.Phase.FINALIZE;
        default:
            throw new AssertionError(phase);
        }
    }

    private static String toId(int id) {
        return String.valueOf(id);
    }
}
