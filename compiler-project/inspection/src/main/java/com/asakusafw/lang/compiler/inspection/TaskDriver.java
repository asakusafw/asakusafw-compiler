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
package com.asakusafw.lang.compiler.inspection;

import static com.asakusafw.lang.compiler.inspection.Util.*;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.BlockingReference;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Inspects task elements.
 */
public class TaskDriver {

    static final String PROPERTY_DESCRIPTION = "description"; //$NON-NLS-1$

    static final String PROPERTY_MODULE = "module"; //$NON-NLS-1$

    /**
     * Inspects the target batch.
     * @param object the target batch
     * @return the inspection object
     */
    public InspectionNode inspect(BatchReference object) {
        String title = "Batch"; //$NON-NLS-1$
        InspectionNode node = new InspectionNode(id(object), title);
        node.withProperty(PROPERTY_KIND, "Batch"); //$NON-NLS-1$
        node.getProperties().putAll(extract(object));
        for (InspectionNode element : inspectJobflows(object.getJobflows()).values()) {
            node.withElement(element);
        }
        node.getProperties().putAll(extractAttributes(object));
        return node;
    }

    /**
     * Inspects the target jobflow.
     * @param object the target jobflow
     * @return the inspection object
     */
    public InspectionNode inspect(JobflowReference object) {
        String title = "Jobflow"; //$NON-NLS-1$
        InspectionNode node = new InspectionNode(id(object), title);
        node.withProperty(PROPERTY_KIND, "Jobflow"); //$NON-NLS-1$
        node.getProperties().putAll(extract(object));
        for (InspectionNode element : inspectPhases(object).values()) {
            node.withElement(element);
        }
        node.getProperties().putAll(extractAttributes(object));
        return node;
    }

    Map<TaskReference.Phase, InspectionNode> inspectPhases(TaskReferenceMap taskMap) {
        Map<TaskReference.Phase, InspectionNode> results = new LinkedHashMap<>();
        InspectionNode last = null;
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            String title = "Phase"; //$NON-NLS-1$
            InspectionNode node = new InspectionNode(id(phase), title);
            node.withProperty(PROPERTY_KIND, "Phase"); //$NON-NLS-1$
            addDependencyPorts(node);
            if (last != null) {
                addDependency(last, node);
            }
            last = node;
            results.put(phase, node);
        }
        for (Map.Entry<TaskReference.Phase, InspectionNode> entry : results.entrySet()) {
            Collection<? extends TaskReference> tasks = taskMap.getTasks(entry.getKey());
            InspectionNode node = entry.getValue();
            for (InspectionNode element : inspectTasks(tasks).values()) {
                node.withElement(element);
            }
        }
        return results;
    }

    Map<JobflowReference, InspectionNode> inspectJobflows(Collection<? extends JobflowReference> elements) {
        Map<JobflowReference, InspectionNode> results = new LinkedHashMap<>();
        for (JobflowReference element : elements) {
            InspectionNode node = inspect(element);
            results.put(element, node);
        }
        resolveDependencies(results);
        return results;
    }

    Map<TaskReference, InspectionNode> inspectTasks(Collection<? extends TaskReference> elements) {
        Counter counter = new Counter();
        Map<TaskReference, InspectionNode> results = new LinkedHashMap<>();
        for (TaskReference element : elements) {
            String id = counter.fetchId(element.getModuleName());
            InspectionNode node = inspect(id, element);
            results.put(element, node);
        }
        resolveDependencies(results);
        return results;
    }

    InspectionNode inspect(String id, TaskReference object) {
        InspectionNode node;
        if (object instanceof CommandTaskReference) {
            node = inspectFlat(id, (CommandTaskReference) object);
        } else if (object instanceof HadoopTaskReference) {
            node = inspectFlat(id, (HadoopTaskReference) object);
        } else {
            String title = String.format("Unknown(%s)", getPrintableName(object.getClass())); //$NON-NLS-1$
            node = new InspectionNode(id, title);
            node.withProperty(PROPERTY_KIND, title);
            node.withProperty(PROPERTY_MODULE, object.getModuleName());
        }
        node.getProperties().putAll(extractAttributes(object));
        return node;
    }

    private String getPrintableName(Class<?> aClass) {
        if (aClass.isAnonymousClass() == false) {
            return aClass.getSimpleName();
        }
        Class<?> parent = aClass.getSuperclass();
        if (parent != null && parent != Object.class) {
            return getPrintableName(parent);
        }
        for (Class<?> intf : aClass.getInterfaces()) {
            return getPrintableName(intf);
        }
        return "?"; //$NON-NLS-1$
    }

    private InspectionNode inspectFlat(String id, CommandTaskReference object) {
        String title = "Command"; //$NON-NLS-1$
        InspectionNode node = new InspectionNode(id, title);
        node.withProperty(PROPERTY_KIND, "CommandTask"); //$NON-NLS-1$
        node.withProperty(PROPERTY_MODULE, object.getModuleName());
        node.withProperty("profile", object.getProfileName()); //$NON-NLS-1$
        node.withProperty("command", object.getCommand().toPath()); //$NON-NLS-1$
        int index = 0;
        for (CommandToken token : object.getArguments()) {
            node.withProperty(
                    String.format("arguments.%d", index++), //$NON-NLS-1$
                    token.toString());
        }
        return node;
    }

    private InspectionNode inspectFlat(String id, HadoopTaskReference object) {
        String title = "Hadoop"; //$NON-NLS-1$
        InspectionNode node = new InspectionNode(id, title);
        node.withProperty(PROPERTY_KIND, "HadoopTask"); //$NON-NLS-1$
        node.withProperty(PROPERTY_MODULE, object.getModuleName());
        node.withProperty("class", object.getMainClass().getClassName()); //$NON-NLS-1$
        return node;
    }

    static <T extends BlockingReference<T>> void resolveDependencies(Map<T, InspectionNode> elements) {
        for (InspectionNode node : elements.values()) {
            addDependencyPorts(node);
        }
        for (Map.Entry<T, InspectionNode> entry : elements.entrySet()) {
            T downstreamElement = entry.getKey();
            InspectionNode downstreamNode = entry.getValue();
            for (T upstreamElement : downstreamElement.getBlockers()) {
                InspectionNode upstreamNode = elements.get(upstreamElement);
                assert upstreamNode != null;
                addDependency(upstreamNode, downstreamNode);
            }
        }
    }
}
