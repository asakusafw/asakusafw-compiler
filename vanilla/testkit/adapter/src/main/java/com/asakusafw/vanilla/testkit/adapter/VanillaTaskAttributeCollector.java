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
package com.asakusafw.vanilla.testkit.adapter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.internalio.InternalIoConstants;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;
import com.asakusafw.lang.compiler.testdriver.adapter.TaskAttributeCollector;
import com.asakusafw.lang.compiler.tester.ExternalPortMap;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.vanilla.compiler.common.VanillaTask;
import com.asakusafw.vanilla.testkit.common.VanillaTaskInfo;
import com.asakusafw.workflow.model.TaskInfo;
import com.google.common.base.Objects;

class VanillaTaskAttributeCollector implements TaskAttributeCollector {

    @Override
    public Collection<? extends TaskInfo.Attribute> collect(JobflowArtifact jobflow, TaskReference task) {
        if (isSupported(task) == false) {
            return Collections.emptySet();
        }
        boolean internalIoOnly = collectExternalPorts(jobflow)
                .map(ExternalPortInfo::getModuleName)
                .filter(it -> it != null)
                .allMatch(Predicate.isEqual(InternalIoConstants.MODULE_NAME));

        Set<VanillaTaskInfo.Requiremnt> requirements;
        if (internalIoOnly) {
            requirements = Collections.emptySet();
        } else {
            requirements = EnumSet.of(VanillaTaskInfo.Requiremnt.CORE_CONFIGURATION_FILE);
        }
        return Arrays.asList(new VanillaTaskInfo(requirements));
    }

    private static boolean isSupported(TaskReference task) {
        return task instanceof CommandTaskReference
                && Objects.equal(task.getModuleName(), VanillaTask.MODULE_NAME)
                && Objects.equal(((CommandTaskReference) task).getProfileName(), VanillaTask.PROFILE_NAME)
                && Objects.equal(((CommandTaskReference) task).getCommand(), VanillaTask.PATH_COMMAND);
    }

    private static Stream<ExternalPortInfo> collectExternalPorts(JobflowArtifact jobflow) {
        ExternalPortMap ports = jobflow.getExternalPorts();
        return Stream.concat(
                ports.getInputs().stream().map(ports::findInputInfo),
                ports.getOutputs().stream().map(ports::findOutputInfo));
    }
}
