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
package com.asakusafw.vanilla.compiler.core;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.compiler.codegen.ApplicationGenerator;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.CleanupStageClientGenerator;
import com.asakusafw.dag.compiler.flow.DataFlowGenerator;
import com.asakusafw.dag.compiler.flow.adapter.ClassGeneratorContextAdapter;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.planner.DagPlanning;
import com.asakusafw.lang.compiler.api.Exclusive;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.hadoop.HadoopCommandRequired;
import com.asakusafw.lang.compiler.inspection.InspectionExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.vanilla.compiler.common.VanillaPackage;
import com.asakusafw.vanilla.compiler.common.VanillaTask;

/**
 * An implementation of {@link JobflowProcessor} for Asakusa Vanilla.
 * @since 0.4.0
 */
@Exclusive
public class VanillaJobflowProcessor implements JobflowProcessor {

    static final Logger LOG = LoggerFactory.getLogger(VanillaJobflowProcessor.class);

    /**
     * The compiler option key prefix.
     */
    public static final String KEY_PREFIX = "vanilla."; //$NON-NLS-1$

    static final String KEY_CODEGEN = KEY_PREFIX + "codegen"; //$NON-NLS-1$

    @Override
    public void process(Context context, Jobflow source) throws IOException {
        LOG.debug("computing execution plan: {}", source.getFlowId());
        Plan plan = plan(context, source);
        try {
            if (context.getOptions().get(KEY_CODEGEN, true) == false) {
                LOG.info("code generation was skipped: {} ({}=true)", source.getFlowId(), KEY_CODEGEN);
                return;
            }
            LOG.debug("generating vertices: {}", source.getFlowId());
            GraphInfo graph = generateGraph(context, source, plan);
            LOG.debug("generating application entry: {}", source.getFlowId());
            addApplication(context, source, graph);
            LOG.debug("generating cleanup : {}", source.getFlowId());
            addCleanup(context, source);
        } finally {
            LOG.debug("generating inspection info: {} ({})",
                    source.getFlowId(), VanillaPackage.PATH_PLAN_INSPECTION);
            InspectionExtension.inspect(context, VanillaPackage.PATH_PLAN_INSPECTION, plan);
        }
    }

    private Plan plan(Context context, Jobflow source) {
        PlanDetail detail = DagPlanning.plan(context, source);
        return detail.getPlan();
    }

    private GraphInfo generateGraph(JobflowProcessor.Context context, JobflowInfo info, Plan plan) {
        ClassGeneratorContext cgContext = new ClassGeneratorContextAdapter(context, VanillaPackage.CLASS_PREFIX);
        VanillaDescriptorFactory descriptors = new VanillaDescriptorFactory(cgContext);
        return DataFlowGenerator.generate(context, cgContext, descriptors, info, plan);
    }

    private void addApplication(JobflowProcessor.Context context, JobflowInfo info, GraphInfo graph) {
        add(context, VanillaPackage.PATH_GRAPH_INFO, output -> GraphInfo.save(output, graph));
        ClassDescription application = add(context, new ApplicationGenerator().generate(
                VanillaPackage.PATH_GRAPH_INFO,
                new ClassDescription(VanillaPackage.CLASS_APPLICATION)));
        TaskReference task = context.addTask(
                VanillaTask.MODULE_NAME,
                VanillaTask.PROFILE_NAME,
                VanillaTask.PATH_COMMAND,
                Arrays.asList(new CommandToken[] {
                        CommandToken.BATCH_ID,
                        CommandToken.FLOW_ID,
                        CommandToken.EXECUTION_ID,
                        CommandToken.BATCH_ARGUMENTS,
                        CommandToken.of(application.getBinaryName()),
                }),
                Arrays.asList());
        HadoopCommandRequired.put(task, false);
    }

    private void addCleanup(JobflowProcessor.Context context, JobflowInfo info) {
        add(context, new CleanupStageClientGenerator().generate(
                context.getBatchId(),
                info.getFlowId(),
                context.getOptions().getRuntimeWorkingDirectory(),
                CleanupStageClientGenerator.DEFAULT_CLASS));
    }

    private void add(JobflowProcessor.Context context, Location location, Action<OutputStream, IOException> action) {
        try (OutputStream output = context.addResourceFile(location)) {
            action.perform(output);
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while adding a resource: {0}",
                    location), e);
        }
    }

    private ClassDescription add(JobflowProcessor.Context context, ClassData data) {
        if (data.hasContents()) {
            try (OutputStream output = context.addClassFile(data.getDescription())) {
                data.dump(output);
            } catch (IOException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "error occurred while generating a class file: {0}",
                        data.getDescription().getBinaryName()), e);
            }
        }
        return data.getDescription();
    }
}
