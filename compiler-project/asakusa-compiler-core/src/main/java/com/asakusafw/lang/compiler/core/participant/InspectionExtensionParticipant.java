/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.participant;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerContext;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.core.util.DiagnosticUtil;
import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.InspectionNodeRepository;
import com.asakusafw.lang.compiler.inspection.driver.InspectionExtension;
import com.asakusafw.lang.compiler.inspection.driver.ObjectInspector;
import com.asakusafw.lang.compiler.inspection.json.JsonInspectionNodeRepository;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A compiler participant for enabling {@link InspectionExtension}.
 */
public class InspectionExtensionParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(InspectionExtensionParticipant.class);

    static final String KEY_PREFIX = "inspection."; //$NON-NLS-1$

    /**
     * The property key whether DSL inspection is enabled or not.
     */
    public static final String KEY_DSL = KEY_PREFIX + "dsl"; //$NON-NLS-1$

    /**
     * The property key whether task inspection is enabled or not.
     */
    public static final String KEY_TASK = KEY_PREFIX + "task"; //$NON-NLS-1$

    static final Location OUTPUT_BASE = Location.of("etc/inspection"); //$NON-NLS-1$

    static final Location OUTPUT_DSL = OUTPUT_BASE.append("dsl.json"); //$NON-NLS-1$

    static final Location OUTPUT_TASK = OUTPUT_BASE.append("task.json"); //$NON-NLS-1$

    @Override
    public void beforeBatch(BatchCompiler.Context context, Batch batch) {
        setUp(context, context.getOutput());
    }

    @Override
    public void beforeJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        setUp(context, context.getOutput());
    }

    @Override
    public void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
        if (isEnabled(context, KEY_DSL, true)) {
            inspect(context, OUTPUT_DSL, batch);
        }
        if (isEnabled(context, KEY_TASK, true)) {
            inspect(context, OUTPUT_TASK, reference);
        }
        cleanUp(context);
    }

    @Override
    public void afterJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        cleanUp(context);
    }

    private InspectionExtension setUp(ExtensionContainer.Editable extensions, ResourceContainer output) {
        Extension extension = new Extension(new ObjectInspector(), output);
        extensions.registerExtension(InspectionExtension.class, extension);
        return extension;
    }

    private void cleanUp(ExtensionContainer.Editable extensions) {
        extensions.registerExtension(InspectionExtension.class, null);
    }

    private boolean isEnabled(CompilerContext context, String key, boolean defaultValue) {
        String value = context.getOptions().get(key, "").trim(); //$NON-NLS-1$
        if (value.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private void inspect(CompilerContext context, Location output, Object element) {
        try {
            InspectionExtension.inspect(context, output, element);
        } catch (RuntimeException e) {
            if (e instanceof DiagnosticException) {
                for (Diagnostic diagnostic : ((DiagnosticException) e).getDiagnostics()) {
                    DiagnosticUtil.log(LOG, diagnostic);
                }
            }
            LOG.warn(MessageFormat.format(
                    "error occurred while inspecting element: {0}",
                    output), e);
        }
    }

    private static class Extension extends InspectionExtension {

        private final ObjectInspector inspector;

        private final ResourceContainer container;

        private final InspectionNodeRepository repository = new JsonInspectionNodeRepository();

        public Extension(ObjectInspector inspector, ResourceContainer container) {
            this.inspector = inspector;
            this.container = container;
        }

        @Override
        public boolean isSupported(Object element) {
            return inspector.isSupported(element);
        }

        @Override
        public void inspect(Location location, Object element) {
            InspectionNode node = inspector.inspect(element);
            try (OutputStream output = container.addResource(location)) {
                repository.store(output, node);
            } catch (IOException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "failed to save inspection object: {0}",
                        node), e);
            }
        }
    }
}
