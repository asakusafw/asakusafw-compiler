/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * Composition of {@link CompilerParticipant}s.
 */
public class CompositeCompilerParticipant extends AbstractCompositeElement<CompilerParticipant>
        implements CompilerParticipant {

    private final List<CompilerParticipant> elements;

    private final List<CompilerParticipant> reverse;

    /**
     * Creates a new instance.
     * @param elements the element participants
     */
    public CompositeCompilerParticipant(List<? extends CompilerParticipant> elements) {
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
        this.reverse = new ArrayList<>(elements);
        Collections.reverse(reverse);
    }

    @Override
    public List<CompilerParticipant> getElements() {
        return elements;
    }

    /**
     * Composites the element participants.
     * @param elements the element participants
     * @return the composite participant
     */
    public static CompilerParticipant composite(List<? extends CompilerParticipant> elements) {
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new CompositeCompilerParticipant(elements);
    }

    @Override
    public void beforeBatch(BatchCompiler.Context context, Batch batch) {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (CompilerParticipant element : elements) {
            try {
                element.beforeBatch(context, batch);
            } catch (DiagnosticException e) {
                error = true;
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (error) {
            throw new DiagnosticException(diagnostics);
        }
    }

    @Override
    public void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (CompilerParticipant element : reverse) {
            try {
                element.afterBatch(context, batch, reference);
            } catch (DiagnosticException e) {
                error = true;
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (error) {
            throw new DiagnosticException(diagnostics);
        }
    }

    @Override
    public void beforeJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (CompilerParticipant element : elements) {
            try {
                element.beforeJobflow(context, batch, jobflow);
            } catch (DiagnosticException e) {
                error = true;
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (error) {
            throw new DiagnosticException(diagnostics);
        }
    }

    @Override
    public void afterJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (CompilerParticipant element : reverse) {
            try {
                element.afterJobflow(context, batch, jobflow);
            } catch (DiagnosticException e) {
                error = true;
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (error) {
            throw new DiagnosticException(diagnostics);
        }
    }
}
