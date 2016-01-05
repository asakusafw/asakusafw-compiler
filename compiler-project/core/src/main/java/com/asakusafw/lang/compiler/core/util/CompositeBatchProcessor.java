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
package com.asakusafw.lang.compiler.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;

/**
 * Composition of {@link BatchProcessor}s.
 */
public class CompositeBatchProcessor extends AbstractCompositeElement<BatchProcessor> implements BatchProcessor {

    private final List<BatchProcessor> elements;

    /**
     * Creates a new instance.
     * @param elements the element processors
     */
    public CompositeBatchProcessor(List<? extends BatchProcessor> elements) {
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    public List<BatchProcessor> getElements() {
        return elements;
    }

    /**
     * Composites the element processors.
     * @param elements the element processors
     * @return the composite processor
     */
    public static BatchProcessor composite(List<? extends BatchProcessor> elements) {
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new CompositeBatchProcessor(elements);
    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (BatchProcessor element : elements) {
            try {
                element.process(context, source);
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
