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
package com.asakusafw.lang.compiler.extension.iterative;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.asakusafw.lang.compiler.analyzer.JobflowAnalyzer;
import com.asakusafw.lang.compiler.analyzer.adapter.JobflowAdapter;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.iterative.IterativeBatch;

/**
 * Analyzes a <em>iterative batch class</em>.
 * @since 0.3.0
 */
public class IterativeBatchAnalyzer {

    /**
     * The constant flow ID for iterative batches.
     */
    public static final String FLOW_ID = "main"; //$NON-NLS-1$

    private static final Pattern PATTERN_ID = Pattern.compile(
            "[A-Za-z_][0-9A-Za-z_]*(\\.[A-Za-z_][0-9A-Za-z_]*)*"); //$NON-NLS-1$

    private final JobflowAnalyzer elementAnalyzer;

    /**
     * Creates a new instance.
     * @param elementAnalyzer the jobflow analyzer
     */
    public IterativeBatchAnalyzer(JobflowAnalyzer elementAnalyzer) {
        Objects.requireNonNull(elementAnalyzer);
        this.elementAnalyzer = elementAnalyzer;
    }

    /**
     * Analyzes the target <em>iterative batch class</em> and returns a complete graph model object.
     * @param aClass the target class
     * @return the related complete graph model object
     */
    public Batch analyze(Class<?> aClass) {
        BatchInfo info = analyzeInfo(aClass);
        Jobflow flow = analyzeFlow(aClass);
        Batch batch = new Batch(info);
        batch.addElement(flow);
        return batch;
    }

    private Jobflow analyzeFlow(Class<?> aClass) {
        JobflowInfo info = new JobflowInfo.Basic(FLOW_ID, Descriptions.classOf(aClass));
        JobflowAdapter adapter = JobflowAdapter.analyze(info, aClass);
        return elementAnalyzer.analyze(adapter);
    }

    /**
     * Returns whether the target is a <em>batch class</em> or not.
     * @param aClass the target class
     * @return {@code true} if the target is a <em>batch class</em> class, otherwise {@code false}
     */
    public static boolean isBatch(Class<?> aClass) {
        if (aClass.isAnnotationPresent(IterativeBatch.class) == false) {
            return false;
        }
        if (aClass == FlowDescription.class || FlowDescription.class.isAssignableFrom(aClass) == false) {
            return false;
        }
        return true;
    }

    /**
     * Analyzes the target <em>batch class</em> and returns its structural information.
     * @param aClass the target class
     * @return structural information of the target batch
     * @throws DiagnosticException if the target class is not a valid <em>batch class</em>
     */
    public static BatchInfo analyzeInfo(Class<?> aClass) {
        IterativeBatch annotation = aClass.getAnnotation(IterativeBatch.class);
        if (annotation == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "iterative batch must be annotated with @{1} ({0})",
                    aClass.getName(),
                    IterativeBatch.class.getSimpleName()));
        }
        if (FlowDescription.class.isAssignableFrom(aClass) == false) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "iterative batch must be a subtype of {1} ({0})",
                    aClass.getName(),
                    FlowDescription.class.getSimpleName()));
        }
        String batchId = annotation.name();
        if (isValidIdentifier(batchId) == false) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "batch ID must be a valid identifier \"{1}\" ({0})",
                    aClass.getName(),
                    batchId));
        }
        List<BatchInfo.Parameter> parameters = analyzeParameters(aClass, annotation);
        Set<BatchInfo.Attribute> attributes = analyzeAttributes(annotation);
        return new BatchInfo.Basic(
                batchId,
                Descriptions.classOf(aClass),
                analyzeComment(annotation.comment()),
                parameters,
                attributes);
    }

    private static List<BatchInfo.Parameter> analyzeParameters(Class<?> aClass, IterativeBatch annotation) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        List<BatchInfo.Parameter> parameters = new ArrayList<>();
        for (IterativeBatch.Parameter parameter : annotation.parameters()) {
            Pattern pattern;
            if (parameter.pattern().equals(IterativeBatch.DEFAULT_PARAMETER_VALUE_PATTERN)) {
                pattern = null;
            } else {
                try {
                    pattern = Pattern.compile(parameter.pattern());
                } catch (PatternSyntaxException e) {
                    raise(diagnostics, aClass, MessageFormat.format(
                            "parameter \"{0}\" must have valid regex pattern: \"{1}\"",
                            parameter.key(),
                            parameter.pattern()));
                    continue;
                }
            }
            parameters.add(new BatchInfo.Parameter(
                    parameter.key(),
                    analyzeComment(parameter.comment()),
                    parameter.required(),
                    pattern));
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        return parameters;
    }

    private static Set<BatchInfo.Attribute> analyzeAttributes(IterativeBatch annotation) {
        Set<BatchInfo.Attribute> attributes = EnumSet.noneOf(BatchInfo.Attribute.class);
        if (annotation.strict()) {
            attributes.add(BatchInfo.Attribute.STRICT_PARAMETERS);
        }
        return attributes;
    }

    private static String analyzeComment(String comment) {
        if (comment == null || comment.isEmpty()) {
            return null;
        }
        return comment;
    }

    private static void raise(List<Diagnostic> diagnostics, Class<?> atClass, String message) {
        String decorated = MessageFormat.format(
                "{0} ({1})", //$NON-NLS-1$
                message,
                atClass.getName());
        diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, decorated, null));
    }

    private static boolean isValidIdentifier(String id) {
        return PATTERN_ID.matcher(id).matches();
    }
}
