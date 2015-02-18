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
package com.asakusafw.lang.compiler.analyzer.adapter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.vocabulary.batch.BatchDescription;

/**
 * A DSL adapter for batches.
 */
public class BatchAdapter {

    private static final Pattern PATTERN_ID = Pattern.compile(
            "[A-Za-z_][0-9A-Za-z_]*(\\.[A-Za-z_][0-9A-Za-z_]*)*"); //$NON-NLS-1$

    private static final Object[] EMPTY = new Object[0];

    private final BatchInfo info;

    private final Constructor<? extends BatchDescription> constructor;

    /**
     * Creates a new instance.
     * @param info the structural batch information
     * @param constructor the batch constructor
     */
    public BatchAdapter(BatchInfo info, Constructor<? extends BatchDescription> constructor) {
        this.info = info;
        this.constructor = constructor;
    }

    /**
     * Returns structural information of this batch.
     * @return structural information
     */
    public BatchInfo getInfo() {
        return info;
    }

    /**
     * Returns the batch description.
     * @return the batch description
     */
    public Class<? extends BatchDescription> getDescription() {
        return getConstructor().getDeclaringClass();
    }

    /**
     * Returns the batch constructor.
     * @return the constructor
     */
    public Constructor<? extends BatchDescription> getConstructor() {
        return constructor;
    }

    /**
     * Creates a new batch description instance.
     * @return the created instance
     */
    public BatchDescription newInstance() {
        try {
            return constructor.newInstance(EMPTY);
        } catch (Exception e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to initialize batch: {0}",
                    constructor.getDeclaringClass().getName()), e);
        }
    }

    /**
     * Returns whether the target is a <em>batch class</em> or not.
     * @param aClass the target class
     * @return {@code true} if the target is a <em>batch class</em> class, otherwise {@code false}
     */
    public static boolean isBatch(Class<?> aClass) {
        if (aClass.isAnnotationPresent(com.asakusafw.vocabulary.batch.Batch.class) == false) {
            return false;
        }
        if (aClass == BatchDescription.class
                || BatchDescription.class.isAssignableFrom(aClass) == false) {
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
        com.asakusafw.vocabulary.batch.Batch annotation =
                aClass.getAnnotation(com.asakusafw.vocabulary.batch.Batch.class);
        if (annotation == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "batch must be annotated with @{1} ({0})",
                    aClass.getName(),
                    com.asakusafw.vocabulary.flow.JobFlow.class.getSimpleName()));
        }
        if (BatchDescription.class.isAssignableFrom(aClass) == false) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "batch must be a subtype of {1} ({0})",
                    aClass.getName(),
                    BatchDescription.class.getSimpleName()));
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

    private static List<BatchInfo.Parameter> analyzeParameters(
            Class<?> aClass,
            com.asakusafw.vocabulary.batch.Batch annotation) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        List<BatchInfo.Parameter> parameters = new ArrayList<>();
        for (com.asakusafw.vocabulary.batch.Batch.Parameter parameter : annotation.parameters()) {
            Pattern pattern;
            if (parameter.pattern().equals(com.asakusafw.vocabulary.batch.Batch.DEFAULT_PARAMETER_VALUE_PATTERN)) {
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

    private static Set<BatchInfo.Attribute> analyzeAttributes(com.asakusafw.vocabulary.batch.Batch annotation) {
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

    /**
     * Analyzes the target <em>batch class</em> and returns its DSL adapter object.
     * @param aClass the target class
     * @return the DSL adapter for the target <em>batch class</em>
     * @throws DiagnosticException if the target class is not a valid <em>batch class</em>
     */
    public static BatchAdapter analyze(Class<?> aClass) {
        BatchInfo info = analyzeInfo(aClass);
        Constructor<? extends BatchDescription> constructor = detectConstructor(aClass);
        return new BatchAdapter(info, constructor);
    }

    private static Constructor<? extends BatchDescription> detectConstructor(Class<?> aClass) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        if (aClass.getEnclosingClass() != null) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "batch class must be top-level ({0})",
                    aClass.getName()));
        }
        if (Modifier.isPublic(aClass.getModifiers()) == false) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "batch class must be public ({0})",
                    aClass.getName()));
        }
        if (Modifier.isAbstract(aClass.getModifiers())) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "batch class must not be abstract ({0})",
                    aClass.getName()));
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        try {
            return aClass.asSubclass(BatchDescription.class).getConstructor();
        } catch (ReflectiveOperationException e) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "batch class must have a zero-parameters public constructor ({0})",
                    aClass.getName()));
            throw new DiagnosticException(diagnostics);
        }
    }

    private static void raise(List<Diagnostic> diagnostics, Class<?> atClass, String message) {
        String decorated = MessageFormat.format(
                "{0} ({1})",
                message,
                atClass.getName());
        diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, decorated, null));
    }

    private static boolean isValidIdentifier(String id) {
        return PATTERN_ID.matcher(id).matches();
    }
}
