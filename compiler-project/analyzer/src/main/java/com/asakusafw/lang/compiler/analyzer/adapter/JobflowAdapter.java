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
package com.asakusafw.lang.compiler.analyzer.adapter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.analyzer.util.TypeInfo;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;

/**
 * A DSL adapter for jobflows.
 * @since 0.1.0
 * @version 0.3.0
 */
public class JobflowAdapter {

    static final Logger LOG = LoggerFactory.getLogger(JobflowAdapter.class);

    private static final Pattern PATTERN_ID = Pattern.compile("[A-Za-z_][0-9A-Za-z_]*"); //$NON-NLS-1$

    private final JobflowInfo info;

    private final Constructor<? extends FlowDescription> constructor;

    private final List<Parameter> parameters;

    /**
     * Creates a new instance.
     * @param info the structural jobflow information
     * @param constructor the jobflow constructor
     * @param parameters the jobflow parameters
     */
    public JobflowAdapter(
            JobflowInfo info,
            Constructor<? extends FlowDescription> constructor,
            List<Parameter> parameters) {
        this.info = info;
        this.constructor = constructor;
        this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
    }

    /**
     * Returns structural information of this jobflow.
     * @return structural information
     */
    public JobflowInfo getInfo() {
        return info;
    }

    /**
     * Returns the jobflow description.
     * @return the jobflow description
     */
    public Class<? extends FlowDescription> getDescription() {
        return getConstructor().getDeclaringClass();
    }

    /**
     * Returns the jobflow constructor.
     * @return the constructor
     */
    public Constructor<? extends FlowDescription> getConstructor() {
        return constructor;
    }

    /**
     * Returns the jobflow parameters.
     * @return the parameters
     */
    public List<Parameter> getParameters() {
        return parameters;
    }

    /**
     * Creates a new jobflow description instance.
     * @param arguments the jobflow arguments
     * @return the created instance
     */
    public FlowDescription newInstance(List<Object> arguments) {
        try {
            return constructor.newInstance(arguments.toArray(new Object[arguments.size()]));
        } catch (Exception e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to initialize jobflow: {0}",
                    constructor.getDeclaringClass().getName()), e);
        }
    }

    /**
     * Returns whether the target is a <em>jobflow class</em> or not.
     * @param aClass the target class
     * @return {@code true} if the target is a <em>jobflow class</em> class, otherwise {@code false}
     */
    public static boolean isJobflow(Class<?> aClass) {
        if (aClass.isAnnotationPresent(com.asakusafw.vocabulary.flow.JobFlow.class) == false) {
            return false;
        }
        if (FlowDescription.class.isAssignableFrom(aClass) == false) {
            return false;
        }
        return true;
    }

    /**
     * Analyzes the target <em>jobflow class</em> and returns its structural information.
     * @param aClass the target class
     * @return structural information of the target jobflow
     * @throws DiagnosticException if the target class is not a valid <em>jobflow class</em>
     */
    public static JobflowInfo analyzeInfo(Class<?> aClass) {
        com.asakusafw.vocabulary.flow.JobFlow annotation =
                aClass.getAnnotation(com.asakusafw.vocabulary.flow.JobFlow.class);
        if (annotation == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "jobflow must be annotated with @{1} ({0})",
                    aClass.getName(),
                    com.asakusafw.vocabulary.flow.JobFlow.class.getSimpleName()));
        }
        if (FlowDescription.class.isAssignableFrom(aClass) == false) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "jobflow must be a subtype of {1} ({0})",
                    aClass.getName(),
                    FlowDescription.class.getSimpleName()));
        }
        String flowId = annotation.name();
        if (isValidIdentifier(flowId) == false) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "flow ID must be a valid identifier \"{1}\" ({0})",
                    aClass.getName(),
                    flowId));
        }
        return new JobflowInfo.Basic(flowId, Descriptions.classOf(aClass));
    }

    /**
     * Analyzes the target <em>jobflow class</em> and returns its DSL adapter object.
     * @param aClass the target class
     * @return the DSL adapter for the target <em>jobflow class</em>
     * @throws DiagnosticException if the target class is not a valid <em>jobflow class</em>
     */
    public static JobflowAdapter analyze(Class<?> aClass) {
        LOG.debug("analyzing jobflow info: {}", aClass.getName()); //$NON-NLS-1$
        JobflowInfo info = analyzeInfo(aClass);
        LOG.debug("jobflow info: {}", info); //$NON-NLS-1$
        return analyze(info, aClass);
    }

    /**
     * Analyzes the target <em>jobflow class</em> and returns its DSL adapter object.
     * @param info the target jobflow info
     * @param aClass the target class
     * @return the DSL adapter for the target <em>jobflow class</em>
     * @throws DiagnosticException if the target class is not a valid <em>jobflow class</em>
     * @since 0.3.0
     */
    public static JobflowAdapter analyze(JobflowInfo info, Class<?> aClass) {
        Constructor<? extends FlowDescription> constructor = detectConstructor(aClass);
        List<JobflowAdapter.Parameter> parameters = analyzeParameters(constructor);
        JobflowAdapter adapter = new JobflowAdapter(info, constructor, parameters);
        LOG.debug("jobflow adapter: {}", adapter); //$NON-NLS-1$
        return adapter;
    }

    @SuppressWarnings("unchecked")
    private static Constructor<? extends FlowDescription> detectConstructor(Class<?> aClass) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        if (aClass.getEnclosingClass() != null) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "jobflow class must be top-level ({0})",
                    aClass.getName()));
        }
        if (Modifier.isPublic(aClass.getModifiers()) == false) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "jobflow class must be public ({0})",
                    aClass.getName()));
        }
        if (Modifier.isAbstract(aClass.getModifiers())) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "jobflow class must not be abstract ({0})",
                    aClass.getName()));
        }
        Constructor<?>[] candidates = aClass.getConstructors();
        if (candidates.length == 0) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "jobflow class must have at least one public constructor ({0})",
                    aClass.getName()));
        } else if (candidates.length != 1) {
            raise(diagnostics, aClass, MessageFormat.format(
                    "jobflow class must have just one public constructor ({0})",
                    aClass.getName()));
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        return (Constructor<? extends FlowDescription>) candidates[0];
    }

    static List<JobflowAdapter.Parameter> analyzeParameters(Constructor<? extends FlowDescription> constructor) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        List<ParameterCandidate> candidates = collectParameters(constructor);
        List<JobflowAdapter.Parameter> resolved = new ArrayList<>(candidates.size());
        for (ParameterCandidate candidate : candidates) {
            JobflowAdapter.Parameter p = resolveParameter(diagnostics, candidate);
            resolved.add(p);
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }

        validateParameters(diagnostics, constructor.getDeclaringClass(), resolved);
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        return resolved;
    }

    private static void validateParameters(
            List<Diagnostic> diagnostics,
            Class<?> aClass, List<JobflowAdapter.Parameter> parameters) {
        Set<String> inputs = new HashSet<>();
        Set<String> outputs = new HashSet<>();
        for (int i = 0, n = parameters.size(); i < n; i++) {
            JobflowAdapter.Parameter parameter = parameters.get(i);
            String name = parameter.getName();
            if (isValidIdentifier(name) == false) {
                raise(diagnostics, aClass, i, MessageFormat.format(
                        "jobflow {0} must have valid identifier \"{1}\"",
                        parameter.getDirection(),
                        name), null);
            }
            boolean conflict;
            if (parameter.getDirection() == JobflowAdapter.Direction.INPUT) {
                conflict = inputs.contains(name);
                inputs.add(name);
            } else {
                conflict = outputs.contains(name);
                outputs.add(name);
            }
            if (conflict) {
                raise(diagnostics, aClass, i, MessageFormat.format(
                        "duplicate jobflow {0} \"{1}\"",
                        parameter.getDirection(),
                        name), null);
            }
        }
        if (inputs.isEmpty()) {
            diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "jobflow must have at least one input ({0})",
                    aClass.getName())));
        }
        if (outputs.isEmpty()) {
            diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "jobflow must have at least one output ({0})",
                    aClass.getName())));
        }
    }

    private static List<ParameterCandidate> collectParameters(Constructor<? extends FlowDescription> constructor) {
        List<ParameterCandidate> results = new ArrayList<>();
        Type[] types = constructor.getGenericParameterTypes();
        Annotation[][] annotations = constructor.getParameterAnnotations();
        assert types.length == annotations.length;
        for (int i = 0; i < types.length; i++) {
            ParameterCandidate parameter = convert(constructor, i, types[i], annotations[i]);
            results.add(parameter);
        }
        return results;
    }

    private static ParameterCandidate convert(
            Constructor<? extends FlowDescription> constructor,
            int index, Type type, Annotation[] annotations) {
        TypeInfo typeInfo = TypeInfo.of(type);
        Import importInfo = null;
        Export exportInfo = null;
        for (Annotation annotation : annotations) {
            Class<? extends Annotation> annotationType = annotation.annotationType();
            if (annotationType == Import.class) {
                importInfo = (Import) annotation;
            } else if (annotationType == Export.class) {
                exportInfo = (Export) annotation;
            }
        }
        return new ParameterCandidate(constructor, index, typeInfo, importInfo, exportInfo);
    }

    private static JobflowAdapter.Parameter resolveParameter(
            List<Diagnostic> diagnostics, ParameterCandidate candidate) {
        Class<?> raw = candidate.type.getRawType();
        if (raw != In.class && raw != Out.class) {
            raise(diagnostics, candidate, MessageFormat.format(
                    "jobflow parameter must have type of either \"{0}\" or \"{1}\"",
                    In.class.getSimpleName(),
                    Out.class.getSimpleName()));
            return null;
        }
        List<Type> typeArguments = candidate.type.getTypeArguments();
        if (typeArguments.size() != 1 || (typeArguments.get(0) instanceof Class<?>) == false) {
            raise(diagnostics, candidate, MessageFormat.format(
                    "jobflow parameter must have valid data model type in its type argument \"{0}<...>\"",
                    raw.getSimpleName()));
            return null;
        }
        Class<?> dataType = TypeInfo.erase(typeArguments.get(0));
        if (raw == In.class) {
            return resolveInput(diagnostics, candidate, dataType);
        } else if (raw == Out.class) {
            return resolveOutput(diagnostics, candidate, dataType);
        } else {
            throw new AssertionError(raw);
        }
    }

    private static JobflowAdapter.Parameter resolveInput(
            List<Diagnostic> diagnostics, ParameterCandidate candidate, Class<?> dataType) {
        if (candidate.exportInfo != null) {
            raise(diagnostics, candidate, MessageFormat.format(
                    "jobflow input must not be annotated with @{0}",
                    Export.class.getSimpleName()));
        }
        Import info = candidate.importInfo;
        if (info == null) {
            raise(diagnostics, candidate, MessageFormat.format(
                    "jobflow input must be annotated with @{0}",
                    Import.class.getSimpleName()));
            return null;
        }
        Class<?> desc = info.description();
        return new JobflowAdapter.Parameter(JobflowAdapter.Direction.INPUT, info.name(), dataType, desc);
    }

    private static JobflowAdapter.Parameter resolveOutput(
            List<Diagnostic> diagnostics, ParameterCandidate candidate, Class<?> dataType) {
        if (candidate.importInfo != null) {
            raise(diagnostics, candidate, MessageFormat.format(
                    "jobflow output must not be annotated with @{0}",
                    Import.class.getSimpleName()));
        }
        Export info = candidate.exportInfo;
        if (info == null) {
            raise(diagnostics, candidate, MessageFormat.format(
                    "jobflow output must be annotated with @{0}",
                    Export.class.getSimpleName()));
            return null;
        }
        Class<?> desc = info.description();
        return new JobflowAdapter.Parameter(JobflowAdapter.Direction.OUTPUT, info.name(), dataType, desc);
    }

    private static void raise(List<Diagnostic> diagnostics, Class<?> atClass, String message) {
        String decorated = MessageFormat.format(
                "{0} ({1})", //$NON-NLS-1$
                message,
                atClass.getName());
        diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, decorated, null));
    }

    private static void raise(List<Diagnostic> diagnostics, ParameterCandidate at, String message) {
        raise(diagnostics, at, message, null);
    }

    private static void raise(List<Diagnostic> diagnostics, ParameterCandidate at, String message, Exception cause) {
        raise(diagnostics, at.constructor.getDeclaringClass(), at.index, message, cause);
    }

    private static void raise(
            List<Diagnostic> diagnostics, Class<?> atClass, int atIndex, String message, Exception cause) {
        String decorated = MessageFormat.format(
                "{0} ({1} at parameter #{2})",
                message,
                atClass.getName(),
                atIndex);
        diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, decorated, cause));
    }

    private static boolean isValidIdentifier(String id) {
        return PATTERN_ID.matcher(id).matches();
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "jobflow:{0}@{1}{2}", //$NON-NLS-1$
                info.getFlowId(),
                info.getDescriptionClass().getClassName(),
                parameters);
    }

    private static final class ParameterCandidate {

        final Constructor<? extends FlowDescription> constructor;

        final int index;

        final TypeInfo type;

        final Import importInfo;

        final Export exportInfo;

        ParameterCandidate(
                Constructor<? extends FlowDescription> constructor,
                int index, TypeInfo type,
                Import importInfo, Export exportInfo) {
            this.constructor = constructor;
            this.index = index;
            this.type = type;
            this.importInfo = importInfo;
            this.exportInfo = exportInfo;
        }
    }

    /**
     * Represents a jobflow parameter.
     */
    public static final class Parameter {

        private final Direction direction;

        private final String name;

        private final Class<?> dataModelClass;

        private final Class<?> descriptionClass;

        /**
         * Creates a new instance.
         * @param direction the input/output direction
         * @param name the parameter name
         * @param dataModelClass the data model class
         * @param descriptionClass external input/output description class
         */
        public Parameter(Direction direction, String name, Class<?> dataModelClass, Class<?> descriptionClass) {
            this.direction = direction;
            this.name = name;
            this.dataModelClass = dataModelClass;
            this.descriptionClass = descriptionClass;
        }

        /**
         * Returns the input/output direction.
         * @return the input/output direction
         */
        public Direction getDirection() {
            return direction;
        }

        /**
         * Returns the input/output name.
         * @return the input/output name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the data model class.
         * @return the data model class
         */
        public Class<?> getDataModelClass() {
            return dataModelClass;
        }

        /**
         * Returns the input/output description.
         * @return the input/output description
         */
        public Class<?> getDescriptionClass() {
            return descriptionClass;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "{0}(name={1}, type={2}, description={3})", //$NON-NLS-1$
                    direction.name(),
                    name,
                    dataModelClass.getSimpleName(),
                    descriptionClass.getSimpleName());
        }
    }

    /**
     * Represents input/output direction.
     */
    public enum Direction {

        /**
         * Input.
         */
        INPUT(In.class, "input"),

        /**
         * Output.
         */
        OUTPUT(Out.class, "output"),
        ;

        private final Class<?> rawClass;

        private final String description;

        Direction(Class<?> rawClass, String description) {
            this.rawClass = rawClass;
            this.description = description;
        }

        /**
         * Returns raw parameter type.
         * @return the raw parameter type
         */
        public Class<?> getRawClass() {
            return rawClass;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
