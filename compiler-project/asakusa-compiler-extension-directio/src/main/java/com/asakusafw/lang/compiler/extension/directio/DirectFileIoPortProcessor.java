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
package com.asakusafw.lang.compiler.extension.directio;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.extension.directio.emitter.OutputStageEmitter;
import com.asakusafw.lang.compiler.extension.directio.emitter.OutputStageInfo;
import com.asakusafw.lang.compiler.extension.externalio.AbstractExternalPortProcessor;
import com.asakusafw.lang.compiler.extension.externalio.ExternalPortStageInfo;
import com.asakusafw.lang.compiler.extension.externalio.Naming;
import com.asakusafw.lang.compiler.hadoop.HadoopFormatExtension;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.mapreduce.CopyStageEmitter;
import com.asakusafw.lang.compiler.mapreduce.CopyStageInfo;
import com.asakusafw.lang.compiler.mapreduce.SourceInfo;
import com.asakusafw.lang.compiler.mapreduce.StageInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.DirectDataSourceConstants;
import com.asakusafw.runtime.directio.FilePattern;
import com.asakusafw.runtime.directio.FilePattern.PatternElementKind;
import com.asakusafw.vocabulary.directio.DirectFileInputDescription;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;

/**
 * An implementation of {@link ExternalPortProcessor} for Direct file I/O.
 */
public class DirectFileIoPortProcessor
        extends AbstractExternalPortProcessor<DirectFileInputDescription, DirectFileOutputDescription> {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileIoPortProcessor.class);

    private static final String MODULE_NAME = "directio"; //$NON-NLS-1$

    private static final TaskReference.Phase PHASE_INPUT = TaskReference.Phase.PROLOGUE;

    private static final TaskReference.Phase PHASE_OUTPUT = TaskReference.Phase.EPILOGUE;

    private static final Set<PatternElementKind> INVALID_BASE_PATH_KIND =
            EnumSet.of(PatternElementKind.WILDCARD, PatternElementKind.SELECTION);

    private static final String PATTERN_DUMMY_INPUT = "directio:%s/%s"; //$NON-NLS-1$

    @Override
    protected String getModuleName() {
        return MODULE_NAME;
    }

    @Override
    protected Class<DirectFileInputDescription> getInputDescriptionType() {
        return DirectFileInputDescription.class;
    }

    @Override
    protected Class<DirectFileOutputDescription> getOutputDescriptionType() {
        return DirectFileOutputDescription.class;
    }

    @Override
    protected ValueDescription analyzeInputProperties(
            AnalyzeContext context,
            String name,
            DirectFileInputDescription description) {
        DirectFileInputModel model = analyzeDescription(context, description);
        return Descriptions.valueOf(model);
    }

    @Override
    protected ValueDescription analyzeOutputProperties(
            AnalyzeContext context,
            String name,
            DirectFileOutputDescription description) {
        DirectFileOutputModel model = analyzeDescription(context, description);
        return Descriptions.valueOf(model);
    }

    private DirectFileInputModel analyzeDescription(AnalyzeContext context, DirectFileInputDescription description) {
        ValidateContext validate = new ValidateContext(context, Descriptions.classOf(description.getClass()));
        assertPresent(validate, description.getBasePath(), "getBasePath"); //$NON-NLS-1$
        assertPresent(validate, description.getResourcePattern(), "getResourcePattern"); //$NON-NLS-1$
        assertPresent(validate, description.getFormat(), "getFormat"); //$NON-NLS-1$
        validate.raiseException();
        return new DirectFileInputModel(description);
    }

    private DirectFileOutputModel analyzeDescription(AnalyzeContext context, DirectFileOutputDescription description) {
        ValidateContext validate = new ValidateContext(context, Descriptions.classOf(description.getClass()));
        assertPresent(validate, description.getBasePath(), "getBasePath"); //$NON-NLS-1$
        assertPresent(validate, description.getResourcePattern(), "getResourcePattern"); //$NON-NLS-1$
        assertPresent(validate, description.getFormat(), "getFormat"); //$NON-NLS-1$
        assertPresent(validate, description.getOrder(), "getOrder"); //$NON-NLS-1$
        assertPresent(validate, description.getDeletePatterns(), "getDeletePatterns"); //$NON-NLS-1$
        validate.raiseException();
        return new DirectFileOutputModel(description);
    }

    private void assertPresent(ValidateContext context, Object value, String method) {
        if (value != null) {
            return;
        }
        context.error(MessageFormat.format(
                "{0}.{1}() must be set",
                context.target.getClassName(),
                method));
    }

    @Override
    public void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        List<ResolvedInput> resolvedInputs = new ArrayList<>();
        List<ResolvedOutput> resolvedOutputs = new ArrayList<>();
        for (Map.Entry<String, ExternalInputInfo> entry : inputs.entrySet()) {
            try {
                resolvedInputs.add(restoreModel(context, entry.getKey(), entry.getValue()));
            } catch (DiagnosticException e) {
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        for (Map.Entry<String, ExternalOutputInfo> entry : outputs.entrySet()) {
            try {
                resolvedOutputs.add(restoreModel(context, entry.getKey(), entry.getValue()));
            } catch (DiagnosticException e) {
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
        checkConflict(context, resolvedInputs, resolvedOutputs);
    }

    private String normalizePath(String path) {
        assert path != null;
        return Location.of(path).toPath() + '/';
    }

    private ResolvedInput restoreModel(AnalyzeContext context, String name, ExternalInputInfo info) {
        DirectFileInputModel model = resolveContents(context, info, DirectFileInputModel.class);
        ValidateContext validate = new ValidateContext(context, info.getDescriptionClass());
        analyzeModel(validate, model, info.getDataModelClass());
        validate.raiseException();
        return new ResolvedInput(name, info, model);
    }

    private ResolvedOutput restoreModel(AnalyzeContext context, String name, ExternalOutputInfo info) {
        DirectFileOutputModel model = resolveContents(context, info, DirectFileOutputModel.class);
        ValidateContext validate = new ValidateContext(context, info.getDescriptionClass());
        analyzeModel(validate, model, info.getDataModelClass());
        validate.raiseException();
        return new ResolvedOutput(name, info, model);
    }

    private <T> T resolveContents(AnalyzeContext context, ExternalPortInfo info, Class<T> type) {
        ValueDescription contents = info.getContents();
        if (contents == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to restore external port detail: {0}",
                    info));
        }
        try {
            Object resolved = contents.resolve(context.getClassLoader());
            return type.cast(resolved);
        } catch (ReflectiveOperationException | ClassCastException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to restore external port detail: {0}",
                    info), e);
        }
    }

    private void analyzeModel(ValidateContext context, DirectFileInputModel model, ClassDescription dataType) {
        checkBasePath(context, model.getBasePath());
        checkInputPattern(context, model.getResourcePattern());
        checkFormatClass(context, model.getFormatClass(), dataType);
    }

    private void analyzeModel(ValidateContext context, DirectFileOutputModel model, ClassDescription dataType) {
        checkBasePath(context, model.getBasePath());
        checkOutput(context, model, dataType);
        checkDeletePatterns(context, model.getDeletePatterns());
        checkFormatClass(context, model.getFormatClass(), dataType);
    }

    private void checkBasePath(ValidateContext context, String value) {
        try {
            FilePattern pattern = FilePattern.compile(value);
            if (pattern.containsTraverse()) {
                context.error(MessageFormat.format(
                        "base path must not contain wildcards (**) ({0}.getBasePath())",
                        context.target.getClassName()));
            }
            Set<PatternElementKind> kinds = pattern.getPatternElementKinds();
            for (PatternElementKind kind : kinds) {
                if (INVALID_BASE_PATH_KIND.contains(kind)) {
                    context.error(MessageFormat.format(
                            "base path must not contain \"{1}\" ({0}.getBasePath())",
                            context.target.getClassName(),
                            kind.getSymbol()));
                }
            }
        } catch (IllegalArgumentException e) {
            context.error(MessageFormat.format(
                    "failed to compile base path ({0}.getBasePath()): {1}",
                    context.target.getClassName(),
                    e.getMessage()));
        }
    }

    private void checkInputPattern(ValidateContext context, String value) {
        try {
            FilePattern.compile(value);
        } catch (IllegalArgumentException e) {
            context.error(MessageFormat.format(
                    "failed to compile input resource pattern ({0}.getResourcePattern()): {1}",
                    context.target.getClassName(),
                    e.getMessage()));
        }
    }

    private void checkOutput(ValidateContext context, DirectFileOutputModel model, ClassDescription dataType) {
        DataModelReference dataModel = context.context.getDataModelLoader().load(dataType);
        List<OutputPattern.CompiledSegment> resourcePattern;
        try {
            resourcePattern = OutputPattern.compileResourcePattern(model.getResourcePattern(), dataModel);
        } catch (IllegalArgumentException e) {
            context.error(MessageFormat.format(
                    "failed to compile output resource pattern ({0}.getResourcePattern()): {1}",
                    context.target.getClassName(),
                    e.getMessage()));
            resourcePattern = null;
        }
        List<OutputPattern.CompiledOrder> orders;
        try {
            orders = OutputPattern.compileOrder(model.getOrder(), dataModel);
        } catch (IllegalArgumentException e) {
            context.error(MessageFormat.format(
                    "failed to compile record order ({0}.getOrder()): {1}",
                    context.target.getClassName(),
                    e.getMessage()));
            orders = null;
        }
        if (resourcePattern == null || orders == null) {
            return;
        }
        if (OutputPattern.isContextRequired(resourcePattern)) {
            if (OutputPattern.isGatherRequired(resourcePattern)) {
                context.error(MessageFormat.format(
                        "output resource pattern with wildcards (*) "
                        + "must not contain any properties ('{'name'}') nor random numbers ([m..n]) "
                        + "({0}.getResourcePattern()): {1}",
                        context.target.getClassName()));
            }
            if (orders.isEmpty() == false) {
                context.error(MessageFormat.format(
                        "output resource pattern with wildcards (*) must not contain any orders "
                        + "({0}.getOrder()): {1}",
                        context.target.getClassName()));
            }
        }
    }

    private void checkDeletePatterns(ValidateContext context, List<String> values) {
        for (int index = 0, n = values.size(); index < n; index++) {
            String value = values.get(index);
            try {
                FilePattern.compile(value);
            } catch (IllegalArgumentException e) {
                context.error(MessageFormat.format(
                        "failed to compile delete resource pattern ({0}.getDeletePatterns()@1): {2}",
                        context.target.getClassName(),
                        index,
                        e.getMessage()));
            }
        }
    }

    private void checkFormatClass(ValidateContext context, ClassDescription formatClass, ClassDescription dataType) {
        Class<?> resolvedDataType;
        try {
            resolvedDataType = dataType.resolve(context.context.getClassLoader());
        } catch (ReflectiveOperationException e) {
            context.error(MessageFormat.format(
                    "failed to resolve data model class: {0}",
                    dataType.getClassName()), e);
            return;
        }
        DataFormat<?> formatObject;
        try {
            Class<?> resolvedFormatClass = formatClass.resolve(context.context.getClassLoader());
            if (DataFormat.class.isAssignableFrom(resolvedFormatClass) == false) {
                throw new ReflectiveOperationException(MessageFormat.format(
                        "data format must be a subtype of {0}: {1}",
                        DataFormat.class.getName(),
                        formatClass.getClassName()));
            }
            formatObject = (DataFormat<?>) resolvedFormatClass.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            context.error(MessageFormat.format(
                    "failed to resolve data format ({0}.getFormatClass()): {1}",
                    context.target.getClassName(),
                    formatClass.getClassName()), e);
            return;
        }
        if (formatObject.getSupportedType().isAssignableFrom(resolvedDataType) == false) {
            context.error(MessageFormat.format(
                    "data format must support \"{2}\" ({0}.getFormatClass()): {1}",
                    context.target.getClassName(),
                    formatClass.getClassName(),
                    dataType.getClassName()));
        }
    }

    private void checkConflict(AnalyzeContext context, List<ResolvedInput> inputs, List<ResolvedOutput> outputs) {
        assert inputs != null;
        assert outputs != null;
        ValidateContext c = new ValidateContext(context, null);
        NavigableMap<String, ResolvedInput> inputPaths = new TreeMap<>();
        for (ResolvedInput input : inputs) {
            String path = normalizePath(input.model.getBasePath());
            inputPaths.put(path, input);
        }
        NavigableMap<String, ResolvedOutput> outputPaths = new TreeMap<>();
        for (ResolvedOutput self : outputs) {
            String path = normalizePath(self.model.getBasePath());
            for (Map.Entry<String, ResolvedInput> entry : inputPaths.tailMap(path, true).entrySet()) {
                if (entry.getKey().startsWith(path) == false) {
                    break;
                }
                ResolvedInput other = entry.getValue();
                c.error(MessageFormat.format(
                        "conflict input/output base paths: {0}[{1}] -> {2}[{3}]",
                        self.info.getDescriptionClass().getClassName(),
                        self.model.getBasePath(),
                        other.info.getDescriptionClass().getClassName(),
                        other.model.getBasePath()));
            }
            if (outputPaths.containsKey(path)) {
                ResolvedOutput other = outputPaths.get(path);
                c.error(MessageFormat.format(
                        "conflict output/output base paths: {0}[{1}] <-> {2}[{3}]",
                        self.info.getDescriptionClass().getClassName(),
                        self.model.getBasePath(),
                        other.info.getDescriptionClass().getClassName(),
                        other.model.getBasePath()));
            } else {
                outputPaths.put(path, self);
            }
        }
        for (Map.Entry<String, ResolvedOutput> base : outputPaths.entrySet()) {
            String path = base.getKey();
            for (Map.Entry<String, ResolvedOutput> entry : outputPaths.tailMap(path, false).entrySet()) {
                if (entry.getKey().startsWith(path) == false) {
                    break;
                }
                ResolvedOutput self = base.getValue();
                ResolvedOutput other = entry.getValue();
                c.error(MessageFormat.format(
                        "conflict input/output base paths: {0}[{1}] -> {2}[{3}]",
                        self.info.getDescriptionClass().getClassName(),
                        self.model.getBasePath(),
                        other.info.getDescriptionClass().getClassName(),
                        other.model.getBasePath()));
            }
        }
        c.raiseException();
    }

    @Override
    protected Set<String> computeInputPaths(Context context, String name, ExternalInputInfo info) {
        String base = getTemporaryPath(context, PHASE_INPUT, null);
        String path = CopyStageInfo.getOutputPath(base, name);
        return Collections.singleton(path);
    }

    @Override
    public void process(
            Context context,
            List<ExternalInputReference> inputs,
            List<ExternalOutputReference> outputs) throws IOException {
        LOG.debug("processing Direct file I/O ports: {}/{}", context.getBatchId(), context.getFlowId()); //$NON-NLS-1$
        processInputs(context, inputs);
        processOutputs(context, outputs);
    }

    private void processInputs(Context context, List<ExternalInputReference> ports) throws IOException {
        if (ports.isEmpty()) {
            return;
        }
        List<CopyStageInfo.Operation> operations = new ArrayList<>();
        for (ExternalInputReference port : ports) {
            LOG.debug("processing Direct file input: {}={}", port.getName(), port.getDescriptionClass()); //$NON-NLS-1$
            CopyStageInfo.Operation operation = processInput(context, port);
            operations.add(operation);
        }
        String stageId = Naming.getStageId(getModuleName(), PHASE_INPUT);
        CopyStageInfo info = new CopyStageInfo(
                new StageInfo(context.getBatchId(), context.getFlowId(), stageId),
                operations,
                getTemporaryPath(context, PHASE_INPUT, null));
        ClassDescription clientClass = Naming.getClass(getModuleName(), PHASE_INPUT, "StageClient"); //$NON-NLS-1$
        CopyStageEmitter.emit(clientClass, info, getJavaCompiler(context));
        registerJob(context, PHASE_INPUT, clientClass);
    }

    private void processOutputs(Context context, List<ExternalOutputReference> ports) throws IOException {
        if (ports.isEmpty()) {
            return;
        }
        List<OutputStageInfo.Operation> operations = new ArrayList<>();
        for (ExternalOutputReference port : ports) {
            LOG.debug("processing Direct file output: {}={}", port.getName(), port.getDescriptionClass()); //$NON-NLS-1$
            OutputStageInfo.Operation operation = processOutput(context, port);
            operations.add(operation);
        }
        OutputStageInfo info = new OutputStageInfo(
                new ExternalPortStageInfo(getModuleName(), context.getBatchId(), context.getFlowId(), PHASE_OUTPUT),
                operations,
                getTemporaryPath(context, PHASE_OUTPUT, null));
        ClassDescription clientClass = OutputStageEmitter.emit(info, getJavaCompiler(context));
        registerJob(context, PHASE_OUTPUT, clientClass);
    }

    private JavaSourceExtension getJavaCompiler(Context context) {
        JavaSourceExtension extension = context.getExtension(JavaSourceExtension.class);
        if (extension == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, "Java compiler must be supported");
        }
        return extension;
    }

    private CopyStageInfo.Operation processInput(Context context, ExternalInputReference reference) {
        ResolvedInput resolved = restoreModel(context, reference.getName(), reference);
        String dummyInputPath = String.format(PATTERN_DUMMY_INPUT, reference.getName(), resolved.model.getBasePath());
        Map<String, String> inputAttributes = getInputAttributes(resolved);
        return new CopyStageInfo.Operation(
                reference.getName(),
                new SourceInfo(
                        dummyInputPath,
                        reference.getDataModelClass(),
                        DirectFileIoConstants.CLASS_INPUT_FORMAT,
                        inputAttributes),
                HadoopFormatExtension.getOutputFormat(context),
                Collections.<String, String>emptyMap());
    }

    private Map<String, String> getInputAttributes(ResolvedInput resolved) {
        Map<String, String> results = new LinkedHashMap<>();
        results.put(DirectDataSourceConstants.KEY_DATA_CLASS, resolved.info.getDataModelClass().getBinaryName());
        results.put(DirectDataSourceConstants.KEY_FORMAT_CLASS, resolved.model.getFormatClass().getBinaryName());
        results.put(DirectDataSourceConstants.KEY_BASE_PATH, resolved.model.getBasePath());
        results.put(DirectDataSourceConstants.KEY_RESOURCE_PATH, resolved.model.getResourcePattern());
        results.put(DirectDataSourceConstants.KEY_OPTIONAL, String.valueOf(resolved.model.isOptional()));
        return results;
    }

    private OutputStageInfo.Operation processOutput(Context context, ExternalOutputReference reference) {
        DirectFileOutputModel model = restoreModel(context, reference.getName(), reference).model;
        DataModelReference dataModel = context.getDataModelLoader().load(reference.getDataModelClass());
        List<FilePattern> deletePatterns = new ArrayList<>();
        for (String pattern : model.getDeletePatterns()) {
            deletePatterns.add(FilePattern.compile(pattern));
        }
        return new OutputStageInfo.Operation(
                reference.getName(),
                dataModel,
                Collections.singletonList(new SourceInfo(
                        reference.getPaths(),
                        reference.getDataModelClass(),
                        HadoopFormatExtension.getInputFormat(context),
                        Collections.<String, String>emptyMap())),
                model.getBasePath(),
                OutputPattern.compile(dataModel, model.getResourcePattern(), model.getOrder()),
                deletePatterns,
                model.getFormatClass());
    }

    private void registerJob(Context context, Phase phase, ClassDescription clientClass) {
        LOG.debug("registering Direct file I/O job: {}->{}", phase, clientClass); //$NON-NLS-1$
        context.addTask(phase, new HadoopTaskReference(
                getModuleName(),
                clientClass,
                Collections.<TaskReference>emptyList()));
    }

    private static class ValidateContext {

        final AnalyzeContext context;

        final ClassDescription target;

        final List<Diagnostic> diagnostics = new ArrayList<>();

        ValidateContext(AnalyzeContext context, ClassDescription target) {
            this.context = context;
            this.target = target;
        }

        void error(String message) {
            error(message, null);
        }

        void error(String message, Exception cause) {
            diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, message, cause));
        }

        void raiseException() {
            if (diagnostics.isEmpty() == false) {
                throw new DiagnosticException(diagnostics);
            }
        }
    }

    private static class ResolvedInput {

        final ExternalInputInfo info;

        final DirectFileInputModel model;

        ResolvedInput(String name, ExternalInputInfo info, DirectFileInputModel model) {
            this.info = info;
            this.model = model;
        }
    }

    private static class ResolvedOutput {

        final ExternalOutputInfo info;

        final DirectFileOutputModel model;

        ResolvedOutput(String name, ExternalOutputInfo info, DirectFileOutputModel model) {
            this.info = info;
            this.model = model;
        }
    }
}
