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
package com.asakusafw.lang.compiler.extension.testdriver;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.extension.externalio.AbstractExternalPortProcessor;
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

/**
 * An implementation of {@link ExternalPortProcessor} for internal I/O.
 */
public class InternalIoPortProcessor
        extends AbstractExternalPortProcessor<InternalImporterDescription, InternalExporterDescription> {

    private static final String MODULE_NAME = "internal"; //$NON-NLS-1$

    private static final Set<ClassDescription> BASIC_IMPLEMENTATIONS;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(Descriptions.classOf(InternalImporterDescription.Basic.class));
        set.add(Descriptions.classOf(InternalExporterDescription.Basic.class));
        BASIC_IMPLEMENTATIONS = set;
    }

    private static final Pattern PATTERN_PATH = Pattern.compile("[0-9A-Za-z]+"); //$NON-NLS-1$

    private static final String COMMON_SUFFIX = "-*"; //$NON-NLS-1$

    private static final TaskReference.Phase PHASE_OUTPUT = TaskReference.Phase.EPILOGUE;

    private static final Comparator<String> LOCATION_GROUP_COMPARATOR = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            int l1 = o1.length();
            int l2 = o2.length();
            if (l1 < l2) {
                return +1;
            } else if (l1 > l2) {
                return -1;
            }
            return o1.compareTo(o2);
        }
    };

    @Override
    protected String getModuleName() {
        return MODULE_NAME;
    }

    @Override
    protected Class<InternalImporterDescription> getInputDescriptionType() {
        return InternalImporterDescription.class;
    }

    @Override
    protected Class<InternalExporterDescription> getOutputDescriptionType() {
        return InternalExporterDescription.class;
    }

    @Override
    protected ValueDescription analyzeInputProperties(
            AnalyzeContext context, String name, InternalImporterDescription description) {
        return Descriptions.valueOf(description.getPathPrefix());
    }

    @Override
    protected ValueDescription analyzeOutputProperties(
            AnalyzeContext context, String name, InternalExporterDescription description) {
        return Descriptions.valueOf(description.getPathPrefix());
    }

    @Override
    protected Set<String> computeInputPaths(Context context, String name, ExternalInputInfo info) {
        String pathPrefix = extract(context, info);
        return Collections.singleton(pathPrefix);
    }

    @Override
    public void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        diagnostics.addAll(validatePaths(context, inputs.values()));
        diagnostics.addAll(validatePaths(context, outputs.values()));
        diagnostics.addAll(validateOutputs(context, outputs));
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
    }

    private List<Diagnostic> validatePaths(AnalyzeContext context, Collection<? extends ExternalPortInfo> ports) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (ExternalPortInfo port : ports) {
            String path = extract(context, port);
            String label;
            if (BASIC_IMPLEMENTATIONS.contains(port.getDescriptionClass())) {
                label = port.getDataModelClass().getClassName();
            } else {
                label = port.getDescriptionClass().getClassName();
            }
            diagnostics.addAll(validatePath(path, label));
        }
        return diagnostics;
    }

    private static List<Diagnostic> validatePath(String path, String label) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        if (path.indexOf('/') < 0) {
            diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "path prefix must have at least one \"/\": {0} ({1})",
                    path,
                    label)));
        }
        if (path.endsWith(COMMON_SUFFIX)) {
            String outputName = getOutputName(path);
            if (PATTERN_PATH.matcher(outputName).matches() == false) {
                diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                        "file name must consist of alphabets and digits: {0} ({1})",
                        path,
                        label)));
            }
        } else {
            diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "path prefix must end with \"-*\": {0} ({1})",
                    path,
                    label)));
        }
        return diagnostics;
    }

    private List<Diagnostic> validateOutputs(AnalyzeContext context, Map<String, ExternalOutputInfo> outputs) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        Map<String, ClassDescription> outputLocations = new HashMap<>();
        for (Map.Entry<String, ? extends ExternalPortInfo> entry : outputs.entrySet()) {
            String path = extract(context, entry.getValue());
            ClassDescription description = entry.getValue().getDescriptionClass();
            if (outputLocations.containsKey(path)) {
                diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                        "output path prefix must be unique: {0} ({1} <> {2})",
                        path,
                        description.getClassName(),
                        outputLocations.get(path).getClassName())));
            } else {
                outputLocations.put(path, description);
            }
        }
        return diagnostics;
    }

    @Override
    public void process(
            Context context,
            List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs) throws IOException {
        processOutput(context, outputs);
    }

    private void processOutput(Context context, List<ExternalOutputReference> outputs) throws IOException {
        JavaSourceExtension javac = getJavaCompiler(context);
        Map<String, List<ExternalOutputReference>> groups = groupByPath(context, outputs);
        int index = 0;
        for (Map.Entry<String, List<ExternalOutputReference>> entry : groups.entrySet()) {
            int serial = index++;
            List<CopyStageInfo.Operation> operations = new ArrayList<>();
            for (ExternalOutputReference port : entry.getValue()) {
                String path = extract(context, port);
                String outputName = getOutputName(path);
                CopyStageInfo.Operation operation = new CopyStageInfo.Operation(
                        outputName,
                        new SourceInfo(
                                port.getPaths(),
                                port.getDataModelClass(),
                                HadoopFormatExtension.getInputFormat(context),
                                Collections.<String, String>emptyMap()),
                        HadoopFormatExtension.getOutputFormat(context),
                        Collections.<String, String>emptyMap());
                operations.add(operation);
            }
            CopyStageInfo info = new CopyStageInfo(
                    new StageInfo(
                            context.getBatchId(),
                            context.getFlowId(),
                            String.format("%s%d", getModuleName(), serial)), //$NON-NLS-1$
                    operations,
                    entry.getKey());
            ClassDescription client = getOutputClientClass(serial);
            CopyStageEmitter.emit(client, info, javac);
            context.addTask(PHASE_OUTPUT, new HadoopTaskReference(
                    getModuleName(),
                    client,
                    Collections.<TaskReference>emptyList()));
        }
    }

    private ClassDescription getOutputClientClass(int serial) {
        return Naming.getClass(getModuleName(), PHASE_OUTPUT, "Client", serial); //$NON-NLS-1$
    }

    private JavaSourceExtension getJavaCompiler(Context context) {
        JavaSourceExtension extension = context.getExtension(JavaSourceExtension.class);
        if (extension == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, "Java compiler must be supported");
        }
        return extension;
    }

    private Map<String, List<ExternalOutputReference>> groupByPath(
            Context context, List<ExternalOutputReference> ports) {
        Map<String, List<ExternalOutputReference>> results = new TreeMap<>(LOCATION_GROUP_COMPARATOR);
        for (ExternalOutputReference port : ports) {
            String prefix = extract(context, port);
            String basePath = getBasePath(prefix);
            List<ExternalOutputReference> list = results.get(basePath);
            if (list == null) {
                list = new ArrayList<>();
                results.put(basePath, list);
            }
            list.add(port);
        }
        return results;
    }

    private static String getBasePath(String prefix) {
        int index = prefix.lastIndexOf('/');
        assert index >= 0;
        String group = prefix.substring(0, index);
        return group;
    }

    private static String getOutputName(String prefix) {
        assert prefix.endsWith(COMMON_SUFFIX);
        int index = prefix.lastIndexOf('/');
        String group = prefix.substring(index + 1, prefix.length() - COMMON_SUFFIX.length());
        return group;
    }

    private String extract(AnalyzeContext context, ExternalPortInfo info) {
        try {
            Object contents = info.getContents().resolve(context.getClassLoader());
            assert contents instanceof String;
            return (String) contents;
        } catch (ReflectiveOperationException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to extract the path: {0}",
                    info.getDescriptionClass().getClassName()));
        }
    }
}
