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
package com.asakusafw.lang.compiler.extension.windgate;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.util.StringUtil;
import com.asakusafw.lang.compiler.extension.externalio.AbstractExternalPortProcessor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;
import com.asakusafw.vocabulary.windgate.Constants;
import com.asakusafw.vocabulary.windgate.WindGateExporterDescription;
import com.asakusafw.vocabulary.windgate.WindGateImporterDescription;
import com.asakusafw.vocabulary.windgate.WindGateProcessDescription;
import com.asakusafw.windgate.core.DriverScript;
import com.asakusafw.windgate.core.GateScript;
import com.asakusafw.windgate.core.ProcessScript;
import com.asakusafw.windgate.core.vocabulary.FileProcess;

/**
 * An implementation of {@link ExternalPortProcessor} for WindGate.
 * @since 0.1.0
 * @version 0.4.0
 */
public class WindGatePortProcessor
        extends AbstractExternalPortProcessor<WindGateImporterDescription, WindGateExporterDescription> {

    static final Logger LOG = LoggerFactory.getLogger(WindGatePortProcessor.class);

    static final Location CMD_PROCESS = Location.of("windgate/bin/process.sh"); //$NON-NLS-1$

    static final Location CMD_FINALIZE = Location.of("windgate/bin/finalize.sh"); //$NON-NLS-1$

    static final String OPT_IMPORT = "import"; //$NON-NLS-1$

    static final String OPT_EXPORT = "export"; //$NON-NLS-1$

    static final String PATTERN_SCRIPT_LOCATION = "META-INF/windgate/{0}-{1}.properties"; //$NON-NLS-1$

    static final String OPT_BEGIN = "begin"; //$NON-NLS-1$

    static final String OPT_END = "end"; //$NON-NLS-1$

    static final String OPT_ONESHOT = "oneshot"; //$NON-NLS-1$

    static final String KEY_MODEL = "model"; //$NON-NLS-1$

    private static final ClassDescription MODEL_CLASS = Descriptions.classOf(DescriptionModel.class);

    @Override
    protected String getModuleName() {
        return WindGateConstants.MODULE_NAME;
    }

    @Override
    protected Class<WindGateImporterDescription> getInputDescriptionType() {
        return WindGateImporterDescription.class;
    }

    @Override
    protected Class<WindGateExporterDescription> getOutputDescriptionType() {
        return WindGateExporterDescription.class;
    }

    @Override
    protected ValueDescription analyzeInputProperties(
            AnalyzeContext context, String name, WindGateImporterDescription description) {
        try {
            return extract(description);
        } catch (IllegalStateException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "importer description \"{0}\" is not valid: {1}",
                    name,
                    e.getMessage()));
        }
    }

    @Override
    protected ValueDescription analyzeOutputProperties(
            AnalyzeContext context, String name, WindGateExporterDescription description) {
        try {
            return extract(description);
        } catch (IllegalStateException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "exporter description \"{0}\" is not valid: {1}",
                    name,
                    e.getMessage()));
        }
    }

    @Override
    protected Set<OutputAttribute> analyzeOutputAttributes(
            AnalyzeContext context, String name, WindGateExporterDescription description) {
        // WindGate outputs must be GENERATOR for truncating target outputs
        return EnumSet.of(OutputAttribute.GENERATOR);
    }

    @Override
    protected Set<String> analyzeInputParameterNames(
            AnalyzeContext context, String name, WindGateImporterDescription description) {
        try {
            return new DescriptionModel(description).getDriverScript().getParameterNames();
        } catch (IllegalStateException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "importer description \"{0}\" is not valid: {1}",
                    name,
                    e.getMessage()));
        }
    }

    @Override
    protected Set<String> analyzeOutputParameterNames(
            AnalyzeContext context, String name, WindGateExporterDescription description) {
        try {
            return new DescriptionModel(description).getDriverScript().getParameterNames();
        } catch (IllegalStateException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "exporter description \"{0}\" is not valid: {1}",
                    name,
                    e.getMessage()));
        }
    }

    @Override
    protected Set<String> computeInputPaths(Context context, String name, ExternalInputInfo info) {
        String path = getTemporaryPath(context, Phase.IMPORT, Location.of(name));
        return Collections.singleton(path);
    }

    @Override
    public void process(
            Context context,
            List<ExternalInputReference> inputs,
            List<ExternalOutputReference> outputs) throws IOException {
        LOG.debug("processing WindGate ports: {}/{}", context.getBatchId(), context.getFlowId()); //$NON-NLS-1$
        Map<String, GateScript> importers = toImporterScripts(context, inputs);
        Map<String, GateScript> exporters = toExporterScripts(context, outputs);
        for (Map.Entry<String, GateScript> entry : importers.entrySet()) {
            Location location = getImportScriptLocation(entry.getKey());
            emitScript(context, location, entry.getValue());
        }
        for (Map.Entry<String, GateScript> entry : exporters.entrySet()) {
            Location location = getExportScriptLocation(entry.getKey());
            emitScript(context, location, entry.getValue());
        }
        Set<String> profiles = new TreeSet<>();
        profiles.addAll(importers.keySet());
        profiles.addAll(exporters.keySet());
        for (String profileName : profiles) {
            boolean doImport = importers.containsKey(profileName);
            boolean doExport = exporters.containsKey(profileName);
            registerTasks(context, profileName, doImport, doExport);
        }
    }

    private void registerTasks(Context context, String profileName, boolean doImport, boolean doExport) {
        boolean doBoth = doImport & doExport;
        if (doImport) {
            LOG.debug("registering WindGate import: {}", profileName); //$NON-NLS-1$
            context.addTask(TaskReference.Phase.IMPORT, new CommandTaskReference(
                    getModuleName(),
                    profileName,
                    CMD_PROCESS,
                    Arrays.asList(new CommandToken[] {
                            CommandToken.of(profileName),
                            CommandToken.of(doBoth ? OPT_BEGIN : OPT_ONESHOT),
                            CommandToken.of(getScriptUri(true, profileName)),
                            CommandToken.BATCH_ID,
                            CommandToken.FLOW_ID,
                            CommandToken.EXECUTION_ID,
                            CommandToken.BATCH_ARGUMENTS,
                    }),
                    Collections.emptySet(),
                    Collections.emptyList()));
        }
        if (doExport) {
            LOG.debug("registering WindGate export: {}", profileName); //$NON-NLS-1$
            context.addTask(TaskReference.Phase.EXPORT, new CommandTaskReference(
                    getModuleName(),
                    profileName,
                    CMD_PROCESS,
                    Arrays.asList(new CommandToken[] {
                            CommandToken.of(profileName),
                            CommandToken.of(doBoth ? OPT_END : OPT_ONESHOT),
                            CommandToken.of(getScriptUri(false, profileName)),
                            CommandToken.BATCH_ID,
                            CommandToken.FLOW_ID,
                            CommandToken.EXECUTION_ID,
                            CommandToken.BATCH_ARGUMENTS,
                    }),
                    Collections.emptySet(),
                    Collections.emptyList()));
        }
        LOG.debug("registering WindGate finalize: {}", profileName); //$NON-NLS-1$
        context.addTask(TaskReference.Phase.FINALIZE, new CommandTaskReference(
                getModuleName(),
                profileName,
                CMD_FINALIZE,
                Arrays.asList(new CommandToken[] {
                        CommandToken.of(profileName),
                        CommandToken.BATCH_ID,
                        CommandToken.FLOW_ID,
                        CommandToken.EXECUTION_ID,
                }),
                Collections.emptySet(),
                Collections.emptyList()));
    }

    private static String getScriptUri(boolean importer, String profileName) {
        Location location = getScriptLocation(importer, profileName);
        return String.format("classpath:%s", location.toPath()); //$NON-NLS-1$
    }

    private Map<String, GateScript> toImporterScripts(Context context, List<ExternalInputReference> ports) {
        Map<String, List<ProcessScript<?>>> processes = new LinkedHashMap<>();
        for (ExternalInputReference port : ports) {
            LOG.debug("processing WindGate input: {}={}", port.getName(), port.getDescriptionClass()); //$NON-NLS-1$
            DescriptionModel model = restore(port);
            String profileName = model.getProfileName();
            ProcessScript<?> process = toProcessScript(context, port, model);
            List<ProcessScript<?>> list = processes.get(profileName);
            if (list == null) {
                list = new ArrayList<>();
                processes.put(profileName, list);
            }
            list.add(process);
        }
        return toGateScripts(processes);
    }

    private Map<String, GateScript> toExporterScripts(Context context, List<ExternalOutputReference> ports) {
        Map<String, List<ProcessScript<?>>> processes = new LinkedHashMap<>();
        for (ExternalOutputReference port : ports) {
            LOG.debug("processing WindGate output: {}={}", port.getName(), port.getDescriptionClass()); //$NON-NLS-1$
            DescriptionModel model = restore(port);
            String profileName = model.getProfileName();
            ProcessScript<?> process = toProcessScript(context, port, model);
            List<ProcessScript<?>> list = processes.get(profileName);
            if (list == null) {
                list = new ArrayList<>();
                processes.put(profileName, list);
            }
            list.add(process);
        }
        return toGateScripts(processes);
    }

    private ProcessScript<?> toProcessScript(
            Context context, ExternalInputReference reference, DescriptionModel model) {
        Set<String> paths = reference.getPaths();
        if (paths.size() != 1) {
            throw new IllegalStateException(MessageFormat.format(
                    "WindGate importer must have only one input path: {0}",
                    reference));
        }
        Class<?> dataModelType;
        try {
            dataModelType = reference.getDataModelClass().resolve(context.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to resolve data model type: {0}",
                    reference.getDataModelClass()), e);
        }
        String location = paths.iterator().next();
        DriverScript source = model.getDriverScript();
        DriverScript drain = new DriverScript(
                Constants.HADOOP_FILE_RESOURCE_NAME,
                Collections.singletonMap(FileProcess.FILE.key(), location));
        return createProcessScript(reference.getName(), dataModelType, source, drain);
    }

    private ProcessScript<?> toProcessScript(
            Context context, ExternalOutputReference reference, DescriptionModel model) {
        String locations = StringUtil.join('\n', reference.getPaths());
        Class<?> dataModelType;
        try {
            dataModelType = reference.getDataModelClass().resolve(context.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to resolve data model type: {0}",
                    reference.getDataModelClass()), e);
        }
        DriverScript source;
        if (locations.isEmpty()) {
            source = new DriverScript(
                    Constants.VOID_RESOURCE_NAME,
                    Collections.emptyMap());
        } else {
            source = new DriverScript(
                    Constants.HADOOP_FILE_RESOURCE_NAME,
                    Collections.singletonMap(FileProcess.FILE.key(), locations));
        }
        DriverScript drain = model.getDriverScript();
        return createProcessScript(reference.getName(), dataModelType, source, drain);
    }

    static Location getImportScriptLocation(String profileName) {
        return getScriptLocation(true, profileName);
    }

    static Location getExportScriptLocation(String profileName) {
        return getScriptLocation(false, profileName);
    }

    private static Location getScriptLocation(boolean importer, String profileName) {
        assert profileName != null;
        return Location.of(MessageFormat.format(
                PATTERN_SCRIPT_LOCATION,
                importer ? OPT_IMPORT : OPT_EXPORT,
                profileName));
    }

    private ProcessScript<?> createProcessScript(
            String processName,
            Class<?> dataModelType,
            DriverScript source,
            DriverScript drain) {
        assert processName != null;
        assert dataModelType != null;
        assert source != null;
        assert drain != null;
        return new ProcessScript<>(
                processName,
                Constants.DEFAULT_PROCESS_NAME,
                dataModelType,
                source,
                drain);
    }

    private Map<String, GateScript> toGateScripts(Map<String, List<ProcessScript<?>>> processes) {
        assert processes != null;
        Map<String, GateScript> results = new TreeMap<>();
        for (Map.Entry<String, List<ProcessScript<?>>> entry : processes.entrySet()) {
            results.put(entry.getKey(), new GateScript(entry.getKey(), entry.getValue()));
        }
        return results;
    }

    private void emitScript(Context context, Location path, GateScript script) throws IOException {
        assert path != null;
        assert script != null;
        Properties properties = new Properties();
        script.storeTo(properties);
        try (OutputStream output = context.addResourceFile(path)) {
            properties.store(output, context.getOptions().getBuildId());
        }
    }

    private ValueDescription extract(WindGateProcessDescription description) {
        DescriptionModel model = new DescriptionModel(description);
        ValueDescription value = Descriptions.valueOf(model);
        return value;
    }

    private DescriptionModel restore(ExternalPortInfo info) {
        ValueDescription value = info.getContents();
        if (value == null || value.getValueType().equals(MODEL_CLASS) == false) {
            throw new IllegalStateException();
        }
        try {
            return (DescriptionModel) value.resolve(DescriptionModel.class.getClassLoader());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
