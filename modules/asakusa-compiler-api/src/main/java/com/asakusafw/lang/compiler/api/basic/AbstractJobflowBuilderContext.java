package com.asakusafw.lang.compiler.api.basic;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.JobflowBuilder;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalPortReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;

/**
 * An abstract implementation of {@link JobflowBuilder}.
 */
public abstract class AbstractJobflowBuilderContext implements JobflowBuilder.Context {

    private static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    private final TaskReferenceContainer mainTasks = new TaskReferenceContainer("main");

    private final TaskReferenceContainer finalizeTasks = new TaskReferenceContainer("finalize");

    private final Map<ExternalInput, ExternalPortReference<ExternalInput>> externalInputs = new LinkedHashMap<>();

    private final Map<ExternalOutput, ExternalPortReference<ExternalOutput>> externalOutputs = new LinkedHashMap<>();

    /**
     * Returns tasks which are executes in the main phase.
     * @return the main tasks
     */
    public TaskReferenceContainer getMainTasks() {
        return mainTasks;
    }

    /**
     * Returns tasks which are executes in the finalize phase.
     * @return the finalize tasks
     */
    public TaskReferenceContainer getFinalizeTasks() {
        return finalizeTasks;
    }

    /**
     * Returns the external inputs which {@link #addExternalInput(ExternalInput) added} to this context.
     * @return the added external inputs
     */
    public List<ExternalPortReference<ExternalInput>> getExternalInputs() {
        return new ArrayList<>(externalInputs.values());
    }

    /**
     * Returns the external outputs which {@link #addExternalOutput(ExternalOutput) added} to this context.
     * @return the added external outputs
     */
    public List<ExternalPortReference<ExternalOutput>> getExternalOutputs() {
        return new ArrayList<>(externalOutputs.values());
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        String path = aClass.getName().replace('.', '/') + EXTENSION_CLASS;
        return addResourceFile(Location.of(path, '/'));
    }

    @Override
    public final ExternalPortReference<ExternalInput> addExternalInput(ExternalInput port) {
        if (port.isExternal() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "input must be external: {0}", //$NON-NLS-1$
                    port));
        }
        if (externalInputs.containsKey(port)) {
            throw new IllegalStateException();
        }
        ExternalPortReference<ExternalInput> result = createExternalInput(port);
        externalInputs.put(port, result);
        return result;
    }

    @Override
    public final ExternalPortReference<ExternalOutput> addExternalOutput(ExternalOutput port) {
        if (externalInputs.containsKey(port)) {
            throw new IllegalStateException();
        }
        ExternalPortReference<ExternalOutput> result = createExternalOutput(port);
        externalOutputs.put(port, result);
        return result;
    }

    /**
     * Creates an {@link ExternalPortReference} for the external input.
     * @param port the target port
     * @return the created reference
     */
    protected abstract ExternalPortReference<ExternalInput> createExternalInput(ExternalInput port);

    /**
     * Creates an {@link ExternalPortReference} for the external output.
     * @param port the target port
     * @return the created reference
     */
    protected abstract ExternalPortReference<ExternalOutput> createExternalOutput(ExternalOutput port);

    @Override
    public final TaskReference addTask(
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        CommandTaskReference task = new CommandTaskReference(
                mainTasks.getNextSerialNumber(),
                profileName,
                command,
                arguments,
                Arrays.asList(blockers));
        mainTasks.add(task);
        return task;
    }

    @Override
    public final TaskReference addFinalizer(
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        CommandTaskReference task = new CommandTaskReference(
                finalizeTasks.getNextSerialNumber(),
                profileName,
                command,
                arguments,
                Arrays.asList(blockers));
        finalizeTasks.add(task);
        return task;
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return null;
    }
}
