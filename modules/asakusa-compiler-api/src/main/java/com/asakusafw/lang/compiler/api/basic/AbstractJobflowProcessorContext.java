package com.asakusafw.lang.compiler.api.basic;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract implementation of {@link com.asakusafw.lang.compiler.api.JobflowProcessor.Context}.
 */
public abstract class AbstractJobflowProcessorContext extends BasicExtensionContainer
        implements JobflowProcessor.Context {

    private static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    private final TaskContainerMap tasks = new TaskContainerMap();

    private final Map<String, ExternalInputReference> externalInputs = new LinkedHashMap<>();

    private final Map<String, ExternalOutputReference> externalOutputs = new LinkedHashMap<>();

    /**
     * Returns tasks which are executed in this context.
     * @return tasks
     */
    public TaskContainerMap getTasks() {
        return tasks;
    }

    /**
     * Returns the external inputs which {@link #addExternalInput(String, ClassDescription) added} to this context.
     * @return the added external inputs
     */
    public List<ExternalInputReference> getExternalInputs() {
        return new ArrayList<>(externalInputs.values());
    }

    /**
     * Returns the external outputs which {@link #addExternalOutput(String, ClassDescription, Collection) added}
     * to this context.
     * @return the added external outputs
     */
    public List<ExternalOutputReference> getExternalOutputs() {
        return new ArrayList<>(externalOutputs.values());
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        String path = aClass.getName().replace('.', '/') + EXTENSION_CLASS;
        return addResourceFile(Location.of(path, '/'));
    }

    @Override
    public final ExternalInputReference addExternalInput(
            String name,
            ClassDescription descriptionClass) {
        if (externalInputs.containsKey(name)) {
            throw new IllegalStateException(MessageFormat.format(
                    "external input is already declared in this jobflow: \"{0}\" ({1})",
                    name,
                    descriptionClass.getName()));
        }
        ExternalInputReference result = createExternalInput(name, descriptionClass);
        externalInputs.put(name, result);
        return result;
    }

    @Override
    public final ExternalOutputReference addExternalOutput(
            String name,
            ClassDescription descriptionClass,
            Collection<String> internalOutputPaths) {
        if (externalOutputs.containsKey(name)) {
            throw new IllegalStateException(MessageFormat.format(
                    "external output is already declared in this jobflow: \"{0}\" ({1})",
                    name,
                    descriptionClass.getName()));
        }
        ExternalOutputReference result = createExternalOutput(name, descriptionClass, internalOutputPaths);
        externalOutputs.put(name, result);
        return result;
    }

    /**
     * Creates a new {@link ExternalInputReference}.
     * @param name the input name
     * @param descriptionClass the description class
     * @return the created instance
     * @see #addExternalInput(String, ClassDescription)
     */
    protected abstract ExternalInputReference createExternalInput(
            String name,
            ClassDescription descriptionClass);

    /**
     * Creates a new {@link ExternalOutputReference}.
     * @param name the input name
     * @param descriptionClass the description class
     * @param internalOutputPaths the output paths which will be internally generated in this jobflow
     * @return the created instance
     * @see #addExternalOutput(String, ClassDescription, Collection)
     */
    protected abstract ExternalOutputReference createExternalOutput(
            String name,
            ClassDescription descriptionClass,
            Collection<String> internalOutputPaths);

    @Override
    public final TaskReference addTask(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        return addTask(tasks.getMainTaskContainer(), moduleName, profileName, command, arguments, blockers);
    }

    @Override
    public final TaskReference addFinalizer(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        return addTask(tasks.getFinalizeTaskContainer(), moduleName, profileName, command, arguments, blockers);
    }

    private TaskReference addTask(
            TaskContainer container,
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        CommandTaskReference task = new CommandTaskReference(
                moduleName, profileName,
                command, arguments,
                Arrays.asList(blockers));
        container.add(task);
        return task;
    }
}
