package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.model.Location;

/**
 * A symbol of task with any command.
 */
public class CommandTaskReference implements TaskReference {

    private final ExecutionPhase executionPhase;

    private final int serialNumber;

    private final String profileName;

    private final Location command;

    private final List<CommandToken> arguments;

    private final List<TaskReference> blockerTasks;

    /**
     * Creates a new instance.
     * @param executionPhase the execution phase of this task
     * @param serialNumber the serial number of this task (unique in the same execution phase)
     * @param profileName the profile name where the command is running
     * @param command the command path (relative from {@code ASAKUSA_HOME})
     * @param arguments the command arguments
     * @param blockerTasks the blocker tasks
     */
    public CommandTaskReference(
            ExecutionPhase executionPhase,
            int serialNumber,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            List<? extends TaskReference> blockerTasks) {
        this.executionPhase = executionPhase;
        this.serialNumber = serialNumber;
        this.profileName = profileName;
        this.command = command;
        this.arguments = Collections.unmodifiableList(new ArrayList<>(arguments));
        this.blockerTasks = Collections.unmodifiableList(new ArrayList<>(blockerTasks));
    }

    @Override
    public TaskKind getTaskKind() {
        return TaskKind.COMMAND;
    }

    @Override
    public ExecutionPhase getExecutionPhase() {
        return executionPhase;
    }

    /**
     * Returns the serial number of this task: this must be identical in the same execution phase.
     * @return the serial number
     */
    public int getSerialNumber() {
        return serialNumber;
    }

    @Override
    public List<TaskReference> getBlockerTasks() {
        return blockerTasks;
    }

    /**
     * Returns the profile name where the command is running.
     * @return the profile name
     */
    public String getProfileName() {
        return profileName;
    }

    /**
     * Returns the target command path.
     * @return the target command path (relative from {@code ASAKUSA_HOME})
     */
    public Location getCommand() {
        return command;
    }

    /**
     * Returns the command arguments.
     * @return the command arguments
     */
    public List<CommandToken> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "CommandTask({0}:{1})", //$NON-NLS-1$
                profileName,
                command);
    }
}
