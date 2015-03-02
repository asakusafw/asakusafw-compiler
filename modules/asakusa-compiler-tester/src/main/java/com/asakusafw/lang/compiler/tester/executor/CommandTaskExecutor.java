package com.asakusafw.lang.compiler.tester.executor;

import static com.asakusafw.lang.compiler.tester.executor.TaskExecutors.*;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;

/**
 * Executes a {@link CommandTaskReference}.
 */
public class CommandTaskExecutor implements TaskExecutor {

    private final File launcher;

    /**
     * Creates a new instance without any command launchers.
     */
    public CommandTaskExecutor() {
        this(null);
    }

    /**
     * Creates a new instance.
     * @param launcher a launcher file, or {@code null} if this executor launch a command without launchers
     */
    public CommandTaskExecutor(File launcher) {
        this.launcher = launcher;
    }

    @Override
    public boolean isSupported(Context context, TaskReference task) {
        return task instanceof CommandTaskReference;
    }

    @Override
    public void execute(Context context, TaskReference task) throws IOException, InterruptedException {
        assert task instanceof CommandTaskReference;
        CommandTaskReference commandTask = (CommandTaskReference) task;

        List<String> command = new ArrayList<>();
        if (launcher != null) {
            command.add(launcher.getAbsolutePath());
        }
        command.add(getFrameworkFile(context, commandTask.getCommand()).getAbsolutePath());
        command.addAll(resolveCommandTokens(context, commandTask.getArguments()));

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().putAll(context.getTesterContext().getEnvironmentVariables());
        builder.inheritIO();
        Process process = builder.start();
        try {
            int status = process.waitFor();
            if (status != 0) {
                throw new IOException(MessageFormat.format(
                        "unexpected exit status: task={0}, status={1}",
                        task,
                        status));
            }
        } finally {
            process.destroy();
        }
    }
}
