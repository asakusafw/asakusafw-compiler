package com.asakusafw.lang.compiler.tester.executor;

import java.util.ArrayList;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.TaskReference;

/**
 * Mock {@link TaskExecutor}.
 */
public class DummyTaskExecutor implements TaskExecutor {

    private final List<TaskReference> tasks = new ArrayList<>();

    @Override
    public boolean isSupported(Context context, TaskReference task) {
        return true;
    }

    @Override
    public void execute(Context context, TaskReference task) {
        tasks.add(task);
    }

    /**
     * Returns the executed tasks.
     * @return the executed tasks
     */
    public List<TaskReference> getTasks() {
        return tasks;
    }
}
