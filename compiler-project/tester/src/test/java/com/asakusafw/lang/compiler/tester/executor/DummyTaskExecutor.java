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
