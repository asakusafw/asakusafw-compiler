/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor.basic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A basic implementation of {@link TaskSchedule}.
 * @since 0.4.0
 * @version 0.4.2
 */
public class BasicTaskSchedule implements TaskSchedule {

    private final List<TaskInfo> tasks;

    private final int maxConcurrency;

    /**
     * Creates a new empty instance.
     */
    public BasicTaskSchedule() {
        this(Collections.emptyList());
    }

    /**
     * Creates a new instance.
     * @param tasks each task information
     */
    public BasicTaskSchedule(List<? extends TaskInfo> tasks) {
        this(new ArrayList<>(Arguments.requireNonNull(tasks)), -1);
    }

    /**
     * Creates a new instance.
     * @param tasks each task information
     */
    public BasicTaskSchedule(TaskInfo... tasks) {
        this(Arrays.asList(tasks));
    }

    private BasicTaskSchedule(List<TaskInfo> tasks, int maxConcurrency) {
        Arguments.requireNonNull(tasks);
        this.tasks = tasks;
        this.maxConcurrency = maxConcurrency;
    }

    /**
     * Returns a new schedule with the given max concurrency.
     * @param newValue the new max concurrency
     * @return the created object
     * @since 0.4.2
     */
    public BasicTaskSchedule withMaxConcurrency(int newValue) {
        return new BasicTaskSchedule(tasks, newValue);
    }

    @Override
    public List<TaskInfo> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    @Override
    public OptionalInt getMaxConcurrency() {
        if (maxConcurrency >= 1) {
            return OptionalInt.of(maxConcurrency);
        } else {
            return OptionalInt.empty();
        }
    }

    @Override
    public String toString() {
        return Objects.toString(tasks);
    }
}
