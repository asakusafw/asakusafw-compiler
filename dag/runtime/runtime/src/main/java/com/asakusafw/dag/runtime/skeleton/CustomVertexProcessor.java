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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * A skeletal implementation of {@link VertexProcessor} which requires custom task schedule.
 * @since 0.4.0
 */
public abstract class CustomVertexProcessor implements VertexProcessor {

    @Override
    public final Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        List<? extends CustomTaskInfo> tasks = schedule(context);
        return Optionals.of(new BasicTaskSchedule(tasks));
    }

    /**
     * Computes and returns custom tasks for this processor.
     * @param context the current vertex context
     * @return the computed tasks
     * @throws IOException if I/O error was occurred
     * @throws InterruptedException if this task operation was interrupted
     */
    protected abstract List<? extends CustomTaskInfo> schedule(
            VertexProcessorContext context) throws IOException, InterruptedException;

    @Override
    public final TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        return CustomTaskProcessor.INSTANCE;
    }

    /**
     * An abstract super interface of information of the {@link CustomVertexProcessor} tasks.
     * @since 0.4.0
     */
    @FunctionalInterface
    public interface CustomTaskInfo extends TaskInfo {

        /**
         * Performs this task.
         * @param context the current processing context
         * @throws IOException if I/O error was occurred
         * @throws InterruptedException if this task operation was interrupted
         */
        void perform(TaskProcessorContext context) throws IOException, InterruptedException;
    }

    private static final class CustomTaskProcessor implements TaskProcessor {

        static final CustomTaskProcessor INSTANCE = new CustomTaskProcessor();

        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            context.getTaskInfo()
                .map(CustomTaskInfo.class::cast)
                .orElseThrow(IllegalStateException::new)
                .perform(context);
        }
    }
}
