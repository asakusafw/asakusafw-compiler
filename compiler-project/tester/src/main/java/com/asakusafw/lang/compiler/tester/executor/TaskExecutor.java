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
package com.asakusafw.lang.compiler.tester.executor;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.TesterContext;

/**
 * Executes a {@link TaskReference}.
 * @see TaskExecutors
 */
public interface TaskExecutor {

    /**
     * Returns whether this executor supports executing the target task.
     * @param context the current context
     * @param task the target task
     * @return {@code true} if this supports the target task, otherwise {@code false}
     */
    boolean isSupported(Context context, TaskReference task);

    /**
     * Executes a task.
     * @param context the current context
     * @param task the target task
     * @throws InterruptedException if interrupted while executing the task
     * @throws IOException if task execution was failed
     */
    void execute(Context context, TaskReference task) throws InterruptedException, IOException;

    /**
     * Represents a context of {@link TaskExecutor}.
     */
    public final class Context {

        private final TesterContext testerContext;

        private final BatchInfo batch;

        private final JobflowInfo jobflow;

        private final String executionId;

        private final Map<String, String> batchArguments;

        /**
         * Creates a new instance.
         * @param testerContext the parent tester context
         * @param batch the container batch information
         * @param jobflow the container jobflow information
         * @param executionId the current execution ID
         * @param batchArguments the batch arguments
         */
        public Context(
                TesterContext testerContext,
                BatchInfo batch, JobflowInfo jobflow,
                String executionId,
                Map<String, String> batchArguments) {
            this.testerContext = testerContext;
            this.batch = batch;
            this.jobflow = jobflow;
            this.executionId = executionId;
            this.batchArguments = Collections.unmodifiableMap(new LinkedHashMap<>(batchArguments));
        }

        /**
         * Returns the current tester context.
         * @return the current tester context
         */
        public TesterContext getTesterContext() {
            return testerContext;
        }

        /**
         * Returns the container batch information.
         * @return the container batch information
         */
        public BatchInfo getBatch() {
            return batch;
        }

        /**
         * Returns the container jobflow information.
         * @return the container jobflow information
         */
        public JobflowInfo getJobflow() {
            return jobflow;
        }

        /**
         * Returns the current execution ID.
         * @return the current execution ID
         */
        public String getExecutionId() {
            return executionId;
        }

        /**
         * Returns the batch arguments.
         * @return the batch arguments
         */
        public Map<String, String> getBatchArguments() {
            return batchArguments;
        }
    }
}
