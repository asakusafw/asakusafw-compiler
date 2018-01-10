/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;

/**
 * Executes a {@link HadoopTaskReference}.
 */
public class HadoopTaskExecutor implements TaskExecutor {

    // FIXME replace with workflow
    private static final Location PATH_YAESS_HADOOP =
            Location.of("yaess-hadoop/libexec/hadoop-execute.sh"); //$NON-NLS-1$

    private final CommandTaskExecutor delegate;

    /**
     * Creates a new instance without any command launchers.
     */
    public HadoopTaskExecutor() {
        this(new CommandTaskExecutor());
    }

    /**
     * Creates a new instance.
     * @param delegate delegation target
     */
    public HadoopTaskExecutor(CommandTaskExecutor delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isSupported(Context context, TaskReference task) {
        return task instanceof HadoopTaskReference;
    }

    @Override
    public void execute(Context context, TaskReference task) throws IOException, InterruptedException {
        assert task instanceof HadoopTaskReference;
        HadoopTaskReference hadoop = (HadoopTaskReference) task;
        List<CommandToken> arguments = new ArrayList<>();
        arguments.add(CommandToken.of(hadoop.getMainClass().getClassName()));
        arguments.add(CommandToken.BATCH_ID);
        arguments.add(CommandToken.FLOW_ID);
        arguments.add(CommandToken.EXECUTION_ID);
        arguments.add(CommandToken.BATCH_ARGUMENTS);
        CommandTaskReference commandTask = new CommandTaskReference(
                hadoop.getModuleName(),
                hadoop.getModuleName(),
                PATH_YAESS_HADOOP,
                arguments,
                task.getExtensions(),
                Collections.emptyList());
        delegate.execute(context, commandTask);
    }
}
