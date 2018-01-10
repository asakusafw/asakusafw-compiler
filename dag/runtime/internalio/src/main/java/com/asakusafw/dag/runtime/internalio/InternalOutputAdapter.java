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
package com.asakusafw.dag.runtime.internalio;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.OutputAdapter;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * {@link OutputAdapter} for internal outputs.
 * @since 0.4.0
 */
public class InternalOutputAdapter implements OutputAdapter {

    /**
     * The placeholder symbol.
     */
    public static final char PLACEHOLDER = '*';

    private final StageInfo stage;

    private final Configuration configuration;

    private final List<OutputSpec> specs = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public InternalOutputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        this.stage = context.getResource(StageInfo.class).orElseThrow(AssertionError::new);
        this.configuration = context.getResource(Configuration.class).orElseThrow(AssertionError::new);
    }

    /**
     * Adds a simple output.
     * The output pattern must contain wildcard character ({@code "*"}).
     * @param id the output ID
     * @param pathPattern the path pattern
     * @param dataClass the data class
     * @return this
     */
    public final InternalOutputAdapter bind(String id, String pathPattern, Class<? extends Writable> dataClass) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(pathPattern);
        Arguments.requireNonNull(dataClass);
        String resolved = stage.resolveUserVariables(pathPattern);
        int index = resolved.lastIndexOf(PLACEHOLDER);
        Invariants.require(index >= 0, () -> MessageFormat.format(
                "output path must be contain at least one placeholder \"{0}\": {1}",
                PLACEHOLDER,
                pathPattern));
        resolved = new StringBuilder(resolved).deleteCharAt(index).toString();
        specs.add(new OutputSpec(id, resolved, dataClass));
        return this;
    }

    @Override
    public OutputHandler<? super TaskProcessorContext> newHandler() throws IOException, InterruptedException {
        return new InternalOutputHandler(configuration, specs);
    }
}
