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
package com.asakusafw.dag.runtime.directio;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.OutputAttemptContext;
import com.asakusafw.runtime.io.ModelOutput;

final class DirectFileOutputDriver implements InterruptibleIo {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileOutputDriver.class);

    private final OutputAttemptContext context;

    private final DirectDataSource source;

    private final DataDefinition<Object> definition;

    private final String basePath;

    private final Function<String, String> variableResolver;

    private final DirectFileCounterGroup counters;

    private boolean sawError;

    private boolean initialized;

    @SuppressWarnings("unchecked")
    DirectFileOutputDriver(
            OutputAttemptContext context,
            DirectDataSource source, DataDefinition<?> definition,
            String basePath, Function<String, String> variableResolver,
            DirectFileCounterGroup counters) {
        this.context = context;
        this.source = source;
        this.definition = (DataDefinition<Object>) definition;
        this.basePath = basePath;
        this.variableResolver = variableResolver;
        this.counters = counters;
    }

    public OutputAttemptContext getContext() {
        return context;
    }

    public Counter getRecordCounter() {
        return counters.getRecordCount();
    }

    public ModelOutput<Object> newInstance(String path) throws IOException, InterruptedException {
        if (initialized == false) {
            doInitialize();
            initialized = true;
        }
        String resolved = variableResolver.apply(path);
        return source.openOutput(context, definition, basePath, resolved, counters.getFileSize());
    }

    public void error(Throwable throwable) {
        LOG.error(MessageFormat.format(
                "error occurred while processing Direct file output: basePath={0}, context={1}",
                basePath,
                context), throwable);
        this.sawError = true;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (initialized) {
            doFinalize();
        }
    }

    private void doInitialize() throws IOException, InterruptedException {
        source.setupAttemptOutput(context);
    }

    private void doFinalize() throws IOException, InterruptedException {
        if (sawError == false) {
            source.commitAttemptOutput(context);
        }
        source.cleanupAttemptOutput(context);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "DirectFileOutput(basePath={0}, dataType={1})", //$NON-NLS-1$
                basePath,
                definition.getDataClass().getSimpleName());
    }
}
