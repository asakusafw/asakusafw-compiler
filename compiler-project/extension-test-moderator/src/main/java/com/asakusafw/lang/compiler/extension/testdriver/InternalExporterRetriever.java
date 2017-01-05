/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.testdriver;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.internalio.InternalExporterDescription;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.runtime.util.VariableTable;
import com.asakusafw.testdriver.core.BaseExporterRetriever;
import com.asakusafw.testdriver.core.DataModelDefinition;
import com.asakusafw.testdriver.core.DataModelSource;
import com.asakusafw.testdriver.core.ExporterRetriever;
import com.asakusafw.testdriver.core.TestContext;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;

/**
 * Implementation of {@link ExporterRetriever} for {@link InternalExporterDescription}s.
 */
public class InternalExporterRetriever extends BaseExporterRetriever<InternalExporterDescription> {

    static final Logger LOG = LoggerFactory.getLogger(InternalExporterRetriever.class);

    private final ConfigurationFactory configurations;

    /**
     * Creates a new instance with default configurations.
     */
    public InternalExporterRetriever() {
        this(ConfigurationFactory.getDefault());
    }

    /**
     * Creates a new instance.
     * @param configurations the configuration factory
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public InternalExporterRetriever(ConfigurationFactory configurations) {
        this.configurations = Objects.requireNonNull(configurations, "configurations must not be null"); //$NON-NLS-1$
    }

    @Override
    public void truncate(
            InternalExporterDescription description,
            TestContext context) throws IOException {
        LOG.debug("deleting output directory: {}", description); //$NON-NLS-1$
        VariableTable variables = createVariables(context);
        Configuration config = configurations.newInstance();
        FileSystem fs = FileSystem.get(config);
        String resolved = variables.parse(description.getPathPrefix(), false);
        Path path = new Path(resolved);
        Path output = path.getParent();
        Path target;
        if (output == null) {
            LOG.warn(MessageFormat.format(
                    "skipped deleting output directory because it is a base directory: {0}",
                    path));
            target = fs.makeQualified(path);
        } else {
            LOG.debug("output directory will be deleted: {}", output); //$NON-NLS-1$
            target = fs.makeQualified(output);
        }
        LOG.debug("deleting output target: {}", target); //$NON-NLS-1$
        try {
            FileStatus[] stats = fs.globStatus(path);
            for (FileStatus s : stats) {
                Path f = s.getPath();
                boolean deleted = fs.delete(f, true);
                LOG.debug("deleted output target (succeed={}): {}", deleted, f); //$NON-NLS-1$
            }
        } catch (IOException e) {
            LOG.debug("exception in truncate", e);
        }
    }

    @Override
    public <V> ModelOutput<V> createOutput(
            DataModelDefinition<V> definition,
            InternalExporterDescription description,
            TestContext context) throws IOException {
        LOG.debug("preparing initial output: {}", description); //$NON-NLS-1$
        checkType(definition, description);
        VariableTable variables = createVariables(context);
        String destination = description.getPathPrefix().replace('*', '_');
        String resolved = variables.parse(destination, false);
        Configuration conf = configurations.newInstance();
        ModelOutput<V> output = TemporaryStorage.openOutput(conf, definition.getModelClass(), new Path(resolved));
        return output;
    }

    @Override
    public <V> DataModelSource createSource(
            DataModelDefinition<V> definition,
            InternalExporterDescription description,
            TestContext context) throws IOException {
        LOG.debug("retrieving output: {}", description); //$NON-NLS-1$
        VariableTable variables = createVariables(context);
        checkType(definition, description);
        Configuration conf = configurations.newInstance();
        String resolved = variables.parse(description.getPathPrefix(), false);
        return new TemporaryDataModelSource(conf, definition, resolved);
    }

    private VariableTable createVariables(TestContext context) {
        assert context != null;
        VariableTable result = new VariableTable();
        result.defineVariables(context.getArguments());
        return result;
    }

    private <V> void checkType(DataModelDefinition<V> definition,
            InternalExporterDescription description) throws IOException {
        if (definition.getModelClass() != description.getModelType()) {
            throw new IOException(MessageFormat.format(
                    "inconsistent exporter type: data-type={0}, output-type={1} ({2})",
                    definition.getModelClass().getName(),
                    description.getModelType().getName(),
                    description));
        }
    }
}
