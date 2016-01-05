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

import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.runtime.util.VariableTable;
import com.asakusafw.testdriver.core.BaseImporterPreparator;
import com.asakusafw.testdriver.core.DataModelDefinition;
import com.asakusafw.testdriver.core.ImporterPreparator;
import com.asakusafw.testdriver.core.TestContext;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;

/**
 * Implementation of {@link ImporterPreparator} for {@link InternalImporterDescription}s.
 */
public class InternalImporterPreparator extends BaseImporterPreparator<InternalImporterDescription> {

    static final Logger LOG = LoggerFactory.getLogger(InternalImporterPreparator.class);

    private final ConfigurationFactory configurations;

    /**
     * Creates a new instance with default configurations.
     */
    public InternalImporterPreparator() {
        this(ConfigurationFactory.getDefault());
    }

    /**
     * Creates a new instance.
     * @param configurations the configuration factory
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public InternalImporterPreparator(ConfigurationFactory configurations) {
        this.configurations = Objects.requireNonNull(configurations, "configurations must not be null"); //$NON-NLS-1$
    }

    @Override
    public void truncate(InternalImporterDescription description, TestContext context) throws IOException {
        LOG.debug("deleting input: {}", description); //$NON-NLS-1$
        VariableTable variables = createVariables(context);
        Configuration config = configurations.newInstance();
        FileSystem fs = FileSystem.get(config);
        String resolved = variables.parse(description.getPathPrefix(), false);
        Path target = fs.makeQualified(new Path(resolved));
        FileStatus[] stats = fs.globStatus(target);
        if (stats == null || stats.length == 0) {
            return;
        }
        for (FileStatus s : stats) {
            Path path = s.getPath();
            LOG.debug("deleting file: {}", path); //$NON-NLS-1$
            boolean succeed = fs.delete(path, true);
            LOG.debug("deleted file (succeed={}): {}", succeed, path); //$NON-NLS-1$
        }
        return;
    }

    @Override
    public <V> ModelOutput<V> createOutput(
            DataModelDefinition<V> definition,
            InternalImporterDescription description,
            TestContext context) throws IOException {
        LOG.debug("preparing input: {}", description); //$NON-NLS-1$
        checkType(definition, description);
        String resolved = resolvePathPrefix(context, description.getPathPrefix());
        Configuration conf = configurations.newInstance();
        ModelOutput<V> output = TemporaryStorage.openOutput(conf, definition.getModelClass(), new Path(resolved));
        return output;
    }

    static String resolvePathPrefix(TestContext context, String pathPrefix) {
        VariableTable variables = createVariables(context);
        String destination = pathPrefix.replace('*', '_');
        return variables.parse(destination, false);
    }

    private static VariableTable createVariables(TestContext context) {
        assert context != null;
        VariableTable result = new VariableTable();
        result.defineVariables(context.getArguments());
        return result;
    }

    private <V> void checkType(DataModelDefinition<V> definition,
            InternalImporterDescription description) throws IOException {
        if (definition.getModelClass() != description.getModelType()) {
            throw new IOException(MessageFormat.format(
                    "inconsistent importer type: data-type={0}, input-type={1} ({2})",
                    definition.getModelClass().getName(),
                    description.getModelType().getName(),
                    description));
        }
    }
}
