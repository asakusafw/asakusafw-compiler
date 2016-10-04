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
package com.asakusafw.dag.runtime.directio;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.skeleton.CustomVertexProcessor;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.OutputTransactionContext;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;

/**
 * A {@link VertexProcessor} for committing Direct I/O output files.
 * @since 0.4.0
 */
public class DirectFileOutputCommit extends CustomVertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileOutputCommit.class);

    private final List<Spec> specs = new ArrayList<>();

    private TransactionManager transactionManager;

    /**
     * Adds a commit specification.
     * @param id the output ID
     * @param basePath the target base path
     * @return this
     */
    public final DirectFileOutputCommit bind(String id, String basePath) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(basePath);
        Spec spec = new Spec(id, basePath);
        specs.add(spec);
        return this;
    }

    @Override
    protected synchronized List<? extends CustomTaskInfo> schedule(
            VertexProcessorContext context) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        StageInfo stage = context.getResource(StageInfo.class).orElseThrow(AssertionError::new);
        Configuration conf = context.getResource(Configuration.class).orElseThrow(AssertionError::new);
        transactionManager = createTransactionManager(stage, conf);
        List<CustomTaskInfo> actions = resolve(
                HadoopDataSourceUtil.loadRepository(conf),
                stage::resolveUserVariables);
        transactionManager.begin();
        return actions;
    }

    @Override
    public synchronized void close() throws IOException, InterruptedException {
        if (transactionManager != null) {
            transactionManager.end();
            transactionManager = null;
        }
    }

    static TransactionManager createTransactionManager(StageInfo stage, Configuration conf) {
        assert stage != null;
        assert conf != null;
        Map<String, String> props = new LinkedHashMap<>();
        props.put("User Name", stage.getUserName()); //$NON-NLS-1$
        props.put("Batch ID", stage.getBatchId()); //$NON-NLS-1$
        props.put("Flow ID", stage.getFlowId()); //$NON-NLS-1$
        props.put("Execution ID", stage.getExecutionId()); //$NON-NLS-1$
        props.put("Stage ID", stage.getStageId()); //$NON-NLS-1$
        props.put("Batch Arguments", stage.getBatchArguments().toString()); //$NON-NLS-1$
        return new TransactionManager(conf, stage.getExecutionId(), props);
    }

    private List<CustomTaskInfo> resolve(
            DirectDataSourceRepository repository,
            Function<String, String> variableResolver) throws IOException, InterruptedException {
        assert repository != null;
        assert transactionManager != null;
        assert variableResolver != null;
        assert specs != null;
        Set<String> containerPaths = new LinkedHashSet<>();
        for (Spec spec : specs) {
            String basePath = variableResolver.apply(spec.basePath);
            String containerPath = repository.getContainerPath(basePath);
            containerPaths.add(containerPath);
        }
        List<CustomTaskInfo> results = new ArrayList<>();
        for (String containerPath : containerPaths) {
            String id = repository.getRelatedId(containerPath);
            DirectDataSource source = repository.getRelatedDataSource(containerPath);
            OutputTransactionContext context = transactionManager.acquire(id);
            results.add(c -> {
                LOG.debug("commiting Direct I/O file output: {}/*", containerPath);
                source.commitTransactionOutput(context);
                source.cleanupTransactionOutput(context);
                transactionManager.release(context);
            });
        }
        return results;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "DirectFileOutputCommit({0})",
                specs.size());
    }

    private static final class Spec {

        final String id;

        final String basePath;

        Spec(String id, String basePath) {
            assert id != null;
            assert basePath != null;
            this.id = id;
            this.basePath = basePath;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Spec(id={0}, basePath={1})", //$NON-NLS-1$
                    id,
                    basePath);
        }
    }
}
