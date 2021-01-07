/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.skeleton.CustomVertexProcessor;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.FilePattern;
import com.asakusafw.runtime.directio.ResourcePattern;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;

/**
 * A {@link VertexProcessor} for setup Direct I/O file outputs.
 * @since 0.4.0
 */
public class DirectFileOutputSetup extends CustomVertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileOutputSetup.class);

    private final List<Spec> specs = new ArrayList<>();

    /**
     * Adds a setup.
     * @param id the output ID
     * @param basePath the target base path
     * @param deletePatterns delete resource patterns
     * @return this
     */
    public final DirectFileOutputSetup bind(String id, String basePath, List<String> deletePatterns) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(basePath);
        Arguments.requireNonNull(deletePatterns);
        specs.add(new Spec(id, basePath, Arguments.freeze(deletePatterns)));
        return this;
    }

    /**
     * Adds a setup.
     * @param id the output ID
     * @param basePath the target base path
     * @param deletePatterns delete resource patterns
     * @return this
     */
    public final DirectFileOutputSetup bind(String id, String basePath, String[] deletePatterns) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(basePath);
        Arguments.requireNonNull(deletePatterns);
        return bind(id, basePath, Arrays.asList(deletePatterns));
    }

    @Override
    protected List<? extends CustomTaskInfo> schedule(
            VertexProcessorContext context) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        StageInfo stage = context.getResource(StageInfo.class).orElseThrow(AssertionError::new);
        Configuration conf = context.getResource(Configuration.class).orElseThrow(AssertionError::new);
        List<CustomTaskInfo> actions = resolve(
                HadoopDataSourceUtil.loadRepository(conf),
                stage::resolveUserVariables);
        return actions;
    }

    private List<CustomTaskInfo> resolve(
            DirectDataSourceRepository repository,
            Function<String, String> variableResolver) throws IOException, InterruptedException {
        List<CustomTaskInfo> results = new ArrayList<>();
        for (Spec spec : specs) {
            if (spec.deletePatterns.isEmpty()) {
                continue;
            }
            String basePath = variableResolver.apply(spec.basePath);
            String containerPath = repository.getContainerPath(basePath);
            String componentPath = repository.getComponentPath(basePath);
            DirectDataSource source = repository.getRelatedDataSource(containerPath);
            List<ResourcePattern> targets = spec.deletePatterns.stream()
                    .map(variableResolver::apply)
                    .map(FilePattern::compile)
                    .collect(Collectors.toList());
            Counter counter = new Counter();
            results.add(c -> {
                LOG.debug("preparing Direct I/O file output: {}", spec);
                for (ResourcePattern target : targets) {
                    source.delete(componentPath, target, true, counter);
                }
            });
        }
        return results;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "DirectFileOutputSetup({0})", //$NON-NLS-1$
                specs.size());
    }

    private static final class Spec {

        final String id;

        final String basePath;

        final List<String> deletePatterns;

        Spec(String id, String basePath, List<String> deletePatterns) {
            assert id != null;
            assert basePath != null;
            assert deletePatterns != null;
            this.id = id;
            this.basePath = basePath;
            this.deletePatterns = deletePatterns;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Spec(id={0}, basePath={1})", //$NON-NLS-1$
                    id, basePath);
        }
    }
}
