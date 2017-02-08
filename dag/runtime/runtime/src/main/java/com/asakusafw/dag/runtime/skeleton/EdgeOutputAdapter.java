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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.dag.runtime.adapter.OutputAdapter;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputHandler.AggregationStrategy;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputHandler.OutputSpec;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * {@link OutputAdapter} for edge output.
 * @since 0.4.0
 * @version 0.4.1
 */
public class EdgeOutputAdapter implements OutputAdapter {

    /**
     * The configuration key of the maximum aggregation window size (the number of entries, per output*thread).
     */
    public static final String KEY_AGGREGATION_WINDOW_SIZE =
            "com.asakusafw.dag.output.aggregate.window.size"; //$NON-NLS-1$

    /**
     * The configuration key of the aggregation strategy.
     * @since 0.4.1
     */
    public static final String KEY_AGGREGATION_STRATEGY =
            "com.asakusafw.dag.output.aggregate.strategy"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_AGGREGATION_WINDOW_SIZE}.
     */
    public static final int DEFAULT_AGGREGATION_WINDOW_SIZE = 256;

    /**
     * The default value of {@link #KEY_AGGREGATION_STRATEGY}.
     * @since 0.4.1
     */
    static final AggregationStrategy DEFAULT_AGGREGATION_STRATEGY = AggregationStrategy.MAP;

    private final List<OutputSpec> specs = new ArrayList<>();

    final int aggregationWindowSize;

    final AggregationStrategy aggregationStrategy;

    private final Supplier<? extends KeyBuffer> keyBufferFactory;

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public EdgeOutputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        this.aggregationWindowSize = Util.getProperty(
                context,
                "aggregation window size",
                KEY_AGGREGATION_WINDOW_SIZE, DEFAULT_AGGREGATION_WINDOW_SIZE);
        this.aggregationStrategy = Util.getProperty(
                context,
                "aggregation strategy",
                KEY_AGGREGATION_STRATEGY, DEFAULT_AGGREGATION_STRATEGY);
        this.keyBufferFactory = Util.getKeyBufferSupplier(context);
    }

    /**
     * Bind the output.
     * @param name the output name
     * @return this
     */
    public final EdgeOutputAdapter bind(String name) {
        Arguments.requireNonNull(name);
        specs.add(new OutputSpec(name));
        return this;
    }

    /**
     * Bind the output.
     * @param name the output name
     * @param mapperClass the output mapper class (nullable)
     * @param copierClass the output copier class (nullable)
     * @param combinerClass the output combiner class (nullable)
     * @return this
     */
    public final EdgeOutputAdapter bind(
            String name,
            Class<? extends Function<?, ?>> mapperClass,
            Class<? extends ObjectCopier<?>> copierClass, Class<? extends ObjectCombiner<?>> combinerClass) {
        Arguments.requireNonNull(name);
        return bind(name, Util.toSupplier(mapperClass), Util.toSupplier(copierClass), Util.toSupplier(combinerClass));
    }

    /**
     * Bind the output.
     * @param name the output name
     * @param mapper the output mapper (nullable)
     * @param copier the output copier (nullable)
     * @param combiner the output combiner (nullable)
     * @return this
     */
    public final EdgeOutputAdapter bind(
            String name,
            Supplier<? extends Function<?, ?>> mapper,
            Supplier<? extends ObjectCopier<?>> copier, Supplier<? extends ObjectCombiner<?>> combiner) {
        Arguments.requireNonNull(name);
        AggregationStrategy strategy = aggregationStrategy;
        if (copier == null || combiner == null || aggregationWindowSize <= 0) {
            strategy = AggregationStrategy.DISABLED;
        }
        specs.add(new OutputSpec(name, mapper, copier, combiner, keyBufferFactory, aggregationWindowSize, strategy));
        return this;
    }

    @Override
    public OutputHandler<? super TaskProcessorContext> newHandler() throws IOException, InterruptedException {
        if (specs.isEmpty()) {
            return VoidOutputHandler.INSTANCE;
        }
        return new EdgeOutputHandler(specs);
    }
}
