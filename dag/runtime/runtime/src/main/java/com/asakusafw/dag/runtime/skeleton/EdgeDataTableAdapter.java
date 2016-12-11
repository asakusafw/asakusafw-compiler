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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.DataTable.Builder;
import com.asakusafw.dag.runtime.adapter.DataTableAdapter;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.adapter.KeyExtractor;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * {@link DataTableAdapter} for edge output.
 * @since 0.4.0
 * @version 0.4.1
 */
public class EdgeDataTableAdapter implements DataTableAdapter {

    private final VertexProcessorContext context;

    private final Supplier<? extends KeyBuffer> keyBufferFactory;

    private final List<Spec> specs = new ArrayList<>();

    private final Map<String, DataTable<?>> resolved = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public EdgeDataTableAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        this.context = context;
        this.keyBufferFactory = Util.getKeyBufferSupplier(context);
    }

    /**
     * Binds the data table.
     * @param tableId the table ID
     * @param inputId the source input ID
     * @param keyExtractor the key builder (nullable)
     * @param copier the object copier
     * @return this
     */
    public EdgeDataTableAdapter bind(
            String tableId, String inputId,
            Supplier<? extends KeyExtractor<?>> keyExtractor,
            Supplier<? extends ObjectCopier<?>> copier) {
        return bind(tableId, inputId, keyExtractor, copier, null);
    }

    /**
     * Binds the data table.
     * @param tableId the table ID
     * @param inputId the source input ID
     * @param keyExtractor the key builder (nullable)
     * @param copier the object copier
     * @param comparator the comparator (nullable)
     * @return this
     * @since 0.4.1
     */
    public EdgeDataTableAdapter bind(
            String tableId, String inputId,
            Supplier<? extends KeyExtractor<?>> keyExtractor,
            Supplier<? extends ObjectCopier<?>> copier,
            Supplier<? extends Comparator<?>> comparator) {
        Arguments.requireNonNull(tableId);
        Arguments.requireNonNull(inputId);
        Arguments.requireNonNull(copier);
        Supplier<? extends DataTable.Builder<Object>> tableBuilders = () -> new BasicDataTable.Builder<>(
                new HashMap<>(), keyBufferFactory);
        specs.add(new Spec(tableId, inputId, tableBuilders, keyExtractor, copier, comparator));
        return this;
    }

    /**
     * Binds the data table.
     * @param tableId the table ID
     * @param inputId the source input ID
     * @param keyExtractor the key builder (nullable)
     * @param copier the object copier
     * @return this
     */
    public EdgeDataTableAdapter bind(
            String tableId, String inputId,
            Class<? extends KeyExtractor<?>> keyExtractor,
            Class<? extends ObjectCopier<?>> copier) {
        Arguments.requireNonNull(tableId);
        Arguments.requireNonNull(inputId);
        Arguments.requireNonNull(tableId);
        return bind(tableId, inputId, Util.toSupplier(keyExtractor), Util.toSupplier(copier));
    }

    /**
     * Binds the data table.
     * @param tableId the table ID
     * @param inputId the source input ID
     * @param keyExtractor the key builder (nullable)
     * @param copier the object copier
     * @param comparator the entry comparator (nullable)
     * @return this
     * @since 0.4.1
     */
    public EdgeDataTableAdapter bind(
            String tableId, String inputId,
            Class<? extends KeyExtractor<?>> keyExtractor,
            Class<? extends ObjectCopier<?>> copier,
            Class<? extends Comparator<?>> comparator) {
        Arguments.requireNonNull(tableId);
        Arguments.requireNonNull(inputId);
        Arguments.requireNonNull(tableId);
        return bind(tableId, inputId,
                Util.toSupplier(keyExtractor), Util.toSupplier(copier), Util.toSupplier(comparator));
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
        for (Spec spec : specs) {
            ObjectCopier<Object> copier = spec.copier.get();
            KeyExtractor<Object> extractor = spec.keyBuilder == null ? null : spec.keyBuilder.get();
            DataTable.Builder<Object> table = spec.tableBuilder.get();
            KeyBuffer key = table.newKeyBuffer();
            key.clear();
            try (ObjectReader reader = (ObjectReader) context.getInput(spec.inputId)) {
                while (reader.nextObject()) {
                    Object object = copier.newCopy(reader.getObject());
                    if (extractor != null) {
                        key.clear();
                        extractor.buildKey(key, object);
                    }
                    table.add(key, object);
                }
            }
            Comparator<Object> comparator = spec.comparator == null ? null : spec.comparator.get();
            resolved.put(spec.tableId, table.build(comparator));
        }
    }

    @Override
    public Set<String> getIds() {
        return resolved.keySet();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> DataTable<T> getDataTable(Class<T> type, String id) {
        Invariants.require(resolved.containsKey(id));
        return (DataTable<T>) resolved.get(id);
    }

    private static final class Spec {

        final String tableId;

        final String inputId;

        final Supplier<? extends DataTable.Builder<Object>> tableBuilder;

        final Supplier<? extends KeyExtractor<Object>> keyBuilder;

        final Supplier<? extends ObjectCopier<Object>> copier;

        final Supplier<? extends Comparator<Object>> comparator;

        @SuppressWarnings("unchecked")
        Spec(String tableId, String inputId,
                Supplier<? extends DataTable.Builder<?>> tableBuilder,
                Supplier<? extends KeyExtractor<?>> keyBuilder,
                Supplier<? extends ObjectCopier<?>> copier,
                Supplier<? extends Comparator<?>> comparator) {
            this.tableId = tableId;
            this.inputId = inputId;
            this.tableBuilder = (Supplier<? extends Builder<Object>>) tableBuilder;
            this.keyBuilder = (Supplier<? extends KeyExtractor<Object>>) keyBuilder;
            this.copier = (Supplier<? extends ObjectCopier<Object>>) copier;
            this.comparator = (Supplier<? extends Comparator<Object>>) comparator;
        }
    }
}
