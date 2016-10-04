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
package com.asakusafw.dag.extension.counter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.counter.CounterGroup.Category;
import com.asakusafw.dag.api.counter.CounterGroup.Column;
import com.asakusafw.dag.api.counter.CounterGroup.Element;
import com.asakusafw.dag.api.counter.CounterGroup.Scope;
import com.asakusafw.dag.api.counter.CounterRepository;
import com.asakusafw.dag.api.counter.basic.BasicCounterRepository;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Enables {@link CounterRepository}.
 * @since 0.4.0
 */
public class CounterRepositorySupportExtension implements ProcessorContextExtension {

    /**
     * The property key whether this feature is enabled or not.
     */
    public static final String KEY_ENABLED = "com.asakusafw.dag.extension.counter"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_ENABLED}.
     */
    public static final boolean DEFAULT_ENABLED = true;

    static final Logger LOG = LoggerFactory.getLogger(CounterRepositorySupportExtension.class);

    @Override
    public InterruptibleIo install(ProcessorContext context, ProcessorContext.Editor editor) {
        boolean enabled = context.getProperty(KEY_ENABLED)
            .map(Boolean::parseBoolean)
            .orElse(DEFAULT_ENABLED);
        if (enabled == false) {
            LOG.debug("execution counter is disabled");
            return null;
        }
        LOG.debug("enabling execution counter");
        CounterRepository repository = new BasicCounterRepository();
        editor.addResource(CounterRepository.class, repository);
        return () -> printCounters(repository);
    }

    private static void printCounters(CounterRepository repository) {
        if (LOG.isInfoEnabled() == false) {
            return;
        }
        // TODO also print Scope.VERTEX
        Map<Category<?>, Map<String, Map<Column, Long>>> categories = repository.stream()
                .filter(e -> e.getCategory().getScope() == Scope.GRAPH)
                .collect(Collectors.groupingBy(
                        CounterRepository.Entry::getCategory,
                        Collectors.toMap(
                                CounterRepository.Entry::getItemId,
                                CounterRepository.Entry::getCounters,
                                CounterRepository::merge,
                                TreeMap::new)));
        forEachElement(categories, (category, members) -> {
            LOG.info(String.format("%s: %,d entries", category.getDescription(), members.size()));
            Map<Column, Long> total = new LinkedHashMap<>();
            members.forEach((item, counters) -> {
                LOG.info(String.format("  %s:", item));
                forEachElement(counters, (column, value) -> {
                    LOG.info(String.format("    %s: %,d", column.getDescription(), value));
                });
                CounterRepository.mergeInto(counters, total);
            });
            LOG.info(String.format("  %s:", "(TOTAL)"));
            forEachElement(total, (column, value) -> {
                LOG.info(String.format("    %s: %,d", column.getDescription(), value));
            });
        });
    }

    private static <K extends Element, V> void forEachElement(Map<K, V> map, BiConsumer<K, V> action) {
        map.entrySet().stream()
            .map(Tuple::of)
            .sorted((a, b) -> a.left().getIndexText().compareTo(b.left().getIndexText()))
            .forEachOrdered(t -> action.accept(t.left(), t.right()));
    }
}
