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
package com.asakusafw.lang.compiler.core;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.Exclusive;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.core.util.CompositeBatchProcessor;
import com.asakusafw.lang.compiler.core.util.CompositeCompilerParticipant;
import com.asakusafw.lang.compiler.core.util.CompositeDataModelProcessor;
import com.asakusafw.lang.compiler.core.util.CompositeExternalPortProcessor;
import com.asakusafw.lang.compiler.core.util.CompositeJobflowProcessor;

/**
 * Provides compiler services.
 */
public class ToolRepository {

    static final Logger LOG = LoggerFactory.getLogger(ToolRepository.class);

    private final DataModelProcessor dataModelProcessor;

    private final BatchProcessor batchProcessor;

    private final JobflowProcessor jobflowProcessor;

    private final ExternalPortProcessor externalPortProcessor;

    private final CompilerParticipant participant;

    /**
     * Creates a new instance.
     * Clients use {@link #builder(ClassLoader)} instead of directly use this constructor.
     * @param dataModelProcessor the data model processor
     * @param batchProcessor the batch processor
     * @param jobflowProcessor the jobflow processor
     * @param externalPortProcessor the external I/O port processor
     * @param participant the compiler participant
     */
    public ToolRepository(
            DataModelProcessor dataModelProcessor,
            BatchProcessor batchProcessor,
            JobflowProcessor jobflowProcessor,
            ExternalPortProcessor externalPortProcessor,
            CompilerParticipant participant) {
        this.dataModelProcessor = dataModelProcessor;
        this.batchProcessor = batchProcessor;
        this.jobflowProcessor = jobflowProcessor;
        this.externalPortProcessor = externalPortProcessor;
        this.participant = participant;
    }

    /**
     * Creates a new builder.
     * @param serviceClassLoader the service class loader
     * @return the created builder
     */
    public static Builder builder(ClassLoader serviceClassLoader) {
        return new Builder(serviceClassLoader);
    }

    /**
     * Returns the data model processor.
     * @return the data model processor
     */
    public DataModelProcessor getDataModelProcessor() {
        return dataModelProcessor;
    }

    /**
     * Returns the batch processor.
     * @return the batch processor
     */
    public BatchProcessor getBatchProcessor() {
        return batchProcessor;
    }

    /**
     * Returns the jobflow processor.
     * @return the jobflow processor
     */
    public JobflowProcessor getJobflowProcessor() {
        return jobflowProcessor;
    }

    /**
     * Returns the external I/O processor.
     * @return the external I/O processor
     */
    public ExternalPortProcessor getExternalPortProcessor() {
        return externalPortProcessor;
    }

    /**
     * Returns the compiler participant.
     * @return the compiler participant
     */
    public CompilerParticipant getParticipant() {
        return participant;
    }

    /**
     * A builder for {@link ToolRepository}.
     */
    public static class Builder {

        private static final Set<Class<?>> ALL_TOOLS;
        private static final Set<Class<?>> MANDATORY_TOOLS;
        static {
            Set<Class<?>> all = new LinkedHashSet<>();
            all.add(DataModelProcessor.class);
            all.add(BatchProcessor.class);
            all.add(JobflowProcessor.class);
            all.add(ExternalPortProcessor.class);
            all.add(CompilerParticipant.class);

            Set<Class<?>> mandatory = new LinkedHashSet<>();
            mandatory.add(DataModelProcessor.class);
            mandatory.add(ExternalPortProcessor.class);

            ALL_TOOLS = Collections.unmodifiableSet(all);
            MANDATORY_TOOLS = Collections.unmodifiableSet(mandatory);
        }

        private final ClassLoader serviceClassLoader;

        private final Map<Class<?>, List<?>> tools;

        /**
         * Creates a new instance.
         * @param serviceClassLoader the service class loader
         */
        public Builder(ClassLoader serviceClassLoader) {
            this.serviceClassLoader = serviceClassLoader;
            Map<Class<?>, List<?>> map = new LinkedHashMap<>();
            for (Class<?> toolType : ALL_TOOLS) {
                map.put(toolType, new ArrayList<>());
            }
            this.tools = Collections.unmodifiableMap(map);
        }

        /**
         * Adds a {@link DataModelProcessor}.
         * @param tool the tool
         * @return this
         */
        public Builder use(DataModelProcessor tool) {
            use(DataModelProcessor.class, tool);
            return this;
        }

        /**
         * Adds a {@link BatchProcessor}.
         * @param tool the tool
         * @return this
         */
        public Builder use(BatchProcessor tool) {
            use(BatchProcessor.class, tool);
            return this;
        }

        /**
         * Adds a {@link JobflowProcessor}.
         * @param tool the tool
         * @return this
         */
        public Builder use(JobflowProcessor tool) {
            use(JobflowProcessor.class, tool);
            return this;
        }

        /**
         * Adds a {@link ExternalPortProcessor}.
         * @param tool the tool
         * @return this
         */
        public Builder use(ExternalPortProcessor tool) {
            use(ExternalPortProcessor.class, tool);
            return this;
        }

        /**
         * Adds a {@link CompilerParticipant}.
         * @param tool the tool
         * @return this
         */
        public Builder use(CompilerParticipant tool) {
            use(CompilerParticipant.class, tool);
            return this;
        }

        /**
         * Adds default tools about the target type.
         * @param toolType the target tool type (e.g. {@link JobflowProcessor JobflowProcessor.class}})
         * @return this
         * @see #useDefaults()
         */
        public Builder useDefaults(Class<?> toolType) {
            return useDefaults(Collections.singleton(toolType));
        }

        /**
         * Adds default tools about the target types.
         * @param toolTypes the target tool types (e.g. {@link JobflowProcessor JobflowProcessor.class}})
         * @return this
         * @see #useDefaults()
         */
        public Builder useDefaults(Collection<? extends Class<?>> toolTypes) {
            for (Class<?> aClass : toolTypes) {
                useDefaults0(aClass);
            }
            return this;
        }

        /**
         * Adds default tools for all available tool types.
         * @return this
         * @see #useDefaults(Collection)
         */
        public Builder useDefaults() {
            return useDefaults(ALL_TOOLS);
        }

        private <T> void useDefaults0(Class<T> toolType) {
            List<T> elements = get(toolType);
            Set<Class<?>> saw = new HashSet<>();
            // avoid duplication
            for (T element : elements) {
                saw.add(element.getClass());
            }
            for (T service : ServiceLoader.load(toolType, serviceClassLoader)) {
                if (saw.contains(service.getClass())) {
                    LOG.debug("filter duplicate tool: {}", service.getClass().getName()); //$NON-NLS-1$
                    continue;
                }
                elements.add(service);
            }
        }

        private <T> void use(Class<T> toolType, T tool) {
            get(toolType).add(tool);
        }

        private <T> List<T> get(Class<T> toolType) {
            @SuppressWarnings("unchecked")
            List<T> elements = (List<T>) tools.get(toolType);
            if (elements == null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "unknown tool type: {0}",
                        toolType.getName()));
            }
            return elements;
        }

        /**
         * Builds {@link ToolRepository} from added tools.
         * @return the built object
         */
        public ToolRepository build() {
            checkMandatory();
            return new ToolRepository(
                    CompositeDataModelProcessor.composite(resolve(DataModelProcessor.class)),
                    CompositeBatchProcessor.composite(resolve(BatchProcessor.class)),
                    CompositeJobflowProcessor.composite(resolve(JobflowProcessor.class)),
                    CompositeExternalPortProcessor.composite(resolve(ExternalPortProcessor.class)),
                    CompositeCompilerParticipant.composite(resolve(CompilerParticipant.class)));
        }

        private void checkMandatory() {
            for (Class<?> toolType : MANDATORY_TOOLS) {
                List<?> elements = get(toolType);
                if (elements.isEmpty()) {
                    throw new IllegalStateException(MessageFormat.format(
                            "{0} must be specified",
                            toolType.getSimpleName()));
                }
            }
        }

        private <T> List<T> resolve(Class<T> type) {
            List<T> elements = get(type);
            if (elements.size() <= 1) {
                return elements;
            }
            BitSet exclusives = new BitSet(elements.size());
            BitSet optionals = new BitSet(elements.size());
            for (int i = 0, n = elements.size(); i < n; i++) {
                Class<?> aClass = elements.get(i).getClass();
                Exclusive annotation = aClass.getAnnotation(Exclusive.class);
                if (annotation != null) {
                    exclusives.set(i);
                    optionals.set(i, annotation.optional());
                }
            }
            if (exclusives.cardinality() <= 1) {
                // always valid
                return elements;
            }
            if (exclusives.cardinality() == optionals.cardinality()) {
                // all elements are optional, then we keep the first one
                assert optionals.cardinality() > 0;
                optionals.clear(optionals.nextSetBit(0));
            }

            BitSet mandatory = (BitSet) exclusives.clone();
            mandatory.andNot(optionals);
            if (mandatory.cardinality() >= 2) {
                // conflict some exclusive elements
                List<T> conflict = new ArrayList<>();
                for (int i = mandatory.nextSetBit(0); i >= 0; i = mandatory.nextSetBit(i + 1)) {
                    T element = elements.get(i);
                    conflict.add(element);
                }
                throw new IllegalStateException(MessageFormat.format(
                        "found multiple exclusive tools: {0}",
                        conflict));
            }

            List<T> results = new ArrayList<>();
            for (int i = 0, n = elements.size(); i < n; i++) {
                T element = elements.get(i);
                if (optionals.get(i)) {
                    LOG.info(MessageFormat.format(
                            "disabled optional tool: {0}",
                            element));
                } else {
                    results.add(element);
                }
            }
            return results;
        }
    }
}
