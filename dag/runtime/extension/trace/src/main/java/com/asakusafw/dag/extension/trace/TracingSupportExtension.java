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
package com.asakusafw.dag.extension.trace;

import java.lang.reflect.Constructor;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.Reportable;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextDecorator;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * Enables edge I/O tracing support.
 * @since 0.4.0
 */
public class TracingSupportExtension implements ProcessorContextExtension {

    /**
     * The property key of {@link PortTracer} implementation class.
     */
    public static final String KEY_IMPLEMENTATION = "com.asakusafw.dag.extension.trace"; //$NON-NLS-1$

    static final Logger LOG = LoggerFactory.getLogger(TracingSupportExtension.class);

    @Override
    public InterruptibleIo install(ProcessorContext context, ProcessorContext.Editor editor) {
        String className = context.getProperty(KEY_IMPLEMENTATION).orElse(null);
        if (className == null) {
            LOG.debug("tracing is disabled");
            return null;
        }
        LOG.debug("tracing implementation: {}", className);
        PortTracer loaded = load(context, className);
        if (loaded == null) {
            return null;
        }

        LOG.debug("enable tracing: {}", className);
        editor.addResource(ProcessorContextDecorator.class, new TracingProcessorContextDecorator(loaded));
        if (loaded instanceof Reportable) {
            LOG.debug("reporting enabled: {}", className);
            return () -> ((Reportable) loaded).report();
        } else {
            return null;
        }
    }

    private PortTracer load(ProcessorContext context, String name) {
        try {
            Class<? extends PortTracer> aClass = context.getClassLoader()
                    .loadClass(name)
                    .asSubclass(PortTracer.class);
            try {
                Constructor<? extends PortTracer> constructor = aClass.getConstructor(ProcessorContext.class);
                return constructor.newInstance(context);
            } catch (ReflectiveOperationException innter) {
                LOG.debug("missing constructor with ProcessorContext: {}", aClass);
            }
            return aClass.newInstance();
        } catch (ClassCastException | ReflectiveOperationException e) {
            LOG.warn(MessageFormat.format(
                    "error occurred while loading tracing implementation: {0}",
                    name), e);
            return null;
        }
    }
}
