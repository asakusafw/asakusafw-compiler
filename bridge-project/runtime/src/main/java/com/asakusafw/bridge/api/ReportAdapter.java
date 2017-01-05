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
package com.asakusafw.bridge.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.MessageFormat;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceCacheStorage;
import com.asakusafw.runtime.core.Report.Default;
import com.asakusafw.runtime.core.Report.Delegate;
import com.asakusafw.runtime.core.ResourceConfiguration;

/**
 * An adapter implementation of core Report API.
 * @since 0.1.1
 * @version 0.3.1
 */
final class ReportAdapter implements Closeable {

    static final Logger LOG = LoggerFactory.getLogger(ReportAdapter.class);

    static final String CLASS_DELEGATE = "com.asakusafw.runtime.core.Report.Delegate"; //$NON-NLS-1$

    private static final ResourceCacheStorage<Delegate> CACHE = new ResourceCacheStorage<>();

    private static final Callable<ReportAdapter> SUPPLIER = new Callable<ReportAdapter>() {

        @Override
        public ReportAdapter call() throws Exception {
            ResourceConfiguration conf = ResourceBroker.find(ResourceConfiguration.class);
            Delegate implementation = null;
            if (conf != null) {
                String name = conf.get(CLASS_DELEGATE, null);
                if (name != null) {
                    try {
                        Class<?> aClass = conf.getClassLoader().loadClass(name);
                        if (Delegate.class.isAssignableFrom(aClass)) {
                            implementation = aClass.asSubclass(Delegate.class).newInstance();
                        } else {
                            LOG.error(MessageFormat.format(
                                    "implementation of report API must be a subtype of {2}: {0}={1}",
                                    CLASS_DELEGATE, name,
                                    Delegate.class.getName()));
                        }
                    } catch (ReflectiveOperationException e) {
                        LOG.error(MessageFormat.format(
                                "failed to initialize report API: {0}={1}",
                                CLASS_DELEGATE, name), e);
                    }
                }
            }
            if (implementation == null) {
                LOG.warn("Report API has not been initialized yet");
                implementation = new Default();
            }
            try {
                implementation.setup(conf);
            } catch (InterruptedException e) {
                throw (IOException) new InterruptedIOException().initCause(e);
            }
            ReportAdapter adapter = new ReportAdapter(conf, implementation);
            ResourceBroker.put(ReportAdapter.class, adapter);
            return adapter;
        }
    };

    private final ResourceConfiguration configuration;

    private final Delegate delegate;

    private boolean closed = false;

    /**
     * Creates a new instance.
     * @param configuration the report configuration
     * @param delegate the delegation target
     */
    ReportAdapter(ResourceConfiguration configuration, Delegate delegate) {
        this.configuration = configuration;
        this.delegate = delegate;
    }

    /**
     * Returns the delegate target in the current resource session.
     * @return the delegate target
     * @throws IllegalStateException if resource session has not been started yet
     */
    public static Delegate delegate() {
        return CACHE.get(() -> ResourceBroker.get(ReportAdapter.class, SUPPLIER).delegate);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            delegate.cleanup(configuration);
            closed = true;
        } catch (InterruptedException e) {
            throw (IOException) new InterruptedIOException().initCause(e);
        }
    }
}
