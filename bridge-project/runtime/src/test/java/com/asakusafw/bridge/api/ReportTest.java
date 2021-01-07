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
package com.asakusafw.bridge.api;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceBrokerContext;
import com.asakusafw.runtime.core.Report;
import com.asakusafw.runtime.core.Report.Delegate;
import com.asakusafw.runtime.core.Report.FailedException;
import com.asakusafw.runtime.core.Report.Level;
import com.asakusafw.runtime.core.ResourceConfiguration;

/**
 * Test for {@link com.asakusafw.bridge.api.Report}.
 */
public class ReportTest {

    /**
     * setup/cleanup the test case.
     */
    @Rule
    public final ResourceBrokerContext brokerContext = new ResourceBrokerContext(true);

    /**
     * initializes tracer.
     */
    @Rule
    public final ExternalResource initializer = new ExternalResource() {

        @Override
        protected void before() throws Throwable {
            reset();
        }

        @Override
        protected void after() {
            reset();
        }

        private void reset() {
            Tracer.lastLevel = null;
            Tracer.lastMessage = null;
            Tracer.lastThrowable = null;
        }
    };

    private void configure(Class<? extends Delegate> aClass) {
        ResourceConfiguration conf = new MapConfiguration();
        conf.set(ReportAdapter.CLASS_DELEGATE, aClass.getName());
        ResourceBroker.put(ResourceConfiguration.class, conf);
        brokerContext.activateApis();
    }

    /**
     * info.
     */
    @Test
    public void trace_info() {
        configure(Tracer.class);
        Report.info("testing");
        assertThat(Tracer.lastLevel, is(Level.INFO));
        assertThat(Tracer.lastMessage, is("testing"));
        assertThat(Tracer.lastThrowable, is(nullValue()));
    }

    /**
     * info w/ exception.
     */
    @Test
    public void trace_info_exception() {
        configure(Tracer.class);
        Report.info("testing", new UnsupportedOperationException());
        assertThat(Tracer.lastLevel, is(Level.INFO));
        assertThat(Tracer.lastMessage, is("testing"));
        assertThat(Tracer.lastThrowable, is(instanceOf(UnsupportedOperationException.class)));
    }

    /**
     * warn.
     */
    @Test
    public void trace_warn() {
        configure(Tracer.class);
        Report.warn("testing");
        assertThat(Tracer.lastLevel, is(Level.WARN));
        assertThat(Tracer.lastMessage, is("testing"));
        assertThat(Tracer.lastThrowable, is(nullValue()));
    }

    /**
     * warn w/ exception.
     */
    @Test
    public void trace_warn_exception() {
        configure(Tracer.class);
        Report.warn("testing", new UnsupportedOperationException());
        assertThat(Tracer.lastLevel, is(Level.WARN));
        assertThat(Tracer.lastMessage, is("testing"));
        assertThat(Tracer.lastThrowable, is(instanceOf(UnsupportedOperationException.class)));
    }

    /**
     * error.
     */
    @Test
    public void trace_error() {
        configure(Tracer.class);
        Report.error("testing");
        assertThat(Tracer.lastLevel, is(Level.ERROR));
        assertThat(Tracer.lastMessage, is("testing"));
        assertThat(Tracer.lastThrowable, is(nullValue()));
    }

    /**
     * error w/ exception.
     */
    @Test
    public void trace_error_exception() {
        configure(Tracer.class);
        Report.error("testing", new UnsupportedOperationException());
        assertThat(Tracer.lastLevel, is(Level.ERROR));
        assertThat(Tracer.lastMessage, is("testing"));
        assertThat(Tracer.lastThrowable, is(instanceOf(UnsupportedOperationException.class)));
    }

    /**
     * info.
     */
    @Test(expected = FailedException.class)
    public void fail_info() {
        configure(Raiser.class);
        Report.info("testing");
    }

    /**
     * info w/ exception.
     */
    @Test(expected = FailedException.class)
    public void fail_info_exception() {
        configure(Raiser.class);
        Report.info("testing", new UnsupportedOperationException());
    }

    /**
     * warn.
     */
    @Test(expected = FailedException.class)
    public void fail_warn() {
        configure(Raiser.class);
        Report.warn("testing");
    }

    /**
     * warn w/ exception.
     */
    @Test(expected = FailedException.class)
    public void fail_warn_exception() {
        configure(Raiser.class);
        Report.warn("testing", new UnsupportedOperationException());
    }

    /**
     * error.
     */
    @Test(expected = FailedException.class)
    public void fail_error() {
        configure(Raiser.class);
        Report.error("testing");
    }

    /**
     * error w/ exception.
     */
    @Test(expected = FailedException.class)
    public void fail_error_exception() {
        configure(Raiser.class);
        Report.error("testing", new UnsupportedOperationException());
    }

    /**
     * Traces report API.
     */
    public static final class Tracer extends Delegate {

        static Level lastLevel;

        static String lastMessage;

        static Throwable lastThrowable;

        @Override
        public void report(Level level, String message) throws IOException {
            report(level, message, null);
        }

        @Override
        public void report(Level level, String message, Throwable throwable) throws IOException {
            lastLevel = level;
            lastMessage = message;
            lastThrowable = throwable;
        }
    }

    /**
     * Raises error.
     */
    public static final class Raiser extends Delegate {

        @Override
        public void report(Level level, String message) throws IOException {
            report(level, message, null);
        }

        @Override
        public void report(Level level, String message, Throwable throwable) throws IOException {
            throw new IOException(message, throwable);
        }
    }
}
