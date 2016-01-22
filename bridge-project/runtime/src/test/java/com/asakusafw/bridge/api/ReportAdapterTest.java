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
package com.asakusafw.bridge.api;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceBrokerContext;
import com.asakusafw.runtime.core.Report.Default;
import com.asakusafw.runtime.core.Report.Delegate;
import com.asakusafw.runtime.core.ResourceConfiguration;
import com.asakusafw.runtime.report.CommonsLoggingReport;

/**
 * Test for {@link ReportAdapter}.
 */
public class ReportAdapterTest {

    /**
     * setup/cleanup the test case.
     */
    @Rule
    public final ResourceBrokerContext brokerContext = new ResourceBrokerContext(true);

    /**
     * use default delegate.
     */
    @Test
    public void default_delegate() {
        Delegate delegate = ReportAdapter.delegate();
        assertThat(delegate, is(instanceOf(Default.class)));
    }

    /**
     * use custom delegate.
     */
    @Test
    public void custom_delegate() {
        ResourceConfiguration conf = new MapConfiguration();
        conf.set(ReportAdapter.CLASS_DELEGATE, CommonsLoggingReport.class.getName());
        ResourceBroker.put(ResourceConfiguration.class, conf);

        Delegate delegate = ReportAdapter.delegate();
        assertThat(delegate, is(instanceOf(CommonsLoggingReport.class)));
    }

    /**
     * use unknown delegate.
     */
    @Test
    public void unknown_delegate() {
        ResourceConfiguration conf = new MapConfiguration();
        conf.set(ReportAdapter.CLASS_DELEGATE, "__MISSING__");
        ResourceBroker.put(ResourceConfiguration.class, conf);

        Delegate delegate = ReportAdapter.delegate();
        assertThat(delegate, is(instanceOf(Default.class)));
    }

    /**
     * use incompatible delegate.
     */
    @Test
    public void incompatible_delegate() {
        ResourceConfiguration conf = new MapConfiguration();
        conf.set(ReportAdapter.CLASS_DELEGATE, String.class.getName());
        ResourceBroker.put(ResourceConfiguration.class, conf);

        Delegate delegate = ReportAdapter.delegate();
        assertThat(delegate, is(instanceOf(Default.class)));
    }
}
