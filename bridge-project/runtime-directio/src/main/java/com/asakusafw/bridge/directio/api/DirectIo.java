/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.bridge.directio.api;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.runtime.core.ResourceConfiguration;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.api.DirectIoDelegate;
import com.asakusafw.runtime.io.ModelInput;

/**
 * A framework API for accessing Direct I/O.
 * <p>
 * Clients can use this class <em>only in operator methods</em>.
 * </p>
 *
 * <h3> requirements </h3>
 * <p>
 * This API requires that either {@link Configuration a Hadoop configuration} object or
 * {@link ResourceConfiguration Asakusa configuration} object has been registered to {@link ResourceBroker}.
 * </p>
 * @deprecated this API is not for application developers,
 *      please use {@link com.asakusafw.runtime.directio.api.DirectIo} instead
 */
@Deprecated
public final class DirectIo {

    private DirectIo() {
        return;
    }

    /**
     * Returns data model objects from Direct I/O data sources.
     * <p>
     * Clients can obtain each data model object:
     * </p>
<pre><code>
try (ModelInput&lt;Hoge&gt; input = DirectIo.open(...)) {
    Hoge object = new Hoge();
    while (input.readTo(object)) {
        // process object
        System.out.println(object);
    }
}
</code></pre>
     * , or can build a list of data model objects:
<pre><code>
List&lt;Hoge&gt; list = new ArrayList&lt;&gt;();
try (ModelInput&lt;Hoge&gt; input = DirectIo.open(...)) {
    while (true) {
        // create a new object in each iteration
        Hoge object = new Hoge();
        if (!input.readTo(object)) {
            break;
        }
        list.add(object);
    }
}
</code></pre>
     * @param <T> the data model object type
     * @param formatClass the Direct I/O data format class
     * @param basePath the base path (must not contain variables)
     * @param resourcePattern the resource pattern (must not contain variables)
     * @return the data model objects
     * @throws IOException if failed to open data model objects on the data source
     */
    public static <T> ModelInput<T> open(
            Class<? extends DataFormat<T>> formatClass,
            String basePath,
            String resourcePattern) throws IOException {
        DirectIoDelegate delegate = getDelegate();
        return delegate.open(formatClass, basePath, resourcePattern);
    }

    private static DirectIoDelegate getDelegate() {
        Configuration conf = getHadoopConfiguration();
        assert conf != null;
        return new DirectIoDelegate(conf);
    }

    private static Configuration getHadoopConfiguration() {
        ResourceConfiguration asakusa = ResourceBroker.find(ResourceConfiguration.class);
        if (asakusa instanceof Configurable) {
            Configuration conf = ((Configurable) asakusa).getConf();
            if (conf != null) {
                return conf;
            }
        }
        Configuration hadoop = ResourceBroker.find(Configuration.class);
        if (hadoop != null) {
            return hadoop;
        }
        throw new IllegalStateException(MessageFormat.format(
                "required resource has not been prepared yet: {0}",
                Configuration.class));
    }
}
