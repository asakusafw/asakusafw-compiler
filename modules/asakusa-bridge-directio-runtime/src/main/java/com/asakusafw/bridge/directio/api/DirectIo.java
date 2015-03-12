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
 * {@link ResourceConfiguration a Asakusa configuration} object has been registered to {@link ResourceBroker}.
 * </p>
 */
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
        if (asakusa != null && asakusa instanceof Configurable) {
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
