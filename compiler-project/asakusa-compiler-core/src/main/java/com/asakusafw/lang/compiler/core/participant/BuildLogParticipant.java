/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.participant;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler.Context;
import com.asakusafw.lang.compiler.core.ToolRepository;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.core.util.CompositeElement;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.runtime.core.context.RuntimeContext;

/**
 * Logs building environment.
 */
public class BuildLogParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(BuildLogParticipant.class);

    static final Charset ENCODING = Charset.forName("UTF-8"); //$NON-NLS-1$

    /**
     * The name prefix of the target environment variables.
     */
    static final String PREFIX_ENV = "ASAKUSA_"; //$NON-NLS-1$

    /**
     * The key prefix of the target system properties.
     */
    static final String PREFIX_SYSPROP = "com.asakusafw."; //$NON-NLS-1$

    /**
     * The output file path.
     */
    static final Location LOCATION = Location.of("etc/build.log"); //$NON-NLS-1$

    @Override
    public void afterBatch(Context context, Batch batch, BatchReference reference) {
        LOG.debug("creating build log: {}", batch.getBatchId()); //$NON-NLS-1$
        try (Editor editor = new Editor(context.getOutput().addResource(LOCATION))) {
            perform(editor, context, batch);
        } catch (IOException e) {
            LOG.error(MessageFormat.format(
                    "failed to create build log: {0}",
                    context.getOutput().toFile(LOCATION)), e);
        }
    }

    private void perform(Editor editor, Context context, Batch batch) {
        // about application
        editor.put("application.batchId", batch.getBatchId()); //$NON-NLS-1$
        editor.put("application.batchClass", batch.getDescriptionClass().getClassName()); //$NON-NLS-1$

        // about environment
        editor.putAll("environment.", filter(System.getenv(), PREFIX_ENV)); //$NON-NLS-1$
        editor.putAll("property.", filter(System.getProperties(), PREFIX_SYSPROP)); //$NON-NLS-1$

        // about compiler
        editor.put("compiler.buildId", context.getOptions().getBuildId()); //$NON-NLS-1$
        editor.put("compiler.runtimeVersion", RuntimeContext.getRuntimeVersion()); //$NON-NLS-1$
        editor.putAll("compiler.option.", context.getOptions().getProperties()); //$NON-NLS-1$

        ToolRepository tools = context.getTools();
        editor.put("compiler.dataModelProcessor", inspect(tools.getDataModelProcessor())); //$NON-NLS-1$
        editor.put("compiler.externalPortProcessor", inspect(tools.getExternalPortProcessor())); //$NON-NLS-1$
        editor.put("compiler.jobflowProcessor", inspect(tools.getJobflowProcessor())); //$NON-NLS-1$
        editor.put("compiler.batchProcessor", inspect(tools.getBatchProcessor())); //$NON-NLS-1$
        editor.put("compiler.participant", inspect(tools.getParticipant())); //$NON-NLS-1$
    }

    private String inspect(Object element) {
        if (element == null) {
            return "N/A"; //$NON-NLS-1$
        }
        if (element instanceof CompositeElement<?>) {
            Collection<?> children = ((CompositeElement<?>) element).getElements();
            if (children.isEmpty()) {
                return "N/A"; //$NON-NLS-1$
            }
            List<String> results = new ArrayList<>();
            for (Object o : children) {
                results.add(inspect(o));
            }
            return results.toString();
        }
        String info = inspectInfo(element);
        File library = ResourceUtil.findLibraryByClass(element.getClass());
        if (library != null && library.isFile()) {
            return String.format("%s:%s", info, library.getName()); //$NON-NLS-1$
        }
        return info;
    }

    private String inspectInfo(Object element) {
        Class<?> aClass = element.getClass();
        try {
            // use toString() only if the target element has explicit one
            Method method = aClass.getMethod("toString");
            if (method.getDeclaringClass() != Object.class) {
                return element.toString();
            }
        } catch (NoSuchMethodException | SecurityException e) {
            LOG.debug("{} may not have explicit toString() method", aClass.getName());
        }
        return aClass.getSimpleName();
    }

    private SortedMap<String, String> filter(Map<?, ?> map, String prefix) {
        assert map != null;
        assert prefix != null;
        SortedMap<String, String> results = new TreeMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith(prefix)) {
                results.put(key, String.valueOf(entry.getValue()));
            }
        }
        return results;
    }

    private static class Editor implements Closeable {

        private final PrintWriter writer;

        public Editor(OutputStream output) {
            writer = new PrintWriter(new OutputStreamWriter(output, ENCODING));
        }

        void put(String key, Object value) {
            LOG.debug("build log: {} = {}", key, value); //$NON-NLS-1$
            writer.printf("%s = %s%n", key, value); //$NON-NLS-1$
        }

        void putAll(String prefix, Map<String, String> map) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                put(prefix + entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
