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
package com.asakusafw.lang.compiler.extension.hive;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.directio.hive.info.InputInfo;
import com.asakusafw.directio.hive.info.LocationInfo;
import com.asakusafw.directio.hive.info.OutputInfo;
import com.asakusafw.directio.hive.info.TableInfo;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.extension.directio.DirectFileInputModel;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoModels;
import com.asakusafw.lang.compiler.extension.directio.DirectFileOutputModel;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Collects Hive I/O schema information from jobflows.
 * @since 0.3.1
 */
public class HiveSchemaCollectorProcessor implements JobflowProcessor {

    static final Logger LOG = LoggerFactory.getLogger(HiveSchemaCollectorProcessor.class);

    private static final Location PATH_BASE = Location.of("META-INF/asakusafw-compiler/hive-schema"); //$NON-NLS-1$

    /**
     * The output path of input schema information.
     */
    public static final Location PATH_INPUT = PATH_BASE.append("input.json"); //$NON-NLS-1$

    /**
     * The output path of output schema information.
     */
    public static final Location PATH_OUTPUT = PATH_BASE.append("output.json"); //$NON-NLS-1$

    @Override
    public void process(Context context, Jobflow source) throws IOException {
        ClassLoader loader = context.getClassLoader();
        if (Util.isAvailable(loader) == false) {
            return;
        }
        LOG.debug("collecting Hive inputs: {}", source.getFlowId());
        List<InputInfo> inputs = collectInputs(loader, source.getOperatorGraph().getInputs().values());
        List<OutputInfo> outputs = collectOutputs(loader, source.getOperatorGraph().getOutputs().values());
        LOG.debug("generating Hive input table schema: {} entries", inputs.size());
        try (OutputStream stream = context.addResourceFile(PATH_INPUT)) {
            Persistent.write(InputInfo.class, inputs, stream);
        }
        LOG.debug("generating Hive output table schema: {} entries", outputs.size());
        try (OutputStream stream = context.addResourceFile(PATH_OUTPUT)) {
            Persistent.write(OutputInfo.class, outputs, stream);
        }
    }

    private List<InputInfo> collectInputs(ClassLoader classLoader, Collection<ExternalInput> elements) {
        List<InputInfo> results = new ArrayList<>();
        for (ExternalInput port : elements) {
            InputInfo schema = extract(classLoader, port);
            if (schema != null) {
                LOG.debug("found hive schema info: {}=>{}", port.getInfo(), schema);
                results.add(schema);
            }
        }
        return Util.normalize(results);
    }

    private List<OutputInfo> collectOutputs(ClassLoader classLoader, Collection<ExternalOutput> elements) {
        List<OutputInfo> results = new ArrayList<>();
        for (ExternalOutput port : elements) {
            OutputInfo schema = extract(classLoader, port);
            if (schema != null) {
                LOG.debug("found hive schema info: {}=>{}", port.getInfo(), schema);
                results.add(schema);
            }
        }
        return Util.normalize(results);
    }

    private InputInfo extract(ClassLoader classLoader, ExternalInput port) {
        if (port.isExternal() == false) {
            LOG.trace("not external: {}", port);
            return null;
        }
        ExternalInputInfo info = port.getInfo();
        if (DirectFileIoModels.isSupported(info) == false) {
            LOG.trace("not direct I/O: {}", info);
            return null;
        }
        try {
            DirectFileInputModel model = DirectFileIoModels.resolve(info);
            Object format = model.getFormatClass()
                    .resolve(classLoader)
                    .newInstance();
            if ((format instanceof TableInfo.Provider) == false) {
                LOG.trace("not hive data format: {}", info);
                return null;
            }
            return new InputInfo(
                    new LocationInfo(model.getBasePath(), model.getResourcePattern()),
                    ((TableInfo.Provider) format).getSchema());
        } catch (IllegalArgumentException | ReflectiveOperationException e) {
            LOG.debug("failed to resolve external port model: {}", info, e);
            return null;
        }
    }

    private OutputInfo extract(ClassLoader classLoader, ExternalOutput port) {
        if (port.isExternal() == false) {
            LOG.trace("not external: {}", port);
            return null;
        }
        ExternalOutputInfo info = port.getInfo();
        if (DirectFileIoModels.isSupported(info) == false) {
            LOG.trace("not direct I/O: {}", info);
            return null;
        }
        try {
            DirectFileOutputModel model = DirectFileIoModels.resolve(info);
            Object format = model.getFormatClass()
                    .resolve(classLoader)
                    .newInstance();
            if ((format instanceof TableInfo.Provider) == false) {
                LOG.trace("not hive data format: {}", info);
                return null;
            }
            return new OutputInfo(
                    new LocationInfo(model.getBasePath(), model.getResourcePattern()),
                    ((TableInfo.Provider) format).getSchema());
        } catch (ReflectiveOperationException e) {
            LOG.debug("failed to resolve external port model: {}", info, e);
            return null;
        }
    }
}
