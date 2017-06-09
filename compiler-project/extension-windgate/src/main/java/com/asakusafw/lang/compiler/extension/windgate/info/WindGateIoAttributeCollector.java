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
package com.asakusafw.lang.compiler.extension.windgate.info;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.extension.windgate.DescriptionModel;
import com.asakusafw.lang.compiler.extension.windgate.WindGateConstants;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;
import com.asakusafw.lang.info.api.AttributeCollector;
import com.asakusafw.lang.info.windgate.WindGateInputInfo;
import com.asakusafw.lang.info.windgate.WindGateIoAttribute;
import com.asakusafw.lang.info.windgate.WindGateOutputInfo;
import com.asakusafw.lang.info.windgate.WindGatePortInfo;
import com.asakusafw.windgate.core.DriverScript;

/**
 * An implementation of {@link AttributeCollector} for WindGate.
 * @since 0.4.2
 */
public class WindGateIoAttributeCollector implements AttributeCollector {

    static final Logger LOG = LoggerFactory.getLogger(WindGateIoAttributeCollector.class);

    @Override
    public void process(Context context, Jobflow jobflow) {
        OperatorGraph graph = jobflow.getOperatorGraph();
        List<WindGateInputInfo> inputs = collect(graph.getInputs().values(), WindGateInputInfo::new);
        List<WindGateOutputInfo> outputs = collect(graph.getOutputs().values(), WindGateOutputInfo::new);
        if (inputs.isEmpty() == false || outputs.isEmpty() == false) {
            context.putAttribute(new WindGateIoAttribute(inputs, outputs));
        }
    }

    private static <T extends WindGatePortInfo> List<T> collect(
            Collection<? extends ExternalPort> ports,
            InfoFactory<T> factory) {
        return ports.stream()
                .filter(ExternalPort::isExternal)
                .filter(p -> p.getInfo().getModuleName().equals(WindGateConstants.MODULE_NAME))
                .map(factory::convert)
                .flatMap(opt -> opt.map(Stream::of).orElse(Stream.empty()))
                .sorted(Comparator.nullsFirst(Comparator.comparing(WindGatePortInfo::getName)))
                .collect(Collectors.toList());
    }

    static String name(ClassDescription aClass) {
        return Optional.ofNullable(aClass)
                .map(ClassDescription::getBinaryName)
                .orElse(null);
    }

    @FunctionalInterface
    private interface InfoFactory<T> {

        T newInstance(
                String name,
                String profileName,
                String resourceName,
                Map<String, String> configuration);

        default Optional<T> convert(ExternalPort port) {
            ClassLoader classLoader = DescriptionModel.class.getClassLoader();
            ExternalPortInfo info = port.getInfo();
            try {
                DescriptionModel model = (DescriptionModel) info.getContents().resolve(classLoader);
                DriverScript script = model.getDriverScript();
                return Optional.of(newInstance(
                        port.getName(),
                        model.getProfileName(),
                        script.getResourceName(),
                        script.getConfiguration()));
            } catch (ReflectiveOperationException e) {
                LOG.debug("error occurred while building information model: {}",
                        info.getDescriptionClass().getClassName(),
                        e);
                return Optional.empty();
            }
        }
    }
}
