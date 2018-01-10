/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.directio.info;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.info.directio.DirectFileInputInfo;
import com.asakusafw.info.directio.DirectFileIoAttribute;
import com.asakusafw.info.directio.DirectFileOutputInfo;
import com.asakusafw.lang.compiler.extension.directio.DirectFileInputModel;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoConstants;
import com.asakusafw.lang.compiler.extension.directio.DirectFileOutputModel;
import com.asakusafw.lang.compiler.info.AttributeCollector;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * An implementation of {@link AttributeCollector} for Direct I/O files.
 * @since 0.4.1
 */
public class DirectFileIoAttributeCollector implements AttributeCollector {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileIoAttributeCollector.class);

    @Override
    public void process(Context context, Jobflow jobflow) {
        List<DirectFileInputInfo> inputs = collectInputs(jobflow);
        List<DirectFileOutputInfo> outputs = collectOutputs(jobflow);
        if (inputs.isEmpty() == false || outputs.isEmpty() == false) {
            context.putAttribute(new DirectFileIoAttribute(inputs, outputs));
        }
    }

    private static List<DirectFileInputInfo> collectInputs(Jobflow jobflow) {
        return jobflow.getOperatorGraph().getInputs().values().stream()
            .filter(ExternalPort::isExternal)
            .filter(p -> p.getInfo().getModuleName().equals(DirectFileIoConstants.MODULE_NAME))
            .flatMap(p -> {
                ClassLoader classLoader = DirectFileInputModel.class.getClassLoader();
                ExternalInputInfo info = p.getInfo();
                try {
                    DirectFileInputModel model = (DirectFileInputModel) info.getContents().resolve(classLoader);
                    return Stream.of(new DirectFileInputInfo(
                            p.getName(),
                            name(info.getDescriptionClass()),
                            model.getBasePath(),
                            model.getResourcePattern(),
                            info.getDataModelClass().getName(),
                            name(model.getFormatClass()),
                            name(model.getFilterClass()),
                            model.isOptional()));
                } catch (ReflectiveOperationException e) {
                    LOG.debug("error occurred while building DirectFileInputInfo: {}",
                            info.getDescriptionClass().getClassName(),
                            e);
                    return Stream.empty();
                }
            })
            .sorted(Comparator.nullsFirst(Comparator.comparing(DirectFileInputInfo::getName)))
            .collect(Collectors.toList());
    }

    private static List<DirectFileOutputInfo> collectOutputs(Jobflow jobflow) {
        return jobflow.getOperatorGraph().getOutputs().values().stream()
                .filter(ExternalPort::isExternal)
                .filter(p -> p.getInfo().getModuleName().equals(DirectFileIoConstants.MODULE_NAME))
                .flatMap(p -> {
                    ClassLoader classLoader = DirectFileOutputModel.class.getClassLoader();
                    ExternalOutputInfo info = p.getInfo();
                    try {
                        DirectFileOutputModel model = (DirectFileOutputModel) info.getContents().resolve(classLoader);
                        return Stream.of(new DirectFileOutputInfo(
                                p.getName(),
                                name(info.getDescriptionClass()),
                                model.getBasePath(),
                                model.getResourcePattern(),
                                info.getDataModelClass().getName(),
                                name(model.getFormatClass()),
                                model.getOrder(),
                                model.getDeletePatterns()));
                    } catch (ReflectiveOperationException e) {
                        LOG.debug("error occurred while building DirectFileOutputInfo: {}",
                                info.getDescriptionClass().getClassName(),
                                e);
                        return Stream.empty();
                    }
                })
                .sorted(Comparator.nullsFirst(Comparator.comparing(DirectFileOutputInfo::getName)))
                .collect(Collectors.toList());
    }

    static String name(ClassDescription aClass) {
        return Optional.ofNullable(aClass)
                .map(ClassDescription::getBinaryName)
                .orElse(null);
    }
}
