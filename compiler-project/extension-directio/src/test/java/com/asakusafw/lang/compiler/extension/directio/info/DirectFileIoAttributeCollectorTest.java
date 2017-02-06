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
package com.asakusafw.lang.compiler.extension.directio.info;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.lang.compiler.extension.directio.DirectFileInputModel;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoConstants;
import com.asakusafw.lang.compiler.extension.directio.DirectFileOutputModel;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.info.api.MockAttributeCollectorContext;
import com.asakusafw.lang.info.directio.DirectFileInputInfo;
import com.asakusafw.lang.info.directio.DirectFileIoAttribute;
import com.asakusafw.lang.info.directio.DirectFileOutputInfo;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.vocabulary.directio.DirectFileInputDescription;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;

/**
 * Test for {@link DirectFileIoAttributeCollector}.
 */
public class DirectFileIoAttributeCollectorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ExternalInput input = ExternalInput.newInstance("in", new ExternalInputInfo.Basic(
                new ClassDescription("com.example.InputDesc"),
                DirectFileIoConstants.MODULE_NAME,
                Descriptions.classOf(MockData.class),
                ExternalInputInfo.DataSize.UNKNOWN,
                Descriptions.valueOf(new DirectFileInputModel(new DirectFileInputDescription() {
                    @Override
                    public Class<?> getModelType() {
                        return MockData.class;
                    }
                    @Override
                    public String getBasePath() {
                        return "base/input";
                    }
                    @Override
                    public String getResourcePattern() {
                        return "example/resource/*";
                    }
                    @Override
                    public Class<? extends DataFormat<?>> getFormat() {
                        return MockDataFormat.class;
                    }
                }))));
        ExternalOutput output = ExternalOutput.newInstance("out", new ExternalOutputInfo.Basic(
                new ClassDescription("com.example.OutputDesc"),
                DirectFileIoConstants.MODULE_NAME,
                Descriptions.classOf(MockData.class),
                Descriptions.valueOf(new DirectFileOutputModel(new DirectFileOutputDescription() {
                    @Override
                    public Class<?> getModelType() {
                        return MockData.class;
                    }
                    @Override
                    public String getBasePath() {
                        return "base/output";
                    }
                    @Override
                    public String getResourcePattern() {
                        return "example/resource/*";
                    }
                    @Override
                    public Class<? extends DataFormat<?>> getFormat() {
                        return MockDataFormat.class;
                    }
                }))));
        OperatorGraph graph = new OperatorGraph();
        graph.add(input);
        graph.add(output);
        input.getOperatorPort().connect(output.getOperatorPort());

        Jobflow model = new Jobflow(
                "testing",
                new ClassDescription("com.example.ExampleFlow"),
                graph);
        DirectFileIoAttribute info = new DirectFileIoAttribute(
                Arrays.asList(new DirectFileInputInfo(
                        input.getName(),
                        "com.example.InputDesc",
                        "base/input",
                        "example/resource/*",
                        MockData.class.getName(),
                        MockDataFormat.class.getName(),
                        null,
                        false)),
                Arrays.asList(new DirectFileOutputInfo(
                        output.getName(),
                        "com.example.OutputDesc",
                        "base/output",
                        "example/resource/*",
                        MockData.class.getName(),
                        MockDataFormat.class.getName(),
                        null,
                        null)));
        assertThat(
                new MockAttributeCollectorContext().collect(model, new DirectFileIoAttributeCollector()),
                contains(info));
    }
}
