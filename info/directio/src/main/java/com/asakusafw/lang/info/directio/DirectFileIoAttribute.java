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
package com.asakusafw.lang.info.directio;

import java.util.Collection;
import java.util.List;

import com.asakusafw.lang.info.Attribute;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An attribute of Direct I/O.
 * @since 0.4.1
 */
public class DirectFileIoAttribute implements Attribute {

    /**
     * The attribute ID.
     */
    public static final String ID = "directio";

    private final List<? extends DirectFileInputInfo> inputs;

    private final List<? extends DirectFileOutputInfo> outputs;

    /**
     * Creates a new instance.
     * @param inputs the input ports
     * @param outputs the output ports
     */
    public DirectFileIoAttribute(
            @JsonProperty("inputs") Collection<? extends DirectFileInputInfo> inputs,
            @JsonProperty("outputs")Collection<? extends DirectFileOutputInfo> outputs) {
        this.inputs = Util.freeze(inputs);
        this.outputs = Util.freeze(outputs);
    }

    @Override
    public String getId() {
        return ID;
    }

    /**
     * Returns the inputs.
     * @return the inputs
     */
    @JsonProperty("inputs")
    public List<? extends DirectFileInputInfo> getInputs() {
        return inputs;
    }

    /**
     * Returns the outputs.
     * @return the outputs
     */
    @JsonProperty("outputs")
    public List<? extends DirectFileOutputInfo> getOutputs() {
        return outputs;
    }
}
