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
package com.asakusafw.lang.compiler.core.dummy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Mock {@link ExternalPortProcessor}.
 */
@SuppressWarnings("javadoc")
public class DummyExternalPortProcessor implements ExternalPortProcessor, DummyElement {

    final String id;

    public DummyExternalPortProcessor() {
        this("default");
    }

    public DummyExternalPortProcessor(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return false;
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getAdaper(AnalyzeContext context, Class<T> adapterType, Class<?> descriptionClass) {
        return null;
    }
}
