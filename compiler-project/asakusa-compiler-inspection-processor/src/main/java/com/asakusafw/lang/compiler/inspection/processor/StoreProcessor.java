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
package com.asakusafw.lang.compiler.inspection.processor;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.InspectionNodeRepository;
import com.asakusafw.lang.compiler.inspection.json.JsonInspectionNodeRepository;

/**
 * Just copy {@link InspectionNode}.
 */
public class StoreProcessor implements InspectionNodeProcessor {

    private final InspectionNodeRepository repository;

    /**
     * Creates a new instance.
     */
    public StoreProcessor() {
        this(new JsonInspectionNodeRepository());
    }

    /**
     * Creates a new instance.
     * @param repository the repository
     */
    public StoreProcessor(InspectionNodeRepository repository) {
        this.repository = repository;
    }

    @Override
    public void process(Context context, InspectionNode node, OutputStream output) throws IOException {
        repository.store(output, node);
    }
}
