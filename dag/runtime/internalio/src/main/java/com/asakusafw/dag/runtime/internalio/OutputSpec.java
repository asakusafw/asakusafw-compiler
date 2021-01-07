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
package com.asakusafw.dag.runtime.internalio;

import org.apache.hadoop.io.Writable;

class OutputSpec {

    final String id;

    final String pathPrefix;

    final Class<? extends Writable> dataClass;

    OutputSpec(String id, String pathPrefix, Class<? extends Writable> dataClass) {
        this.id = id;
        this.pathPrefix = pathPrefix;
        this.dataClass = dataClass;
    }
}
