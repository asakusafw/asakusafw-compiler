/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.tester.executor.util;

import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.JobFlow;

/**
 * A dummy jobflow class.
 * @since 0.3.1
 */
@JobFlow(name = DummyJobflowClass.ID)
public class DummyJobflowClass extends FlowDescription {

    /**
     * The batch ID for this class.
     */
    public static final String ID = "dummy"; //$NON-NLS-1$

    @Override
    protected void describe() {
        throw new UnsupportedOperationException();
    }
}
