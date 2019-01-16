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
package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;
import com.asakusafw.vocabulary.batch.Work;

@SuppressWarnings("javadoc")
@Batch(name = "MockDiamondBatch")
public class MockDiamondBatch extends BatchDescription {

    @Override
    protected void describe() {
        /*
         * a +-- b --+ d
         *    \- c -/
         */
        Work a = run(MockJobA.class).soon();
        Work b = run(MockJobB.class).after(a);
        Work c = run(MockJobC.class).after(a);
        run(MockJobD.class).after(b, c);
    }
}
