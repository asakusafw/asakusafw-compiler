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
package com.asakusafw.vanilla.testkit.adapter;

import com.asakusafw.lang.compiler.testdriver.adapter.CompilerProfileInitializer;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.testdriver.compiler.CompilerConfiguration;

/**
 * {@link CompilerProfileInitializer} for Asakusa Vanilla.
 * @since 0.4.0
 */
public class VanillaCompilerProfileInitializer implements CompilerProfileInitializer {

    @Override
    public void initialize(CompilerProfile profile, CompilerConfiguration configuration) {
        // there are no special operations for vanilla
    }
}
