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
package com.asakusafw.lang.compiler.testdriver.adapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import com.asakusafw.lang.compiler.analyzer.builtin.LoggingOperatorRemover;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.lang.compiler.tester.CompilerTester;
import com.asakusafw.testdriver.compiler.basic.BasicCompilerConfiguration;
import com.asakusafw.vocabulary.operator.Logging;

class CompilerConfigurationAdapter extends BasicCompilerConfiguration {

    private final List<CompilerProfile.Edit> edits = new ArrayList<>();

    public void withDefaults() {
        withClassLoader(getClass().getClassLoader());
        withOptimizeLevel(OptimizeLevel.NORMAL);
        withDebugLevel(DebugLevel.DISABLED);
        withOption("directio.input.filter.enabled", String.valueOf(false)); //$NON-NLS-1$
    }

    public CompilerConfigurationAdapter withEdit(CompilerProfile.Edit edit) {
        edits.add(edit);
        return this;
    }

    public CompilerTester start(Class<?> target) throws IOException {
        ClassLoader cl = getClassLoader();
        CompilerProfile profile = new CompilerProfile(cl);

        // disables framework installation
        profile.withFrameworkInstallation(null);

        profile.forCompilerOptions()
            .withRuntimeWorkingDirectory(Util.createStagePath(), false)
            .withProperties(getOptions());

        profile.forProjectRepository()
            .embed(ResourceUtil.findLibraryByClass(target));

        profile.forToolRepository()
            .useDefaults();

        if (getDebugLevel().compareTo(DebugLevel.NORMAL) >= 0) {
            profile.forCompilerOptions()
                .withProperty(LoggingOperatorRemover.KEY_LOG_LEVEL, Logging.Level.DEBUG.name());
        }

        for (CompilerProfile.Edit edit : edits) {
            profile.apply(edit);
        }

        for (CompilerProfileInitializer initializer : ServiceLoader.load(CompilerProfileInitializer.class, cl)) {
            initializer.initialize(profile, this);
        }

        return profile.build();
    }
}
