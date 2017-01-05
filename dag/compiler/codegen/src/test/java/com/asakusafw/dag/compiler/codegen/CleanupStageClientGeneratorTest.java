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
package com.asakusafw.dag.compiler.codegen;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.stage.AbstractCleanupStageClient;
import com.asakusafw.runtime.stage.StageConstants;

/**
 * Test for {@link CleanupStageClientGenerator}.
 */
public class CleanupStageClientGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassDescription generated = generate();
        assertThat(generated.getBinaryName(), is(AbstractCleanupStageClient.IMPLEMENTATION));
        File keep = touch();
        File target = touch();
        run(generated, target.getName());
        assertThat(keep.exists(), is(true));
        assertThat(target.exists(), is(false));
    }

    private File touch() {
        return Lang.safe(() -> {
            File f = temporary.newFolder();
            new File(f, "DUMMY").createNewFile();
            return f;
        });
    }

    private ClassDescription generate() {
        Path base = new Path(temporary.getRoot().toURI());
        Path target = new Path(base, StageConstants.EXPR_EXECUTION_ID);
        return add(CleanupStageClientGenerator.DEFAULT_CLASS, c -> new CleanupStageClientGenerator().generate(
                "b", "f",
                target.toString(), c));
    }

    private void run(ClassDescription generated, String executionId) {
        Configuration conf = new Configuration();
        conf.set(StageConstants.PROP_USER, "testing");
        conf.set(StageConstants.PROP_EXECUTION_ID, executionId);
        loading(generated, c -> {
            Tool t = ReflectionUtils.newInstance(c.asSubclass(Tool.class), conf);
            assertThat(t.run(new String[0]), is(0));
        });
    }
}
