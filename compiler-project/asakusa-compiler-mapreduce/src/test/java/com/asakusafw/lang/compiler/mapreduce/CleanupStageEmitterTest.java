package com.asakusafw.lang.compiler.mapreduce;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link CleanupStageEmitter}.
 */
public class CleanupStageEmitterTest {

    /**
     * Java compiler.
     */
    @Rule
    public final JavaCompiler javac = new JavaCompiler();

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File a = new File(folder.getRoot(), "a/test.txt");
        File b = new File(folder.getRoot(), "b/test.txt");
        FileEditor.put(a, "Hello, world!");
        FileEditor.put(b, "Hello, world!");

        Path root = new Path(folder.getRoot().toURI());
        Path base = new Path(root, "a");

        ClassDescription client = new ClassDescription("com.example.StageClient");
        CleanupStageInfo info = new CleanupStageInfo(
                new StageInfo("simple", "simple", CleanupStageInfo.DEFAULT_STAGE_ID),
                base.toString());

        CleanupStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.<String, String>emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));

        assertThat(a.isFile(), is(false));
        assertThat(b.isFile(), is(true));
    }
}
