package com.asakusafw.lang.compiler.packaging;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link FileContainerRepository}.
 */
public class FileContainerRepositoryTest {

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
        FileContainerRepository repo = new FileContainerRepository(folder.getRoot());
        FileContainer c0 = repo.newContainer("a");
        FileContainer c1 = repo.newContainer("b");
        FileContainer c2 = repo.newContainer("a");
        assertThat(c0.getBasePath(), is(not(repo.getRoot())));
        assertThat(c0.getBasePath(), is(not(c1.getBasePath())));
        assertThat(c0.getBasePath(), is(not(c2.getBasePath())));

        assertThat(repo.getRoot().exists(), is(true));
        assertThat(c0.getBasePath().exists(), is(true));
        assertThat(c1.getBasePath().exists(), is(true));
        assertThat(c2.getBasePath().exists(), is(true));

        assertThat(repo.toString(), repo.reset(), is(true));
        assertThat(repo.getRoot().exists(), is(false));
        assertThat(c0.getBasePath().exists(), is(false));
        assertThat(c1.getBasePath().exists(), is(false));
        assertThat(c2.getBasePath().exists(), is(false));

        assertThat(repo.toString(), repo.reset(), is(true));
    }
}
