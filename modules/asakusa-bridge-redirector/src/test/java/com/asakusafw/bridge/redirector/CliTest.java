package com.asakusafw.bridge.redirector;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.objectweb.asm.Type;

import com.asakusafw.bridge.redirector.Cli.Configuration;
import com.asakusafw.bridge.redirector.mock.MockCallee0;
import com.asakusafw.bridge.redirector.mock.MockCallee1;
import com.asakusafw.bridge.redirector.mock.MockCallee2;
import com.asakusafw.bridge.redirector.mock.MockCallee3;
import com.asakusafw.bridge.redirector.mock.MockCaller;

/**
 * Test for {@link Cli}.
 */
public class CliTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * parses minimal args.
     * @throws Exception if failed
     */
    @Test
    public void parse_minimal() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        addClass(contents, MockCaller.class);

        File input = save(contents);
        Configuration conf = Cli.parse(strings(new Object[] {
                "--input", input,
                "--rule", rule(MockCallee0.class, MockCallee2.class),
        }));

        assertThat(conf.input, is(input));
        assertThat(conf.output, is(nullValue()));
        assertThat(conf.rule, redirect(MockCallee0.class, MockCallee2.class));
        assertThat(conf.rule, redirect(MockCallee1.class, MockCallee1.class));
        assertThat(conf.overwrite, is(false));
    }

    /**
     * parses full args.
     * @throws Exception if failed
     */
    @Test
    public void parse_full() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        addClass(contents, MockCaller.class);

        File input = save(contents);
        File output = new File(temporary.getRoot(), "output/a.jar");
        Configuration conf = Cli.parse(strings(new Object[] {
                "--input", input,
                "--output", output,
                "--rule", rule(MockCallee0.class, MockCallee2.class),
                "--rule", rule(MockCallee1.class, MockCallee3.class),
                "--overwrite",
        }));

        assertThat(conf.input, is(input));
        assertThat(conf.output, is(output));
        assertThat(conf.rule, redirect(MockCallee0.class, MockCallee2.class));
        assertThat(conf.rule, redirect(MockCallee1.class, MockCallee3.class));
        assertThat(conf.overwrite, is(true));
    }

    /**
     * parses empty args.
     * @throws Exception if failed
     */
    @Test(expected = Exception.class)
    public void parse_empty() throws Exception {
        Cli.parse();
    }

    /**
     * executes w/ minimal args.
     * @throws Exception if failed
     */
    @Test
    public void execute_minimal() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        contents.put("a.bin", new byte[] { 1, 2, 3 });
        addClass(contents, MockCaller.class);

        File input = save(contents);
        int status = Cli.execute(strings(new Object[] {
                "--input", input,
                "--rule", rule(MockCallee0.class, MockCallee2.class),
        }));
        assertThat(status, is(0));

        File escape = new File(input.getParentFile(), input.getName() + ".bak");
        assertThat(escape.isFile(), is(true));

        assertThat(apply(escape), is("0:0:1"));
        assertThat(apply(input), is("0:2:1"));
        assertThat(dump(input), hasEntry("a.bin", new byte[] { 1, 2, 3 }));
    }

    /**
     * executes w/ minimal args.
     * @throws Exception if failed
     */
    @Test
    public void execute_full() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        contents.put("a.bin", new byte[] { 1, 2, 3 });
        addClass(contents, MockCaller.class);

        File input = save(contents);
        File output = new File(temporary.getRoot(), "output/a.jar");
        int status = Cli.execute(strings(new Object[] {
                "--input", input,
                "--output", output,
                "--rule", rule(MockCallee0.class, MockCallee2.class),
                "--rule", rule(MockCallee1.class, MockCallee3.class),
                "--overwrite",
        }));
        assertThat(status, is(0));

        File escape = new File(input.getParentFile(), input.getName() + ".bak");
        assertThat(escape.isFile(), is(false));

        assertThat(apply(input), is("0:0:1"));
        assertThat(apply(output), is("0:2:3"));
        assertThat(dump(output), hasEntry("a.bin", new byte[] { 1, 2, 3 }));
    }

    /**
     * executes w/ overwrite.
     * @throws Exception if failed
     */
    @Test
    public void execute_overwrite() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        contents.put("a.bin", new byte[] { 1, 2, 3 });
        addClass(contents, MockCaller.class);

        File input = save(contents);
        int status = Cli.execute(strings(new Object[] {
                "--input", input,
                "--rule", rule(MockCallee0.class, MockCallee2.class),
                "--overwrite",
        }));
        assertThat(status, is(0));

        File escape = new File(input.getParentFile(), input.getName() + ".bak");
        assertThat(escape.isFile(), is(false));

        assertThat(apply(input), is("0:2:1"));
        assertThat(dump(input), hasEntry("a.bin", new byte[] { 1, 2, 3 }));
    }

    /**
     * executes w/ empty args.
     * @throws Exception if failed
     */
    @Test
    public void execute_empty() throws Exception {
        int status = Cli.execute();
        assertThat(status, is(not(0)));
    }

    private String apply(File file) throws IOException {
        Map<String, byte[]> contents = dump(file);
        String name = VolatileClassLoader.toPath(MockCaller.class);
        assertThat(contents, hasKey(name));

        VolatileClassLoader loader = new VolatileClassLoader(getClass().getClassLoader());
        Class<?> aClass = loader.forceLoad(contents.get(name));
        try {
            return aClass.newInstance().toString();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private Map<String, byte[]> dump(File file) throws IOException, FileNotFoundException {
        Map<String, byte[]> contents;
        try (ZipInputStream input = new ZipInputStream(new FileInputStream(file))) {
            contents = ZipUtil.dump(input);
        }
        return contents;
    }

    private Matcher<RedirectRule> redirect(Class<?> from, Class<?> to) {
        final Type a = Type.getType(from);
        final Type b = Type.getType(to);
        return new FeatureMatcher<RedirectRule, Type>(is(b), String.format("redirects %s => ", a), "redirect") {
            @Override
            protected Type featureValueOf(RedirectRule actual) {
                return actual.redirect(a);
            }
        };
    }

    private File save(Map<String, byte[]> contents) throws IOException {
        File file = temporary.newFile();
        try (ZipOutputStream output = new ZipOutputStream(new FileOutputStream(file))) {
            ZipUtil.load(output, contents);
        }
        return file;
    }

    private void addClass(Map<String, byte[]> contents, Class<?> aClass) throws IOException {
        String path = VolatileClassLoader.toPath(aClass);
        byte[] bin = VolatileClassLoader.dump(aClass);
        contents.put(path, bin);
    }

    private String[] strings(Object... values) {
        String[] results = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            results[i] = String.valueOf(values[i]);
        }
        return results;
    }

    private String rule(Class<?> from, Class<?> to) {
        return String.format("%s%s%s",
                from.getName(),
                Cli.RULE_VALUE_SEPARATOR,
                to.getName());
    }
}
