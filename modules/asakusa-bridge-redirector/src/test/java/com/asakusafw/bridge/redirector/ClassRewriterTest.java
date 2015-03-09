package com.asakusafw.bridge.redirector;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.objectweb.asm.Type;

import com.asakusafw.bridge.redirector.mock.MockCallee0;
import com.asakusafw.bridge.redirector.mock.MockCallee1;
import com.asakusafw.bridge.redirector.mock.MockCallee2;
import com.asakusafw.bridge.redirector.mock.MockCallee3;
import com.asakusafw.bridge.redirector.mock.MockCaller;

/**
 * Test for {@link ClassRewriter}.
 */
public class ClassRewriterTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        RedirectRule rule = new RedirectRule();
        rule.add(Type.getType(MockCallee0.class), Type.getType(MockCallee2.class));

        assertThat(apply(rule), is("0:2:1"));
    }

    /**
     * empty rule.
     * @throws Exception if failed
     */
    @Test
    public void empty_rule() throws Exception {
        RedirectRule rule = new RedirectRule();

        assertThat(apply(rule), is("0:0:1"));
    }

    /**
     * multiple rules.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        RedirectRule rule = new RedirectRule();
        rule.add(Type.getType(MockCallee0.class), Type.getType(MockCallee2.class));
        rule.add(Type.getType(MockCallee1.class), Type.getType(MockCallee3.class));

        assertThat(apply(rule), is("0:2:3"));
    }

    private String apply(RedirectRule rule) throws IOException {
        ClassRewriter rewriter = new ClassRewriter(rule);

        byte[] contents = VolatileClassLoader.dump(MockCaller.class);
        byte[] results;
        try (ByteArrayInputStream input = new ByteArrayInputStream(contents);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            rewriter.rewrite(input, output);
            results = output.toByteArray();
        }

        VolatileClassLoader loader = new VolatileClassLoader(getClass().getClassLoader());
        Class<?> aClass = loader.forceLoad(results);
        try {
            return aClass.newInstance().toString();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
