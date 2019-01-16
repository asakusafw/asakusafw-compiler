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
package com.asakusafw.lang.compiler.redirector;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.objectweb.asm.Type;

import com.asakusafw.lang.compiler.redirector.mock.MockCallee0;
import com.asakusafw.lang.compiler.redirector.mock.MockCallee1;
import com.asakusafw.lang.compiler.redirector.mock.MockCallee2;
import com.asakusafw.lang.compiler.redirector.mock.MockCallee3;
import com.asakusafw.lang.compiler.redirector.mock.MockCaller;

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
        ByteArrayInputStream input = new ByteArrayInputStream(contents);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        rewriter.rewrite(input, output);
        VolatileClassLoader loader = new VolatileClassLoader(getClass().getClassLoader());
        Class<?> aClass = loader.forceLoad(output.toByteArray());
        try {
            return aClass.newInstance().toString();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
