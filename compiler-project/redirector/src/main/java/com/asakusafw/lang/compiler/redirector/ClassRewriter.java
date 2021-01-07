/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites a class file binary to redirect API invocations.
 */
public class ClassRewriter {

    static final Logger LOG = LoggerFactory.getLogger(ClassRewriter.class);

    final RedirectRule rule;

    /**
     * Creates a new instance.
     * @param rule the redirect rule
     */
    public ClassRewriter(RedirectRule rule) {
        this.rule = rule;
    }

    /**
     * Rewrite the class binary and write it into the output.
     * @param input input stream which provides the original class binary
     * @param output the output stream which accepts the modified class binary
     * @throws IOException if failed to rewrite by I/O error
     */
    public void rewrite(InputStream input, OutputStream output) throws IOException {
        ClassReader reader = new ClassReader(input);

        /* NOTE: Avoid ClassWriter.COMPUTE_MAX|COMPUTE_FRAME
         * This may occur ClassWriter.getCommonSupreClass(), and then it may load the target classes.
         */
        ClassWriter writer = new ClassWriter(0);
        reader.accept(new ClassEditor(writer), 0);
        output.write(writer.toByteArray());
    }

    private class ClassEditor extends ClassVisitor {

        ClassEditor(ClassVisitor forward) {
            super(Opcodes.ASM5, forward);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor forward = super.visitMethod(access, name, desc, signature, exceptions);
            return new MethodEditor(forward);
        }
    }

    private class MethodEditor extends MethodVisitor {

        MethodEditor(MethodVisitor forward) {
            super(Opcodes.ASM5, forward);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            if (opcode != Opcodes.INVOKESTATIC) {
                super.visitMethodInsn(opcode, owner, name, desc, itf);
            } else {
                Type type = Type.getObjectType(owner);
                Type target = rule.redirect(type);
                if (LOG.isDebugEnabled() && type.equals(target) == false) {
                    LOG.debug("rewrite invocation: ({} => {}).{}{}", new Object[] { //$NON-NLS-1$
                            type.getClassName(),
                            target.getClassName(),
                            name,
                            Arrays.toString(Type.getMethodType(desc).getArgumentTypes()),
                    });
                }
                super.visitMethodInsn(opcode, target.getInternalName(), name, desc, itf);
            }
        }
    }
}
