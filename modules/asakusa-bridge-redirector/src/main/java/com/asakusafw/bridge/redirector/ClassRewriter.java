package com.asakusafw.bridge.redirector;

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
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        reader.accept(new ClassEditor(writer), 0);
        output.write(writer.toByteArray());
    }

    private class ClassEditor extends ClassVisitor {

        public ClassEditor(ClassVisitor forward) {
            super(Opcodes.ASM5, forward);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor forward = super.visitMethod(access, name, desc, signature, exceptions);
            return new MethodEditor(forward);
        }
    }

    private class MethodEditor extends MethodVisitor {

        public MethodEditor(MethodVisitor forward) {
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
                    LOG.debug("rewrite invocation: ({} => {}).{}{}", new Object[] {
                            type,
                            target,
                            name,
                            Arrays.toString(Type.getMethodType(desc).getArgumentTypes()),
                    });
                }
                super.visitMethodInsn(opcode, target.getInternalName(), name, desc, itf);
            }
        }
    }
}
