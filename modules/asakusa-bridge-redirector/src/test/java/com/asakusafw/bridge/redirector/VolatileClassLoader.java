package com.asakusafw.bridge.redirector;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

/**
 * Volatile class loader for directly loading class binary.
 */
public class VolatileClassLoader extends ClassLoader {

    /**
     * Creates a new instance.
     * @param parent the parent loader
     */
    public VolatileClassLoader(ClassLoader parent) {
        super(parent);
    }

    /**
     * Returns a resource path of the target class file.
     * @param aClass the target class
     * @return the class file resource path (relative from the classpath)
     */
    public static String toPath(Class<?> aClass) {
        String name = aClass.getName();
        String path = name.replace('.', '/') + ".class";
        return path;
    }

    /**
     * Returns the byte contents of the target class file.
     * @param aClass the target class
     * @return the class file contents
     * @throws IOException if failed to obtain the contents by I/O error
     */
    public static byte[] dump(Class<?> aClass) throws IOException {
        String path = toPath(aClass);
        InputStream input = aClass.getClassLoader().getResourceAsStream(path);
        if (input == null) {
            throw new FileNotFoundException(path);
        }
        try {
            return consume(input);
        } finally {
            input.close();
        }
    }

    private static byte[] consume(InputStream contents) throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            Util.copy(contents, buf);
            bytes = buf.toByteArray();
        }
        return bytes;
    }

    /**
     * Forcibly loads a class in this loader.
     * @param contents the class file contents
     * @return the loaded class
     * @throws IOException if failed to obtain the class contents
     */
    public Class<?> forceLoad(InputStream contents) throws IOException {
        byte[] bytes = consume(contents);
        return forceLoad(bytes);
    }

    /**
     * Forcibly loads a class in this loader.
     * @param contents the class file contents
     * @return the loaded class
     */
    public Class<?> forceLoad(byte[] contents) {
        ClassReader reader = new ClassReader(contents);
        String binaryName = Type.getObjectType(reader.getClassName()).getClassName();
        Class<?> loaded = defineClass(binaryName, contents, 0, contents.length);
        resolveClass(loaded);
        return loaded;
    }
}
