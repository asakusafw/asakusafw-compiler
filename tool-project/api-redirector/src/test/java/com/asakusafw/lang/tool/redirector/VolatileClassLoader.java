/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.tool.redirector;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

import com.asakusafw.lang.tool.redirector.Util;

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
        try (InputStream input = aClass.getClassLoader().getResourceAsStream(path)) {
            if (input == null) {
                throw new FileNotFoundException(path);
            }
            return consume(input);
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
