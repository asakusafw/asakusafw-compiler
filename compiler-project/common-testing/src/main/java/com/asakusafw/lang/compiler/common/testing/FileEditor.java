/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.common.testing;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Edits files.
 */
public final class FileEditor {

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private FileEditor() {
        return;
    }

    /**
     * Creates a new input stream for the target file.
     * @param file the target file
     * @return the created stream
     */
    public static InputStream open(File file) {
        try {
            return new FileInputStream(file);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Creates a new file.
     * @param file the target file
     * @return {@code true} if the file is just created, otherwise it already exists
     */
    public static boolean newFile(File file) {
        File parent = file.getAbsoluteFile().getParentFile();
        mkdir(parent);
        try {
            return file.createNewFile();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Creates a new output stream for the target file.
     * @param file the target file
     * @return the created stream
     */
    public static OutputStream create(File file) {
        File parent = file.getAbsoluteFile().getParentFile();
        mkdir(parent);
        try {
            return new FileOutputStream(file);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Creates a new scanner for the target file.
     * @param file the target file
     * @return the created scanner
     */
    public static Scanner scanner(File file) {
        return new Scanner(new InputStreamReader(open(file), ENCODING));
    }

    /**
     * Creates a new writer for the target file.
     * @param file the target file
     * @return the created writer
     */
    public static PrintWriter writer(File file) {
        return new PrintWriter(new OutputStreamWriter(create(file), ENCODING));
    }

    /**
     * Creates a new file with contents.
     * @param file the target file
     * @param contents the file contents
     */
    public static void put(File file, String... contents) {
        try (PrintWriter w = writer(file)) {
            for (String s : contents) {
                w.println(s);
            }
        }
    }

    /**
     * Returns contents in the file.
     * @param file the target file
     * @return the contents
     */
    public static List<String> get(File file) {
        List<String> results = new ArrayList<>();
        try (Scanner s = scanner(file)) {
            while (s.hasNextLine()) {
                results.add(s.nextLine());
            }
        }
        return results;
    }

    /**
     * Copies a file or directory.
     * @param source the source file or directory
     * @param destination the destination file
     */
    public static void copy(File source, File destination) {
        if (source.isDirectory()) {
            copyDirectory(source, destination);
        } else {
            try (InputStream in = open(source)) {
                try (OutputStream out = create(destination)) {
                    copyStream(in, out);
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    /**
     * Copies contents into a file.
     * @param source the source contents
     * @param destination the destination file
     */
    public static void copy(InputStream source, File destination) {
        try (OutputStream out = create(destination)) {
            copyStream(source, out);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Copies contents from a file.
     * @param source the source file
     * @param destination the destination stream
     */
    public static void copy(File source, OutputStream destination) {
        try (InputStream in = open(source)) {
            copyStream(in, destination);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Copies contents between two streams.
     * @param source the source stream
     * @param destination the destination stream
     */
    public static void copy(InputStream source, OutputStream destination) {
        try {
            copyStream(source, destination);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Copies a directory.
     * @param source the source directory
     * @param destination the destination file
     */
    public static void copyDirectory(File source, File destination) {
        if (source.isDirectory() == false) {
            return;
        }
        mkdir(destination);
        for (File file : list(source)) {
            File target = new File(destination, file.getName());
            if (file.isDirectory()) {
                copyDirectory(file, target);
            } else {
                copy(file, target);
            }
        }
    }

    /**
     * Extracts entries into the directory.
     * @param source the source archive file
     * @param destination the destination file
     */
    public static void extract(File source, File destination) {
        try (ZipInputStream input = new ZipInputStream(open(source))) {
            while (true) {
                ZipEntry entry = input.getNextEntry();
                if (entry == null) {
                    break;
                }
                File target = new File(destination, entry.getName());
                if (entry.isDirectory()) {
                    mkdir(target);
                } else {
                    copy(input, target);
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static void mkdir(File file) {
        if (file.mkdirs() == false && file.isDirectory() == false) {
            throw new IOError(new IOException(MessageFormat.format(
                    "failed to create directory: {0}",
                    file)));
        }
    }

    /**
     * Extracts entries into the directory.
     * @param source the source archive input stream
     * @param destination the destination file
     */
    public static void extract(ZipInputStream source, File destination) {
        try {
            while (true) {
                ZipEntry entry = source.getNextEntry();
                if (entry == null) {
                    break;
                }
                File target = new File(destination, entry.getName());
                if (entry.isDirectory()) {
                    mkdir(target);
                } else {
                    copy(source, target);
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Returns a contents in the file.
     * @param source the source file
     * @return the contents in bytes
     */
    public static byte[] dump(File source) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (InputStream in = open(source)) {
            copyStream(in, output);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return output.toByteArray();
    }

    /**
     * Returns a contents in the stream.
     * @param source the source stream
     * @return the contents in bytes
     */
    public static byte[] dump(InputStream source) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            copyStream(source, output);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return output.toByteArray();
    }

    /**
     * Make the file or files in a directory executable.
     * @param file the target file or directory
     * @param extension the target file extension (requires a dot character)
     */
    public static void setExecutable(File file, String extension) {
        if (file.isFile()) {
            if (extension == null || file.getName().endsWith(extension)) {
                file.setExecutable(true, true);
            }
        } else if (file.isDirectory()) {
            for (File f : list(file)) {
                setExecutable(f, extension);
            }
        }
    }

    private static List<File> list(File dir) {
        return Optional.ofNullable(dir.listFiles())
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }

    private static void copyStream(InputStream input, OutputStream output) throws IOException {
        byte[] buf = new byte[256];
        while (true) {
            int read = input.read(buf);
            if (read < 0) {
                break;
            }
            output.write(buf, 0, read);
        }
    }
}
