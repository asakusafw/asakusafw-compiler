package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.runtime.io.ModelInput;

/**
 * A {@link ModelInput} for Hadoop writable objects.
 * @param <T> the data model type
 */
public class WritableModelInput<T extends Writable> implements ModelInput<T> {

    private final DataInput input;

    private String className;

    private boolean sawEof = false;

    private boolean closed = false;

    /**
     * Creates a new instance.
     * @param input the input
     */
    public WritableModelInput(DataInput input) {
        this.input = input;
    }

    /**
     * Creates a new instance.
     * @param <T> the data type
     * @param file the target file
     * @return the created instance
     * @throws IOException if failed to open the file
     */
    public static <T extends Writable> WritableModelInput<T> open(File file) throws IOException {
        return new WritableModelInput<>(new DataInputStream(FileEditor.open(file)));
    }

    /**
     * Collects files in the target directory.
     * @param directory the target directory
     * @param prefix the file name prefix
     * @param suffix the file name suffix
     * @return the found files
     */
    public static Set<File> collect(File directory, String prefix, String suffix) {
        if (directory.isDirectory() == false) {
            return Collections.emptySet();
        }
        Set<File> results = new LinkedHashSet<>();
        for (File file : directory.listFiles()) {
            String name = file.getName();
            if (name.startsWith(".") || name.equals("_SUCCESS")) {
                continue;
            }
            if (prefix != null && name.startsWith(prefix) == false) {
                continue;
            }
            if (suffix != null && name.endsWith(suffix) == false) {
                continue;
            }
            results.add(file);
        }
        return results;
    }

    /**
     * Returns the class name.
     * @return the class name, or {@code null} if this input does not have any records
     * @throws IOException if failed to obtain the header
     */
    public String getClassName() throws IOException {
        if (className == null) {
            if (readHeader() == false) {
                return null;
            }
        }
        assert className != null;
        return className;
    }

    @Override
    public boolean readTo(T model) throws IOException {
        if (className == null) {
            if (readHeader() == false) {
                return false;
            }
        }
        if (readSeparator() == false) {
            return false;
        }
        model.readFields(input);
        return true;
    }

    private boolean readHeader() throws IOException {
        if (readSeparator() == false) {
            return false;
        }
        className = Text.readString(input);
        return true;
    }

    private boolean readSeparator() throws IOException {
        if (sawEof) {
            return false;
        }
        boolean cont = input.readBoolean();
        if (cont) {
            return true;
        } else {
            sawEof = true;
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (input instanceof Closeable) {
            ((Closeable) input).close();
        }
    }
}
