package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * A {@link ModelOutput} for Hadoop writable objects.
 * @param <T> the data model type
 */
public class WritableModelOutput<T extends Writable> implements ModelOutput<T> {

    private final DataOutput output;

    private boolean first = true;

    private boolean closed = false;

    /**
     * Creates a new instance.
     * @param output the output
     */
    public WritableModelOutput(DataOutput output) {
        this.output = output;
    }

    /**
     * Creates a new instance.
     * @param <T> the data model type
     * @param file the target file
     * @return the created instance
     * @throws IOException if failed to open the file
     */
    public static <T extends Writable> WritableModelOutput<T> create(File file) throws IOException {
        return new WritableModelOutput<>(new DataOutputStream(FileEditor.create(file)));
    }

    @Override
    public void write(T model) throws IOException {
        if (first) {
            output.writeBoolean(true);
            Text.writeString(output, model.getClass().getName());
            first = false;
        }
        output.writeBoolean(true);
        model.write(output);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        output.writeBoolean(false);
        if (output instanceof Closeable) {
            ((Closeable) output).close();
        }
    }
}
