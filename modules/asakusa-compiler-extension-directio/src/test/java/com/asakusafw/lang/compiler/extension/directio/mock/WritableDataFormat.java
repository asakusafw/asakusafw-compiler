package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.directio.BinaryStreamFormat;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * {@link BinaryStreamFormat} for {@link WritableModelInput} and {@link WritableModelOutput}.
 * @param <T> the target data type
 */
public abstract class WritableDataFormat<T extends Writable> extends BinaryStreamFormat<T> {

    @Override
    public long getPreferredFragmentSize() throws IOException, InterruptedException {
        return -1;
    }

    @Override
    public long getMinimumFragmentSize() throws IOException, InterruptedException {
        return -1;
    }

    @Override
    public ModelInput<T> createInput(
            Class<? extends T> dataType, String path, InputStream stream,
            long offset, long fragmentSize) {
        return new WritableModelInput<>(new DataInputStream(stream));
    }

    @Override
    public ModelOutput<T> createOutput(Class<? extends T> dataType, String path, OutputStream stream) {
        return new WritableModelOutput<>(new DataOutputStream(stream));
    }
}
