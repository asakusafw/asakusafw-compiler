package com.asakusafw.bridge.redirector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class Util {

    private Util() {
        return;
    }

    /**
     * Copies contents via stream.
     * @param input the input
     * @param output the output
     * @throws IOException if failed to copy by I/O error
     */
    static void copy(InputStream input, OutputStream output) throws IOException {
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
