package com.asakusafw.lang.compiler.mapreduce;

import org.apache.hadoop.fs.Path;

import com.asakusafw.runtime.stage.StageOutput;

/**
 * Utilities for MapReduce operations.
 */
public final class MapReduceUtil {

    private MapReduceUtil() {
        return;
    }

    /**
     * Returns a name for {@link StageOutput}.
     * @param name the output name
     * @return the processed name
     */
    public static String getStageOutputName(String name) {
        StringBuilder buf = new StringBuilder();
        for (char c : name.toCharArray()) {
            // 0 as escape character
            if ('1' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z') {
                buf.append(c);
            } else if (c <= 0xff) {
                buf.append('0');
                buf.append(String.format("%02x", (int) c)); //$NON-NLS-1$
            } else {
                buf.append("0u"); //$NON-NLS-1$
                buf.append(String.format("%04x", (int) c)); //$NON-NLS-1$
            }
        }
        return buf.toString();
    }

    /**
     * Returns the actual path of the target stage output.
     * @param basePath the stage output base path
     * @param name the output name
     * @return the processed path
     */
    public static String getStageOutputPath(String basePath, String name) {
        String prefix = getStageOutputName(name);
        String fileName = String.format("%s-*", prefix); //$NON-NLS-1$
        Path path = new Path(basePath, fileName);
        return path.toString();
    }
}
