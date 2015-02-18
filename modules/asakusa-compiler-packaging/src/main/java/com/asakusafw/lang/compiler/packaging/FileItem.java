package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceItem} using local file.
 */
public class FileItem implements ResourceItem {

    private final Location location;

    private final File file;

    /**
     * Creates a new instance.
     * @param location the file location
     * @param file the target file
     */
    public FileItem(Location location, File file) {
        this.location = location;
        this.file = file;
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public InputStream openResource() throws IOException {
        return new FileInputStream(file);
    }

    /**
     * Returns the file.
     * @return the file
     */
    public File getFile() {
        return file;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + location.hashCode();
        result = prime * result + file.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FileItem other = (FileItem) obj;
        if (!location.equals(other.location)) {
            return false;
        }
        if (!file.equals(other.file)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "FileItem({0}=>{1})", //$NON-NLS-1$
                location,
                file);
    }
}
