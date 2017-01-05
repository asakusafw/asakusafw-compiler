/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.packaging;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Base test class for this project.
 */
public abstract class ResourceTestRoot {

    static final Charset ENCODING = StandardCharsets.UTF_8;

    /**
     * Creates a {@link ContentProvider} for the contents.
     * @param contents the target contents
     * @return the content provider
     */
    public static ContentProvider provider(String contents) {
        return output -> output.write(contents.getBytes(ENCODING));
    }

    /**
     * Creates a {@link ByteArrayItem}.
     * @param path the item path
     * @param contents initial contents
     * @return the created item
     */
    public static ByteArrayItem item(String path, String contents) {
        Location location = Location.of(path);
        return new ByteArrayItem(location, contents.getBytes(ENCODING));
    }

    /**
     * Creates a {@link FileItem}.
     * @param base the base directory
     * @param path the item path
     * @return the created item
     */
    public static FileItem item(File base, String path) {
        Location location = Location.of(path);
        return new FileItem(location, new File(base, location.toPath(File.separatorChar)));
    }

    /**
     * Creates a {@link FileItem}.
     * @param base the base directory
     * @param path the item path
     * @param contents initial contents
     * @return the created item
     * @throws IOException if failed to create the file
     */
    public static FileItem item(File base, String path, String contents) throws IOException {
        FileItem item = item(base, path);
        try (OutputStream out = ResourceUtil.create(item.getFile())) {
            out.write(contents.getBytes(ENCODING));
        }
        return item;
    }

    /**
     * Creates a {@link ResourceItemRepository}.
     * @param items element items
     * @return the created repository
     */
    public static ResourceItemRepository repository(ResourceItem... items) {
        return new ResourceItemRepository(Arrays.asList(items));
    }

    /**
     * Returns the contents of the resource item.
     * @param item the resource item
     * @return the contents
     */
    public static String contents(ResourceItem item) {
        try (InputStream input = item.openResource()) {
            return new String(dump(input), ENCODING);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns the contents of resource on the current cursor.
     * @param cursor the resource item
     * @return the contents
     */
    public static String contents(ResourceRepository.Cursor cursor) {
        try (InputStream input = cursor.openResource()) {
            return new String(dump(input), ENCODING);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] dump(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ResourceUtil.copy(input, output);
        return output.toByteArray();
    }

    static byte[] dump(ContentProvider provider) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        provider.writeTo(output);
        return output.toByteArray();
    }

    /**
     * Dumps all items in the target resource repository.
     * @param repository the target repository
     * @return all items
     */
    public static Map<String, String> dump(ResourceRepository repository) {
        Map<String, String> results = new LinkedHashMap<>();
        try (ResourceRepository.Cursor cursor = repository.createCursor()) {
            while (cursor.next()) {
                results.put(cursor.getLocation().toPath(), contents(cursor).trim());
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return results;
    }

    /**
     * Dumps all items.
     * @param items the target items
     * @return all items
     */
    public static Map<String, String> dump(Collection<? extends ResourceItem> items) {
        Map<String, String> results = new LinkedHashMap<>();
        for (ResourceItem item : items) {
            String key = item.getLocation().toPath();
            assertThat(results, not(hasKey(key)));
            results.put(key, contents(item));
        }
        return results;
    }

    /**
     * Puts an item to sink.
     * @param sink the target sink
     * @param item the item
     */
    public static void put(ResourceSink sink, ResourceItem item) {
        try (InputStream input = item.openResource()) {
            sink.add(item.getLocation(), input);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns a matcher whether the item is on the specified location or not.
     * @param path the expected path
     * @return the matcher
     */
    public static Matcher<ResourceItem> hasLocation(String path) {
        return new FeatureMatcher<ResourceItem, Location>(is(Location.of(path)), "has location", "hasLocation") {
            @Override
            protected Location featureValueOf(ResourceItem actual) {
                return actual.getLocation();
            }
        };
    }

    /**
     * Returns a matcher whether the item has the specified contents or not.
     * @param value the expected contents
     * @return the matcher
     */
    public static Matcher<ResourceItem> hasContents(String value) {
        return new FeatureMatcher<ResourceItem, String>(is(value), "has contents", "hasContents") {
            @Override
            protected String featureValueOf(ResourceItem actual) {
                String contents = contents(actual);
                try {
                    String provider = new String(dump(actual), ENCODING);
                    assertThat(provider, is(contents));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                return contents;
            }
        };
    }
}
