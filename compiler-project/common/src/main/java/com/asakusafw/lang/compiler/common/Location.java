/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.common;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.common.util.StringUtil;

/**
 * Represents a resource location.
 */
public class Location implements Comparable<Location> {

    /**
     * The default path segment separator character.
     */
    public static final char DEFAULT_SEGMENT_SEPARATOR = '/';

    private final Location parent;

    private final String name;

    /**
     * Creates a new instance.
     * @param parent the parent location
     * @param name the local resource name
     */
    public Location(Location parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    /**
     * Returns the parent location.
     * @return the parent location, or {@code null} if this does not have a parent
     */
    public Location getParent() {
        return parent;
    }

    /**
     * Returns the local resource name.
     * @return the local resource name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns a child location.
     * @param lastName the child resource name
     * @return the child location
     */
    public Location append(String lastName) {
        return new Location(this, lastName);
    }

    /**
     * Returns a child location.
     * @param suffix the child location which is relative from this
     * @return the child location
     */
    public Location append(Location suffix) {
        LinkedList<String> segments = suffix.asList();
        Location current = this;
        for (String segment : segments) {
            current = new Location(current, segment);
        }
        return current;
    }

    /**
     * Returns a location from path string using {@link #DEFAULT_SEGMENT_SEPARATOR} as a segment separator.
     * @param pathString the path string
     * @return the location
     * @see #of(String, char)
     */
    public static Location of(String pathString) {
        return of(pathString, DEFAULT_SEGMENT_SEPARATOR);
    }

    /**
     * Returns a location from path string.
     * @param pathString the path string
     * @param separator the segment separator char
     * @return the location
     */
    public static Location of(String pathString, char separator) {
        String[] segments = pathString.split(Pattern.quote(String.valueOf(separator)));
        Location current = null;
        for (String segment : segments) {
            if (segment.isEmpty()) {
                continue;
            }
            current = new Location(current, segment);
        }
        if (current == null) {
            throw new IllegalArgumentException();
        }
        return current;
    }

    /**
     * Returns path string using {@link #DEFAULT_SEGMENT_SEPARATOR} as a segment separator.
     * @return the path string
     */
    public String toPath() {
        return toPath(DEFAULT_SEGMENT_SEPARATOR);
    }

    /**
     * Returns path string.
     * @param separator the segment separator char
     * @return the path string
     */
    public String toPath(char separator) {
        LinkedList<String> segments = asList();
        return StringUtil.join(separator, segments);
    }

    private LinkedList<String> asList() {
        LinkedList<String> segments = new LinkedList<>();
        for (Location current = this; current != null; current = current.parent) {
            segments.addFirst(current.name);
        }
        return segments;
    }

    /**
     * Returns whether this location is a prefix of another location.
     * @param other target location
     * @return {@code true} if is prefix or same location, otherwise {@code false}
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public boolean isPrefixOf(Location other) {
        Objects.requireNonNull(other);
        int thisSegments = count(this);
        int otherSegments = count(other);
        if (thisSegments > otherSegments) {
            return false;
        }
        Location current = other;
        for (int i = 0, n = otherSegments - thisSegments; i < n; i++) {
            current = current.getParent();
        }
        return this.equals(current);
    }

    private int count(Location location) {
        int count = 1;
        Location current = location.getParent();
        while (current != null) {
            count++;
            current = current.getParent();
        }
        return count;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        Location current = this;
        while (current != null) {
            result = prime * result + current.name.hashCode();
            current = current.parent;
        }
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
        Location other = (Location) obj;
        Location thisCur = this;
        Location otherCur = other;
        while (thisCur != null && otherCur != null) {
            if (thisCur == otherCur) {
                return true;
            }
            if (thisCur.name.equals(otherCur.name) == false) {
                return false;
            }
            thisCur = thisCur.parent;
            otherCur = otherCur.parent;
        }
        return thisCur == otherCur;
    }

    @Override
    public int compareTo(Location o) {
        Iterator<String> a = asList().iterator();
        Iterator<String> b = o.asList().iterator();
        while (true) {
            if (a.hasNext() && b.hasNext()) {
                String aSegment = a.next();
                String bSegment = b.next();
                int diff = aSegment.compareTo(bSegment);
                if (diff != 0) {
                    return diff;
                }
                continue;
            } else if (a.hasNext()) {
                assert b.hasNext() == false;
                return +1;
            } else if (b.hasNext()) {
                assert a.hasNext() == false;
                return -1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public String toString() {
        return toPath();
    }
}
