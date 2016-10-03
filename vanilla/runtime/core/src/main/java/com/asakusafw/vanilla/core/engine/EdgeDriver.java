/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.vanilla.core.engine;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.processor.EdgeReader;
import com.asakusafw.dag.api.processor.EdgeWriter;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.mirror.InputPortMirror;
import com.asakusafw.vanilla.core.mirror.OutputPortMirror;

/**
 * An abstract super interface of edge I/O operations.
 * @since 0.4.0
 */
public interface EdgeDriver extends InterruptibleIo {

    /**
     * Returns the number of partitions.
     * @return the number of partitions
     */
    int getNumberOfPartitions();

    /**
     * Acquires an input reader for the given port.
     * @param id the port ID
     * @param taskIndex the task index (0-origin)
     * @param taskCount the number of total tasks
     * @return the corresponded reader
     * @throws IOException if I/O error was occurred while acquiring the reader
     * @throws InterruptedException if operation was interrupted while acquiring the reader
     * @throws IllegalStateException if the target port has been already {@link #complete(PortId) completed}, or
     *    there are incomplete port in opposite of the target port
     */
    EdgeReader acquireInput(PortId id, int taskIndex, int taskCount) throws IOException, InterruptedException;

    /**
     * Acquires an output writer for the given port.
     * @param id the port ID
     * @return the corresponded writer
     * @throws IOException if I/O error was occurred while acquiring the writer
     * @throws InterruptedException if operation was interrupted while acquiring the writer
     * @throws IllegalStateException if the target port has been already {@link #complete(PortId) completed}
     */
    EdgeWriter acquireOutput(PortId id) throws IOException, InterruptedException;

    /**
     * Finalizes all operations about the given port.
     * If the target port has been already completed, this will do nothing.
     * @param id the target port ID
     * @throws IOException if I/O error was occurred while completing the target port
     * @throws InterruptedException if interrupted while completing the target port
     */
    void complete(PortId id) throws IOException, InterruptedException;

    /**
     * An abstract implementation of {@link EdgeDriver}.
     * @since 0.4.0
     */
    abstract class Abstract implements EdgeDriver {

        private static final Logger LOG = LoggerFactory.getLogger(EdgeDriver.Abstract.class);

        private final ConcurrentMap<PortId, State> completed = new ConcurrentHashMap<>();

        /**
         * Returns the input port.
         * @param id the target port ID
         * @return the corresponded port mirror
         */
        protected abstract InputPortMirror getInput(PortId id);

        /**
         * Returns the output port.
         * @param id the target port ID
         * @return the corresponded port mirror
         */
        protected abstract OutputPortMirror getOutput(PortId id);

        /**
         * Acquires a reader of one-to-one input port.
         * @param port the port
         * @return the acquired reader
         * @throws IOException if I/O error was occurred while acquiring input
         * @throws InterruptedException if interrupted while acquiring input
         */
        protected abstract ObjectReader acquireOneToOneInput(
                InputPortMirror port) throws IOException, InterruptedException;

        /**
         * Acquires a reader of broadcast input port.
         * @param port the port
         * @return the acquired reader
         * @throws IOException if I/O error was occurred while acquiring input
         * @throws InterruptedException if interrupted while acquiring input
         */
        protected abstract ObjectReader acquireBroadcastInput(
                InputPortMirror port) throws IOException, InterruptedException;

        /**
         * Acquires a reader of scatter-gather input port.
         * @param port the port
         * @param taskIndex the task index (0-origin)
         * @param taskCount the number of total tasks
         * @return the acquired reader
         * @throws IOException if I/O error was occurred while acquiring input
         * @throws InterruptedException if interrupted while acquiring input
         */
        protected abstract GroupReader acquireScatterGatherInput(
                InputPortMirror port, int taskIndex, int taskCount) throws IOException, InterruptedException;

        /**
         * Acquires a writer of one-to-one output port.
         * @param port the port
         * @return the acquired writer
         * @throws IOException if I/O error was occurred while acquiring output
         * @throws InterruptedException if interrupted while acquiring output
         */
        protected abstract ObjectWriter acquireOneToOneOutput(
                OutputPortMirror port) throws IOException, InterruptedException;

        /**
         * Acquires a writer of broadcast output port.
         * @param port the port
         * @return the acquired writer
         * @throws IOException if I/O error was occurred while acquiring output
         * @throws InterruptedException if interrupted while acquiring output
         */
        protected abstract ObjectWriter acquireBroadcastOutput(
                OutputPortMirror port) throws IOException, InterruptedException;

        /**
         * Acquires a writer of scatter-gather output port.
         * @param port the port
         * @return the acquired writer
         * @throws IOException if I/O error was occurred while acquiring output
         * @throws InterruptedException if interrupted while acquiring output
         */
        protected abstract ObjectWriter acquireScatterGatherOutput(
                OutputPortMirror port) throws IOException, InterruptedException;

        /**
         * Completes one-to-one input port.
         * @param port the port
         * @throws IOException if I/O error was occurred while completing input
         * @throws InterruptedException if interrupted while completing input
         */
        protected abstract void completeOneToOneInput(
                InputPortMirror port) throws IOException, InterruptedException;

        /**
         * Completes broadcast input port.
         * @param port the port
         * @throws IOException if I/O error was occurred while completing input
         * @throws InterruptedException if interrupted while completing input
         */
        protected abstract void completeBroadcastInput(
                InputPortMirror port) throws IOException, InterruptedException;

        /**
         * Completes scatter-gather input port.
         * @param port the port
         * @throws IOException if I/O error was occurred while completing input
         * @throws InterruptedException if interrupted while completing input
         */
        protected abstract void completeScatterGatherInput(
                InputPortMirror port) throws IOException, InterruptedException;

        /**
         * Completes one-to-one output port.
         * @param port the port
         * @throws IOException if I/O error was occurred while completing output
         * @throws InterruptedException if interrupted while completing output
         */
        protected abstract void completeOneToOneOutput(
                OutputPortMirror port) throws IOException, InterruptedException;

        /**
         * Completes broadcast output port.
         * @param port the port
         * @throws IOException if I/O error was occurred while completing output
         * @throws InterruptedException if interrupted while completing output
         */
        protected abstract void completeBroadcastOutput(
                OutputPortMirror port) throws IOException, InterruptedException;

        /**
         * Completes scatter-gather output port.
         * @param port the port
         * @throws IOException if I/O error was occurred while completing output
         * @throws InterruptedException if interrupted while completing output
         */
        protected abstract void completeScatterGatherOutput(
                OutputPortMirror port) throws IOException, InterruptedException;

        @Override
        public EdgeReader acquireInput(
                PortId id, int taskIndex, int taskCount) throws IOException, InterruptedException {
            Arguments.requireNonNull(id);
            Arguments.require(id.getDirection() == PortInfo.Direction.INPUT);
            Invariants.require(completed.get(id) == null);
            InputPortMirror port = getInput(id);
            for (OutputPortMirror upstream : port.getOpposites()) {
                Invariants.require(completed.get(upstream.getId()) == State.DONE);
            }
            LOG.trace("acquiring {}", id);
            switch (port.getMovement()) {
            case ONE_TO_ONE:
                return acquireOneToOneInput(port);
            case BROADCAST:
                return acquireBroadcastInput(port);
            case SCATTER_GATHER:
                return acquireScatterGatherInput(port, taskIndex, taskCount);
            case NOTHING:
                throw new IllegalArgumentException();
            default:
                throw new AssertionError(port.getMovement());
            }
        }

        @Override
        public final EdgeWriter acquireOutput(PortId id) throws IOException, InterruptedException {
            Arguments.requireNonNull(id);
            Arguments.require(id.getDirection() == PortInfo.Direction.OUTPUT);
            Invariants.require(completed.get(id) == null);
            OutputPortMirror port = getOutput(id);
            for (InputPortMirror downstream : port.getOpposites()) {
                Invariants.require(completed.get(downstream.getId()) == null);
            }
            LOG.trace("acquiring {}", id);
            switch (port.getMovement()) {
            case ONE_TO_ONE:
                return acquireOneToOneOutput(port);
            case BROADCAST:
                return acquireBroadcastOutput(port);
            case SCATTER_GATHER:
                return acquireScatterGatherOutput(port);
            case NOTHING:
                throw new IllegalArgumentException();
            default:
                throw new AssertionError(port.getMovement());
            }
        }

        @Override
        public final void complete(PortId id) throws IOException, InterruptedException {
            Arguments.requireNonNull(id);
            if (completed.putIfAbsent(id, State.WIP) != null) {
                return;
            }
            LOG.trace("completing {}", id);
            if (id.getDirection() == PortInfo.Direction.INPUT) {
                InputPortMirror port = getInput(id);
                switch (port.getMovement()) {
                case ONE_TO_ONE:
                    completeOneToOneInput(port);
                    break;
                case BROADCAST:
                    completeBroadcastInput(port);
                    break;
                case SCATTER_GATHER:
                    completeScatterGatherInput(port);
                    break;
                case NOTHING:
                    break;
                default:
                    throw new AssertionError(port.getMovement());
                }
            } else {
                Arguments.require(id.getDirection() == PortInfo.Direction.OUTPUT);
                OutputPortMirror port = getOutput(id);
                switch (port.getMovement()) {
                case ONE_TO_ONE:
                    completeOneToOneOutput(port);
                    break;
                case BROADCAST:
                    completeBroadcastOutput(port);
                    break;
                case SCATTER_GATHER:
                    completeScatterGatherOutput(port);
                    break;
                case NOTHING:
                    break;
                default:
                    throw new AssertionError(port.getMovement());
                }
            }
            completed.replace(id, State.WIP, State.DONE);
        }

        private enum State {

            WIP,

            DONE,
        }
    }
}
