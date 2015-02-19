package com.asakusafw.lang.compiler.core.dummy;

import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;

/**
 * Mock {@link CompilerParticipant}.
 */
@SuppressWarnings("javadoc")
public class DummyCompilerParticipant extends AbstractCompilerParticipant implements DummyElement {

    final String id;

    public DummyCompilerParticipant() {
        this("default");
    }

    public DummyCompilerParticipant(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }
}
