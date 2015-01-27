package com.asakusafw.lang.compiler.planning;

import java.util.Set;

/**
 * Represents an execution plan.
 * TODO invariants
 */
public interface Plan extends AttributeContainer {

    /**
     * Returns sub-plans in this execution plan.
     * @return sub-plans
     */
    Set<? extends SubPlan> getElements();
}
