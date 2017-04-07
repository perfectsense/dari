package com.psddev.dari.db;

/**
 * Marker interface that defines precedence around which {@link Bridge} to
 * consider when there are multiple with the same parameterized type that
 * implement the same interface.
 *
 * <p>Note that in order for this concept to behave properly, it should be
 * reserved for the lowest (project) level.</p>
 */
public interface PriorityBridge {
}
