package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Handles objects that depend on other objects of the same type, checking for
 * any conflicts like circular dependancies (ie a -> b -> c > a).
 *
 *  The caller can add specify any number of items and the dependencies of those items
 *  <p><code><pre>
 *  DependencyResolver<String> resolver = new DependencyResolver<String>();
 *  resolver.add("a");     // an item "a" with no depenedencies
 *  resolver.add("b","c"); // an item "b" that depends on item "c"
 *  resolvers.add("d","e","f") // an item "d" that depends on items "e" and "f"
 *  resolver.add("b","e") // a new dependency "e" for item "b" which was previously added
 *  </pre></code></p>
 *  Once all the items and their dependencies are added, a call to {@code resolve} will return
 *  a list of all items that are necessary to handle the configuration. The items are returned
 *  with dependants before dependers
 *  <p><code><pre>
 *  resolver.resolve(); // f, e, c, d, b, a
 *  </pre></code></p>
 *
 * @param <T>
 */
public class DependencyResolver<T> {

    private final Set<T> allObjects = new LinkedHashSet<>();
    private final Set<T> allDependencies = new LinkedHashSet<>();
    private final Set<Edge<T>> edges = new LinkedHashSet<>();

    /**
     * Add an item that is needed, plus any dependencies
     * @param object
     * @param dependencies
     */
    public void addRequired(T object, Iterable<T> dependencies) {

        allObjects.add(object);

        Edge<T> edge = null;
        for (Edge<T> e : edges) {
            if (object == e.object) {
                edge = e;
                break;
            }
        }
        if (edge == null) {
            edge = new Edge<T>(object);
            edges.add(edge);
        }

        if (dependencies != null) {
            for (T dependency : dependencies) {
                allDependencies.add(dependency);
                edge.dependencies.add(dependency);
            }
        }
    }

    /**
     * Add an item that is needed, plus any dependencies
     * @param object
     * @param dependencies
     */
    public void addRequired(T object, T... dependencies) {
        addRequired(object, dependencies != null ? Arrays.asList(dependencies) : null);
    }

    /**
     * Resolve the dependency configuration and return the list of items that are
     * necessary. Items that are dependants are earlier in the list than their
     * dependers.
     * @throws IllegalStateException if there is a cyclic dependency
     */
    public List<T> resolve() {

        Map<T, Set<T>> graph = new HashMap<T, Set<T>>();
        Queue<T> toBeChecked = new LinkedList<T>();

        toBeChecked.addAll(allDependencies);
        toBeChecked.removeAll(allObjects);

        for (Edge<T> edge : edges) {
            T object = edge.object;
            Set<T> incoming = new LinkedHashSet<>();
            graph.put(object, incoming);
            for (T dependency : edge.dependencies) {
                incoming.add(dependency);
            }
            if (incoming.isEmpty()) {
                toBeChecked.add(object);
            }
        }

        List<T> sorted = new ArrayList<T>();
        for (T item; (item = toBeChecked.poll()) != null;) {
            sorted.add(item);
            for (Map.Entry<T, Set<T>> e : graph.entrySet()) {
                T object = e.getKey();
                Set<T> incoming = e.getValue();
                if (!incoming.isEmpty()) {
                    incoming.remove(item);
                    if (incoming.isEmpty()) {
                        toBeChecked.add(object);
                    }
                }
            }
        }

        if (sorted.size() < edges.size()) {
            throw new IllegalStateException("Cyclic dependency!");
        } else {
            return sorted;
        }
    }

    private static class Edge<T> {

        public final T object;
        public final List<T> dependencies;

        public Edge(T object) {
            this.object = object;
            this.dependencies = new ArrayList<T>();
        }
    }
}

