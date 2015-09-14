package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/**
 * Thread-local variable storage that can keep track of multiple values.
 *
 * @param <T> Type of the variable.
 */
public class ThreadLocalStack<T> {

    private final ThreadLocal<Deque<T>> stackLocal = new ThreadLocal<>();

    /**
     * Pushes the given {@code item} onto the top of the stack.
     *
     * @param item
     *        Can't be {@code null}.
     *
     * @return {@code null} if the stack is empty.
     */
    public T push(T item) {
        Preconditions.checkNotNull(item);

        Deque<T> stack = stackLocal.get();
        T current;

        if (stack == null) {
            stack = new ArrayDeque<>();
            current = null;

            stackLocal.set(stack);

        } else {
            current = stack.peekFirst();
        }

        stack.addFirst(item);

        return current;
    }

    /**
     * Returns the item at the top of the stack.
     *
     * @return {@code null} if the stack is empty.
     */
    public T peek() {
        Deque<T> stack = stackLocal.get();

        return stack != null ? stack.peekFirst() : null;
    }

    /**
     * Removes and returns the item at the top of the stack.
     *
     * @return {@code null} if the stack is empty.
     */
    public T pop() {
        Deque<T> stack = stackLocal.get();

        if (stack == null || stack.isEmpty()) {
            return null;
        }

        T popped = stack.pollFirst();

        if (stack.isEmpty()) {
            stackLocal.remove();
        }

        return popped;
    }

    /**
     * Removes and returns the item at the top of the stack, or errors if it's
     * empty.
     *
     * @return Never {@code null}.
     *
     * @throws NoSuchElementException
     *         If the stack is empty.
     */
    public T popOrError() {
        T popped = pop();

        if (popped == null) {
            throw new NoSuchElementException();
        }

        return popped;
    }

    /**
     * Returns the item at the top of the stack.
     *
     * @return {@code null} if the stack is empty.
     */
    public T get() {
        return peek();
    }

    /**
     * Returns the item at the top of the stack, or errors if it's empty.
     *
     * @return Never {@code null}.
     *
     * @throws NoSuchElementException
     *         If the stack is empty.
     */
    public T getOrError() {
        T item = get();

        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    /**
     * Sets the stack to only contain the given {@code item}.
     *
     * @param item
     *        Can't be {@code null}.
     */
    public void set(T item) {
        Preconditions.checkNotNull(item);

        Deque<T> stack = new ArrayDeque<T>();

        stack.addFirst(item);
        stackLocal.set(stack);
    }

    /**
     * Removes all items from the stack.
     */
    public void remove() {
        stackLocal.remove();
    }

    /**
     * Executes the given {@code procedure} with the given {@code item}
     * at the top of the stack.
     *
     * @param item
     *        Can't be {@code null}.
     *
     * @param procedure
     *        Can't be {@code null}.
     */
    public void with(T item, WithProcedure procedure) {
        Preconditions.checkNotNull(item);
        Preconditions.checkNotNull(procedure);

        push(item);

        try {
            procedure.execute();

        } finally {
            popOrError();
        }
    }

    /**
     * For use with {@link ThreadLocalStack#with(Object, WithProcedure)}.
     */
    @FunctionalInterface
    public interface WithProcedure {

        void execute();
    }
}
