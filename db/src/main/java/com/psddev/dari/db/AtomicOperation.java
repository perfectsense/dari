package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Atomic operation on a field value within a state.
 *
 * @see com.psddev.dari.db.AtomicOperation.Add
 * @see com.psddev.dari.db.AtomicOperation.Increment
 * @see com.psddev.dari.db.AtomicOperation.Put
 * @see com.psddev.dari.db.AtomicOperation.Remove
 * @see com.psddev.dari.db.AtomicOperation.Replace
 */
public abstract class AtomicOperation {

    private final String field;

    /**
     * Creates an instance that operates on the given {@code field}.
     *
     * @param field
     *        Can't be {@code null}.
     */
    protected AtomicOperation(String field) {
        Preconditions.checkNotNull(field);

        this.field = field;
    }

    /**
     * Returns the name of the field whose value is to be changed
     * atomically.
     *
     * @return Never {@code null}.
     */
    public String getField() {
        return field;
    }

    /**
     * Executes this atomic operation on the given {@code state}.
     *
     * @param state
     *        Can't be {@code null}.
     */
    public abstract void execute(State state);

    /**
     * Atomic arithmetic increment operation.
     *
     * <p>Note that you can use a negative number for a decrement
     * operation.</p>
     */
    public static class Increment extends AtomicOperation {

        private final double value;

        public Increment(String field, double value) {
            super(field);

            this.value = value;
        }

        @Override
        public void execute(State state) {
            String field = getField();
            Object oldValue = state.getByPath(field);
            double newValue = value;

            if (oldValue instanceof Number) {
                newValue += ((Number) oldValue).doubleValue();
            }

            state.putByPath(field, newValue);
        }

        @Override
        public boolean equals(Object other) {
            return this == other
                    || (other instanceof Increment
                    && Objects.equals(getField(), ((Increment) other).getField())
                    && value == ((Increment) other).value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getField(), value);
        }
    }

    /**
     * Atomic add operation to a collection.
     *
     * <p>If the referenced field doesn't contain a collection, one is
     * automatically created.</p>
     */
    public static class Add extends AtomicOperation {

        private final Object value;

        public Add(String field, Object value) {
            super(field);

            this.value = value;
        }

        @Override
        public void execute(State state) {
            String field = getField();
            Object oldValue = state.getByPath(field);

            if (oldValue instanceof Collection) {
                @SuppressWarnings("unchecked")
                Collection<Object> values = (Collection<Object>) oldValue;

                values.add(value);

            } else {
                List<Object> values = new ArrayList<>();

                values.add(value);
                state.putByPath(field, values);
            }
        }

        @Override
        public boolean equals(Object other) {
            return this == other
                    || (other instanceof Add
                    && Objects.equals(getField(), ((Add) other).getField())
                    && Objects.equals(value, ((Add) other).value));
        }

        @Override
        public int hashCode() {
            return Objects.hash(getField(), value);
        }
    }

    /**
     * Atomic remove operation from a collection.
     *
     * <p>If the referenced field doesn't contain a collection, this does
     * nothing.</p>
     */
    public static class Remove extends AtomicOperation {

        private final Object value;

        public Remove(String field, Object value) {
            super(field);

            this.value = value;
        }

        @Override
        public void execute(State state) {
            String field = getField();
            Object oldValue = state.getByPath(field);

            if (oldValue instanceof Collection) {
                Collection<?> collection = (Collection<?>) oldValue;

                while (true) {
                    if (!collection.remove(value)) {
                        break;
                    }
                }
            }
        }

        @Override
        public boolean equals(Object other) {
            return this == other
                    || (other instanceof Remove
                    && Objects.equals(getField(), ((Remove) other).getField())
                    && Objects.equals(value, ((Remove) other).value));
        }

        @Override
        public int hashCode() {
            return Objects.hash(getField(), value);
        }
    }

    /**
     * Atomic replace operation that only sets the new value if the old
     * value doesn't change.
     *
     * <p>{@link #execute(State)} may throw {@link ReplacementException}
     * if the old value changes before the operation executes.</p>
     */
    public static class Replace extends AtomicOperation {

        private final Object oldValue;
        private final Object newValue;

        public Replace(String field, Object oldValue, Object newValue) {
            super(field);

            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        @Override
        public void execute(State state) {
            String field = getField();

            if (Objects.equals(state.getByPath(field), oldValue)) {
                state.putByPath(field, newValue);

            } else {
                throw new ReplacementException(state, field, oldValue, newValue);
            }
        }

        @Override
        public boolean equals(Object other) {
            return this == other
                    || (other instanceof Replace
                    && Objects.equals(getField(), ((Replace) other).getField())
                    && Objects.equals(oldValue, ((Replace) other).oldValue)
                    && Objects.equals(newValue, ((Replace) other).newValue));
        }

        @Override
        public int hashCode() {
            return Objects.hash(getField(), oldValue, newValue);
        }
    }

    /**
     * Atomic put operation.
     */
    public static class Put extends AtomicOperation {

        private final Object value;

        public Put(String field, Object value) {
            super(field);

            this.value = value;
        }

        @Override
        public void execute(State state) {
            state.putByPath(getField(), value);
        }

        @Override
        public boolean equals(Object other) {
            return this == other
                    || (other instanceof Put
                    && Objects.equals(getField(), ((Put) other).getField())
                    && Objects.equals(value, ((Put) other).value));
        }

        @Override
        public int hashCode() {
            return Objects.hash(getField(), value);
        }
    }

    /**
     * Thrown if the object state changes between when an atomic
     * replacement operation is requested and executes.
     */
    public static class ReplacementException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final State state;
        private final String field;
        private final Object oldValue;
        private final Object newValue;

        /**
         *
         * @param state Can't be {@code null}.
         * @param field Can't be {@code null}.
         * @param oldValue May be {@code null}.
         * @param newValue May be {@code null}.
         */
        public ReplacementException(State state, String field, Object oldValue, Object newValue) {
            super(String.format("Can't replace [%s] in #[%s]!", field, state.getId()));

            Preconditions.checkNotNull(state);
            Preconditions.checkNotNull(field);

            this.state = state;
            this.field = field;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        /**
         * Returns the state where the atomic operation is being executed.
         *
         * @return Never {@code null}.
         */
        public State getState() {
            return state;
        }

        /**
         * Returns the name of the field whose value is being replaced.
         *
         * @return Never {@code null}.
         */
        public String getField() {
            return field;
        }

        /**
         * Returns the field value when the replacement was requested.
         *
         * @return Never {@code null}.
         */
        public Object getOldValue() {
            return oldValue;
        }

        /**
         * Returns the replacement field value.
         *
         * @return Never {@code null}.
         */
        public Object getNewValue() {
            return newValue;
        }
    }
}
