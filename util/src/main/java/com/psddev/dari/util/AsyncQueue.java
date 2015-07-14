package com.psddev.dari.util;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Blocking queue to be used with the asynchronous task classes. */
public class AsyncQueue<E> {

    /** Default capacity of the underlying {@link BlockingQueue}. */
    public static final int DEFAULT_QUEUE_CAPACITY = 250;

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncQueue.class);

    private final String id = UUID.randomUUID().toString();
    private final BlockingQueue<E> queue;
    private boolean isCloseAutomatically;
    private final Map<Object, Boolean> producers = new ConcurrentHashMap<Object, Boolean>();
    private volatile boolean isClosed;

    private final AtomicLong addSuccessCount = new AtomicLong();
    private final AtomicLong addFailureCount = new AtomicLong();
    private final AtomicLong addWait = new AtomicLong();
    private final AtomicLong removeCount = new AtomicLong();
    private final AtomicLong removeWait = new AtomicLong();

    /** Creates an instance with the given blocking {@code queue}. */
    public AsyncQueue(BlockingQueue<E> queue) {
        LOGGER.debug("Creating queue [{}]", this);
        this.queue = queue == null
                ? new ArrayBlockingQueue<E>(DEFAULT_QUEUE_CAPACITY)
                : queue;
    }

    /** Creates an instance with a new blocking queue. */
    public AsyncQueue() {
        this(null);
    }

    /**
     * Adds the given {@code item} to this queue. This method may block
     * until more space becomes available in this queue. If interrupted
     * while blocked, this queue will be closed.
     *
     * @param item If {@code null}, it won't be added.
     * @return {@code true} if the given {@code item} was added successfully.
     * @throws IllegalStateException If this queue is closed.
     */
    public boolean add(E item) {
        if (isClosed()) {
            throw new IllegalStateException("Can't add to a closed queue!");
        }

        long startTime = System.nanoTime();
        try {

            if (item != null) {
                while (true) {
                    try {
                        if (queue.offer(item, 10, TimeUnit.MILLISECONDS)) {
                            addSuccessCount.incrementAndGet();
                            return true;
                        }
                    } catch (InterruptedException ex) {
                        handleInterrupt(item, ex);
                    }
                }
            }

            addFailureCount.incrementAndGet();
            return false;

        } finally {
            addWait.addAndGet(System.nanoTime() - startTime);
        }
    }

    /**
     * Removes an item from this queue and returns it. This method may
     * block until more items become available in this queue. If interrupted
     * while blocked, this queue will be closed.
     *
     * @return {@code null} if there aren't any more items, which also
     *         implies that this queue is closed.
     */
    public E remove() {
        long startTime = System.nanoTime();
        try {

            while (true) {
                if (isClosed()) {
                    E item = queue.poll();
                    if (item != null) {
                        removeCount.incrementAndGet();
                    }
                    return item;

                } else {
                    try {
                        E item = queue.poll(10, TimeUnit.MILLISECONDS);
                        if (item != null) {
                            removeCount.incrementAndGet();
                            return item;
                        }
                    } catch (InterruptedException ex) {
                        handleInterrupt(null, ex);
                    }
                }
            }

        } finally {
            removeWait.addAndGet(System.nanoTime() - startTime);
        }
    }

    public void closeAutomatically() {
        if (producers.isEmpty()) {
            close();
        } else {
            isCloseAutomatically = true;
        }
    }

    public void addProducer(Object producer) {
        LOGGER.debug("Adding [{}] producer to [{}]", producer, this);
        producers.put(producer, Boolean.TRUE);
    }

    public void removeProducer(Object producer) {
        LOGGER.debug("Removing [{}] producer from [{}]", producer, this);
        producers.remove(producer);
        if (isCloseAutomatically && producers.isEmpty()) {
            close();
        }
    }

    /** Returns {@code true} if this queue is closed. */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Closes this queue so that more items can't be added. Existing
     * items in this queue won't be affected, and {@link #remove} may
     * continue to return items.
     */
    public void close() {
        LOGGER.debug("Closing [{}]", this);
        isClosed = true;
    }

    public long getAddSuccessCount() {
        return addSuccessCount.get();
    }

    public long getAddFailureCount() {
        return addFailureCount.get();
    }

    public long getAddWait() {
        return addWait.get();
    }

    public long getRemoveCount() {
        return removeCount.get();
    }

    public long getRemoveWait() {
        return removeWait.get();
    }

    /**
     * Called if the thread writing to this queue is interrupted and an
     * InterruptedException is thrown. Subclasses may override this method
     * to handle the interrupt differently. Default implementation closes the
     * queue.
     *
     * @param item the item being added, or null if interrupted during a remove
     *             operation.
     * @param ex the exception thrown.
     */
    protected void handleInterrupt(E item, InterruptedException ex) {
        close();
    }

    // --- Object support ---

    @Override
    public String toString() {
        return id;
    }
}
