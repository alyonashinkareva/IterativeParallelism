package info.kgeorgiy.ja.shinkareva.iterative;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;

/**
 * Class implements {@link ParallelMapper} interfaces.
 *
 * @author Shinkareva Alyona (alyona.i.shinkareva@gmail.com)
 */

public class ParallelMapperImpl implements ParallelMapper {
    private final List<Thread> threads;
    private final SynchronizedQueue<Runnable> tasks;
    private boolean closed = false;

    /**
     * Class constructor.
     *
     * @param numberOfThreads is the number of threads.
     */

    public ParallelMapperImpl(int numberOfThreads) {
        this.threads = new ArrayList<>(Collections.nCopies(numberOfThreads, null));
        this.tasks = new SynchronizedQueue<>(numberOfThreads);
        Runnable worker = () -> {
            Runnable task;
            while (true) {
                try {
                    task = tasks.getFirst();
                    task.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            threads.set(i, new Thread(worker));
            threads.get(i).start();
        }
    }

    /**
     * Evaluates the function on each of the specified arguments in list in parallel.
     *
     * @param function function to evaluate.
     * @param list     list of arguments.
     * @param <T>      value type.
     * @param <R>      value type.
     * @return list of result of evaluating function on every argument
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> function, List<? extends T> list) throws InterruptedException {
        List<R> result = new ArrayList<>();
        SynchronizedCounter counterOfLeftTasks = new SynchronizedCounter(list.size());
        for (int i = 0; i < list.size(); i++) {
            final int idx = i;
            result.add(null);
            Runnable task = () -> {
                result.set(idx, function.apply(list.get(idx)));
                counterOfLeftTasks.counting();
            };
            tasks.add(task);
        }
        counterOfLeftTasks.waitForEnd();
        return result;
    }

    /**
     * Stops all running threads.
     */

    @Override
    public void close() {
        if (!closed) {
            for (var thread : threads) {
                thread.interrupt();
                while (true) {
                    try {
                        thread.join();
                        break;
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            closed = true;
        }
    }

    private static class SynchronizedQueue<T> {
        private final ArrayDeque<T> queue;

        private SynchronizedQueue(final int size) {
            this.queue = new ArrayDeque<>(size);
        }

        private synchronized T getFirst() throws InterruptedException {
            while (queue.isEmpty()) {
                wait();
            }
            return queue.removeFirst();
        }

        private synchronized void add(T elem) {
            queue.addLast(elem);
            notify();
        }
    }

    private static class SynchronizedCounter {
        private int value;

        private SynchronizedCounter(int value) {
            this.value = value;
        }

        private synchronized void counting() {
            value--;
            if (value == 0) {
                notify();
            }
        }

        private synchronized void waitForEnd() throws InterruptedException {
            while (value != 0) {
                wait();
            }
        }
    }
}
