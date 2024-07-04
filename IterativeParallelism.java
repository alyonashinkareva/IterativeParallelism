package info.kgeorgiy.ja.shinkareva.iterative;

import info.kgeorgiy.java.advanced.iterative.NewListIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * Class implements {@link NewListIP} interfaces.
 *
 * @author Shinkareva Alyona (alyona.i.shinkareva@gmail.com)
 */

public class IterativeParallelism implements NewListIP {
    private ParallelMapper mapper;

    /**
     * Default constructor.
     */

    public IterativeParallelism() {
    }

    /**
     * Constructor with {@link ParallelMapper}
     *
     * @param mapper {@link ParallelMapper}.
     */

    public IterativeParallelism(ParallelMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Returns minimum value.
     *
     * @param threads    number of concurrent threads.
     * @param list       list of values to get minimum of.
     * @param comparator value comparator.
     * @param <T>        value type.
     * @return minimum of values of given list.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> T minimum(int threads, List<? extends T> list, Comparator<? super T> comparator) throws InterruptedException {
        return maximum(threads, list, comparator.reversed());
    }

    /**
     * Returns minimum value, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads    number of concurrent threads.
     * @param list       list of values to get minimum of.
     * @param comparator value comparator.
     * @param step       step size.
     * @param <T>        value type.
     * @return minimum of values under consideration of given list.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> T minimum(int threads, List<? extends T> list, Comparator<? super T> comparator, int step) throws InterruptedException {
        return maximum(threads, list, comparator.reversed(), step);
    }

    /**
     * Returns maximum value.
     *
     * @param threads    number of concurrent threads.
     * @param list       list of values to get minimum of.
     * @param comparator value comparator.
     * @param <T>        value type.
     * @return maximum of values of given list.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> T maximum(int threads, List<? extends T> list, Comparator<? super T> comparator) throws InterruptedException {
        return maximum(threads, list, comparator, 1);
    }

    /**
     * Returns maximum value, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads    number of concurrent threads.
     * @param list       list of values to get maximum of.
     * @param comparator value comparator.
     * @param step       step size.
     * @param <T>        value type.
     * @return maximum of values under consideration of given list.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> T maximum(int threads, List<? extends T> list, Comparator<? super T> comparator, int step) throws InterruptedException {
        return runFunction(
                threads,
                step,
                list,
                () -> list.get(0),
                BinaryOperator.maxBy(comparator),
                l -> l.stream().max(comparator).orElse(null)
        );
    }

    /**
     * Returns whether all values satisfy predicate.
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param <T>       value type.
     * @return whether all values satisfy predicate or {@code true}, if no values are given.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> boolean all(int threads, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        return all(threads, list, predicate, 1);
    }

    /**
     * Returns whether all values satisfy predicate, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param step      step size.
     * @param <T>       value type.
     * @return whether all values under consideration satisfy predicate or {@code true}, if no values are given.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> boolean all(int threads, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {
        return runFunction(
                threads,
                step,
                list,
                () -> true,
                (r, t) -> (r && predicate.test(t)),
                l -> l.stream().allMatch(elem -> elem)
        );
    }

    /**
     * Returns whether any of values satisfies predicate.
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param <T>       value type.
     * @return whether any value satisfies predicate or {@code false}, if no values are given.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> boolean any(int threads, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        return any(threads, list, predicate, 1);
    }

    /**
     * Returns whether any of values satisfies predicate, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param step      step size.
     * @param <T>       value type.
     * @return whether any value under consideration satisfies predicate or {@code false}, if no values are given.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> boolean any(int threads, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {
        return !all(threads, list, predicate.negate(), step);
    }

    /**
     * Returns number of values satisfying predicate.
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param <T>       value type.
     * @return number of values satisfying predicate.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> int count(int threads, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        return count(threads, list, predicate, 1);
    }

    /**
     * Returns number of values satisfying predicate, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param step      step size.
     * @param <T>       value type.
     * @return number of values under consideration satisfying predicate.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> int count(int threads, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {
        return runFunction(
                threads,
                step,
                list,
                () -> 0,
                (r, t) -> r + (predicate.test(t) ? 1 : 0),
                l -> l.stream().mapToInt(r -> Integer.parseInt(r.toString())).sum()
        );
    }

    /**
     * Filters values by predicate.
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param <T>       value type.
     * @return list of values satisfying given predicate. Order of values is preserved.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> List<T> filter(int threads, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        return filter(threads, list, predicate, 1);
    }

    /**
     * Filters values by predicate, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads   number of concurrent threads.
     * @param list      list of values to test.
     * @param predicate test predicate.
     * @param step      step size.
     * @param <T>       value type.
     * @return list of values under consideration satisfying given predicate. Order of values is preserved.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T> List<T> filter(int threads, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {
        return runFunction(
                threads,
                step,
                list,
                ArrayList::new,
                (tmpResult, t) -> {
                    if (predicate.test(t)) {
                        tmpResult.add(t);
                    }
                    return tmpResult;
                },
                q -> (ArrayList<T>) q.stream().flatMap(Collection::stream).collect(Collectors.toList())
        );
    }

    /**
     * Maps values.
     *
     * @param threads  number of concurrent threads.
     * @param list     list of values to test.
     * @param function mapper function.
     * @param <T>      value type.
     * @param <U>      value type.
     * @return list of values mapped by given function.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T, U> List<U> map(int threads, List<? extends T> list, Function<? super T, ? extends U> function) throws InterruptedException {
        return map(threads, list, function, 1);
    }

    /**
     * Maps values, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads  number of concurrent threads.
     * @param list     list of values to test.
     * @param function mapper function.
     * @param step     step size.
     * @param <T>      value type.
     * @param <U>      value type.
     * @return list of values under consideration mapped by given function.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public <T, U> List<U> map(int threads, List<? extends T> list, Function<? super T, ? extends U> function, int step) throws InterruptedException {
        return runFunction(
                threads,
                step,
                list,
                ArrayList::new,
                (tmpResult, t) -> {
                    tmpResult.add(function.apply(t));
                    return tmpResult;
                },
                l -> (ArrayList<U>) l.stream().flatMap(Collection::stream).collect(Collectors.toList())
        );
    }

    /**
     * Join values to string.
     *
     * @param threads number of concurrent threads.
     * @param list    list of values to test.
     * @return list of joined results of {@link Object#toString()} call on each value.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public String join(int threads, List<?> list) throws InterruptedException {
        return join(threads, list, 1);
    }

    /**
     * Join values to string, considering only every {@code step}'th element (counting from 0).
     *
     * @param threads number of concurrent threads.
     * @param list    list of values to test.
     * @param step    step size.
     * @return list of joined results of {@link Object#toString()} call on each value under consideration.
     * @throws InterruptedException if executing thread was interrupted.
     */

    @Override
    public String join(int threads, List<?> list, int step) throws InterruptedException {
        return runFunction(
                threads,
                step,
                list,
                () -> "",
                (r, e) -> r + e.toString(),
                l -> String.join("", l)
        );
    }

    private <T> List<T> getSubList(List<T> list, int numberOfThreads, int idx, int step) {
        int startIdx = ((idx * list.size() / numberOfThreads) % step == 0)
                ? (idx * list.size() / numberOfThreads)
                : ((idx * list.size() / numberOfThreads) / step) * (step) + step;
        int endIdx = Math.min((idx + 1) * list.size() / numberOfThreads, list.size());
        return startIdx < endIdx ? list.subList(startIdx, endIdx) : new ArrayList<>();
    }

    private <T> List<List<T>> getAllSubLists(List<T> list, int numberOfThreads, int step) {
        List<List<T>> result = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            result.add(i, getSubList(list, numberOfThreads, i, step));
        }
        return result;
    }

    private <T, R> R getResult(List<? extends T> list, int step, Supplier<R> createNeutralElement, BiFunction<R, T, R> functionToApply) {
        if (list.isEmpty()) {
            return null;
        }
        R result = functionToApply.apply(createNeutralElement.get(), list.get(0));
        for (int i = step; i < list.size(); i += step) {
            result = functionToApply.apply(result, list.get(i));
        }
        return result;
    }

    private <R> List<R> filterNull(List<R> list) {
        return list.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    private <T, R> R functionByThreads(int numberOfTreads,
                                       int step,
                                       List<? extends T> list,
                                       Supplier<R> createNeutralElement,
                                       BiFunction<R, T, R> functionToApply,
                                       Function<List<? extends R>, R> functionForMerging) throws InterruptedException {
        int numberOfThreads = Math.max(1, Math.min(numberOfTreads, list.size()));
        List<Thread> threads = new ArrayList<>();
        List<R> answersOfThreads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final int idx = i;
            answersOfThreads.add(null);
            Thread thread = new Thread(() ->
                    answersOfThreads.set(idx, getResult(getSubList(list, numberOfThreads, idx, step), step, createNeutralElement, functionToApply))
            );
            threads.add(thread);
            thread.start();
        }
        try {
            for (Thread th : threads) {
                th.join();
            }
        } catch (InterruptedException e) {
            threads.forEach(Thread::interrupt);
            threads.forEach(this::joinUntilNot);
            throw e;
        }
        return functionForMerging.apply(filterNull(answersOfThreads));
    }

    private void joinUntilNot(Thread thread) {
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException ignored) {
            }
        }
    }

    private <T, R> R runFunction(int numberOfTreads,
                                 int step,
                                 List<? extends T> list,
                                 Supplier<R> createNeutralElement,
                                 BiFunction<R, T, R> functionToApply,
                                 Function<List<? extends R>, R> functionForMerging) throws InterruptedException {
        int numberOfThreads = Math.max(1, Math.min(numberOfTreads, list.size()));
        return Objects.isNull(mapper)
                ? functionByThreads(numberOfThreads, step, list, createNeutralElement, functionToApply, functionForMerging)
                : functionForMerging.apply(
                filterNull(mapper.map(sublist -> getResult(sublist, step, createNeutralElement, functionToApply), getAllSubLists(list, numberOfThreads, step)))
        );
    }
}