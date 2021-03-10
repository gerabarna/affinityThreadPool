package hu.gerab.concurrent.taskAffinity;

@FunctionalInterface
public interface InterruptibleFunction<T, R> {

    R apply(T t) throws InterruptedException;
}
