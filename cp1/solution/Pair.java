package cp1.solution;

public class Pair<T1, T2> {
    private final T1 first;
    private final T2 second;

    public Pair(T1 f, T2 s) {
        first = f;
        second = s;
    }

    public T1 first() {
        return first;
    }

    public T2 second() {
        return second;
    }
}
