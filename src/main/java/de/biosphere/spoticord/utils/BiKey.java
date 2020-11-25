package de.biosphere.spoticord.utils;

import java.util.Objects;

public class BiKey<T, U> {

    private final T first;
    private final U second;

    public BiKey(final T first, final U second) {
        this.first = first;
        this.second = second;
    }

    public T getFirst() {
        return first;
    }

    public U getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return "BiKey{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

        @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BiKey<?, ?> biKey = (BiKey<?, ?>) o;

        if (!Objects.equals(first, biKey.first)) return false;
        return Objects.equals(second, biKey.second);
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    public static <T, U, V> BiKey<T, U> of(T first, U second) {
        return new BiKey<>(first, second);
    }

}
