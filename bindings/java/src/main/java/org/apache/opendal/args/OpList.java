package org.apache.opendal.args;

import java.util.Optional;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class OpList<T> {
    private final String path;
    private final long limit;
    private final Optional<String> startAfter;
    private final Optional<String> delimiter;
    private final Function<OpList<T>, T> func;

    public T call() {
        return func.apply(this);
    }

    private OpList(
            @NonNull String path,
            long limit,
            Optional<String> startAfter,
            Optional<String> delimiter,
            @NonNull Function<OpList<T>, T> func) {
        this.path = path;
        this.limit = limit;
        this.startAfter = startAfter;
        this.delimiter = delimiter;
        this.func = func;
    }

    public static <T> OpListBuilder<T> builder(String path, Function<OpList<T>, T> func) {
        return new OpListBuilder<>(path, func);
    }

    @ToString
    public static class OpListBuilder<T> {
        private final String path;
        private final Function<OpList<T>, T> func;
        
        private long limit;
        private Optional<String> startAfter;
        private Optional<String> delimiter;

        private OpListBuilder(String path, @NonNull Function<OpList<T>, T> func) {
            this.limit = -1L;
            this.startAfter = Optional.empty();
            this.delimiter = Optional.empty();
            this.path = path;
            this.func = func;
        }

        public OpListBuilder<T> limit(long limit) {
            this.limit = limit;
            return this;
        }

        public OpListBuilder<T> startAfter(String startAfter) {
            this.startAfter = Optional.ofNullable(startAfter);
            return this;
        }

        public OpListBuilder<T> delimiter(String delimiter) {
            this.delimiter = Optional.ofNullable(delimiter);
            return this;
        }

        public OpList<T> build() {
            return new OpList<>(path, limit, startAfter, delimiter, func);
        }
    }
}
