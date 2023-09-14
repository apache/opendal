package org.apache.opendal;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestHelper {
    public static <T, U> BiFunction<T, Throwable, U> assertAsyncOpenDALException(OpenDALException.Code code) {
        return (result, throwable) -> {
            assertThat(result).isNull();
            final Throwable t = stripException(throwable, CompletionException.class);
            assertThat(t).isInstanceOf(OpenDALException.class);
            assertThat(((OpenDALException) t).getCode()).isEqualTo(code);
            return null;
        };
    }

    public static Throwable stripException(Throwable throwableToStrip, Class<? extends Throwable> typeToStrip) {
        while (typeToStrip.isAssignableFrom(throwableToStrip.getClass()) && throwableToStrip.getCause() != null) {
            throwableToStrip = throwableToStrip.getCause();
        }
        return throwableToStrip;
    }
}
