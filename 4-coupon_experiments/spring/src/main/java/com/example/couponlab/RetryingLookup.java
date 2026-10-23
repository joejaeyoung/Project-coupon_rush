package com.example.couponlab;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** A polled record stays owned by this retry loop until its metadata can be read. */
final class RetryingLookup {
    private RetryingLookup() {}
    static <T> Optional<T> load(Supplier<T> lookup, BooleanSupplier running,
                               Consumer<RuntimeException> reportFailure, Runnable backoff) {
        while(running.getAsBoolean()) {
            try { return Optional.of(lookup.get()); }
            catch(RuntimeException error) {
                // The metrics store may be the same unavailable database as the lookup.
                try { reportFailure.accept(error); } catch(RuntimeException ignored) {}
                backoff.run();
            }
        }
        return Optional.empty();
    }
}
