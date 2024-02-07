package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();
  private final ZonedDateTime startTime;

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    this.startTime = ZonedDateTime.now(clock);
  }

  @Override
  public <T> T wrap(Class<T> klass, T delegate) {
    Objects.requireNonNull(klass);

    // Check whether delegate has any @Profiled method, if not throw error
    if (Arrays.stream(klass.getDeclaredMethods())
            .filter(m -> m.isAnnotationPresent(Profiled.class))
            .toList()
            .isEmpty())
      throw new IllegalArgumentException("Class %s doesn't contain any methods with @Profiled annotation".formatted(klass));

    InvocationHandler handler = new ProfilingMethodInterceptor(this.clock, state, delegate);
    Object proxy = Proxy.newProxyInstance(klass.getClassLoader(),
            new Class<?>[]{klass},
            handler);

    return klass.cast(proxy);
  }

  @Override
  public void writeData(Path path) {
    try (Writer writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      writeData(writer);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void writeData(Writer writer) throws IOException {
    writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
    writer.write(System.lineSeparator());
    state.write(writer);
    writer.write(System.lineSeparator());
  }
}
