package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object delegate;

  private final ProfilingState state;

  ProfilingMethodInterceptor(Clock clock, ProfilingState state, Object delegate) {
    this.clock = Objects.requireNonNull(clock);
    this.state = state;
    this.delegate = delegate;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getDeclaringClass().equals(Object.class) ||
            method.getAnnotation(Profiled.class) == null){
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e){
        throw e.getTargetException();
      } catch (IllegalAccessException e){
        throw new RuntimeException(e);
      }
    }

    Instant before = clock.instant();
    Object result;
    try {
      result = method.invoke(delegate, args);
      this.state.record(delegate.getClass(), method, Duration.between(before, clock.instant()));
    } catch (InvocationTargetException e){
      this.state.record(delegate.getClass(), method, Duration.between(before, clock.instant()));
      throw e.getTargetException();
    } catch (IllegalAccessException e){
      this.state.record(delegate.getClass(), method, Duration.between(before, clock.instant()));
      throw new RuntimeException(e);
    } catch (UndeclaredThrowableException e){
      this.state.record(delegate.getClass(), method, Duration.between(before, clock.instant()));
      throw e.getUndeclaredThrowable();
    }

    return result;
  }
}
