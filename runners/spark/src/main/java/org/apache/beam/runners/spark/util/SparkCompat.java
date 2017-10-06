/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.spark.util;

import java.util.Iterator;
import scala.Tuple2;

/** A class with helper functions and utilities for compatibility between spark versions. */
public final class SparkCompat {

  private SparkCompat() {}

  /**
   * A class that wraps an {@link Iterator} and exposes it as an {@link Iterable} or wraps an {@link
   * Iterable} and exposes it as an {@link Iterator}. In the second case it creates an Iterator by
   * calling the iterable.Iterator method and then respects its semantics.
   *
   * <p>Notice that both uses cannot be combined, so it is up to the user to respect the logic since
   * we cannot ensure this.
   *
   * <p>This is an internal class used to ensure compatibility between spark versions and should not
   * be used for other purposes.
   */
  public static class IterableIterator<T> implements Iterable<T>, Iterator<T> {

    private final Iterable<T> delegate;
    private final Iterator<T> iter;

    public IterableIterator(Iterable<T> delegate) {
      this.delegate = delegate;
      this.iter = iterator();
    }

    public IterableIterator(final Iterator<T> iter) {
      this.delegate =
          new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
              return iter;
            }
          };
      this.iter = iter;
    }

    @Override
    public Iterator<T> iterator() {
      // This is done explicitly to respect the semantics of Iterable.
      // In case someone wants to ask for new iterators from an wrapped Iterable more than once.
      // However if we are wrapping an Iterator it won't produce a new Iterator but its reference.
      return delegate.iterator();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public T next() {
      return iter.next();
    }

    @Override
    public void remove() {
      iter.remove();
    }
  }

  /**
   * A SparkCompat.IterableIterator compatible version of {@link
   * org.apache.spark.api.java.function.FlatMapFunction}.
   */
  public interface FlatMapFunction<K, V>
      extends org.apache.spark.api.java.function.FlatMapFunction<K, V> {

    @Override
    SparkCompat.IterableIterator<V> call(K k) throws Exception;
  }

  /**
   * A SparkCompat.IterableIterator compatible version of {@link
   * org.apache.spark.api.java.function.PairFlatMapFunction}.
   */
  public interface PairFlatMapFunction<T, K, V>
      extends org.apache.spark.api.java.function.PairFlatMapFunction<T, K, V> {

    @Override
    SparkCompat.IterableIterator<Tuple2<K, V>> call(T t) throws Exception;
  }
}
