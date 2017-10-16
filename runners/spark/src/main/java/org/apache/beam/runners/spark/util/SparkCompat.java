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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.SparkEnv;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockResult;
import org.apache.spark.storage.StorageLevel;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

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

  /**
   * A spark 1/2 compatible version of the Spark's {@link org.apache.spark.storage.BlockManager}.
   */
  abstract static class BlockManager {
    static final BlockId WATERMARKS_BLOCK_ID = BlockId.apply("broadcast_0WATERMARKS");

    void removeBlock(BlockId blockId, boolean tellMaster) {
      final SparkEnv sparkEnv = SparkEnv.get();
      if (sparkEnv != null) {
        final org.apache.spark.storage.BlockManager blockManager = sparkEnv.blockManager();
        blockManager.removeBlock(WATERMARKS_BLOCK_ID, true);
      }
    }

    abstract void putSingle(Map<Integer, GlobalWatermarkHolder.SparkWatermarks> empty);

    abstract Option<BlockResult> getRemote();
  }

  /**
   * A Factory Method that detects the version of Spark and creates and appropriate BlockManager
   * instance.
   */
  static BlockManager newBlockManager() {
    final String sparkVersion = org.apache.spark.package$.MODULE$.SPARK_VERSION();
    return sparkVersion.startsWith("2.") ? new Spark2BlockManager() : new Spark1BlockManager();
  }

  private static class Spark2BlockManager extends BlockManager {

    private static final ClassTag<Map> WATERMARKS_TAG =
        scala.reflect.ClassManifestFactory.fromClass(Map.class);

    private final Method putSingle;
    private final Method getRemote;

    private Spark2BlockManager() {
      try {
        this.putSingle =
            org.apache.spark.storage.BlockManager.class.getMethod(
                "putSingle",
                BlockId.class,
                Object.class,
                StorageLevel.class,
                boolean.class,
                ClassTag.class);
        this.getRemote =
            org.apache.spark.storage.BlockManager.class.getMethod(
                "get", BlockId.class, ClassTag.class);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void putSingle(Map<Integer, GlobalWatermarkHolder.SparkWatermarks> empty) {
      try {
        putSingle.invoke(
            SparkEnv.get().blockManager(),
            new Object[] {
              WATERMARKS_BLOCK_ID, empty, StorageLevel.MEMORY_ONLY(), true, WATERMARKS_TAG
            });
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException(e.getTargetException());
      }
    }

    @Override
    public Option<BlockResult> getRemote() {
      try {
        return Option.class.cast(
            getRemote.invoke(
                SparkEnv.get().blockManager(), new Object[] {WATERMARKS_BLOCK_ID, WATERMARKS_TAG}));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException(e.getTargetException());
      }
    }
  }

  private static class Spark1BlockManager extends BlockManager {

    private final Method putSingle;
    private final Method getRemote;

    private Spark1BlockManager() {
      try {
        this.putSingle =
            org.apache.spark.storage.BlockManager.class.getMethod(
                "putSingle", BlockId.class, Object.class, StorageLevel.class, boolean.class);
        this.getRemote =
            org.apache.spark.storage.BlockManager.class.getMethod("getRemote", BlockId.class);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void putSingle(Map<Integer, GlobalWatermarkHolder.SparkWatermarks> empty) {
      try {
        putSingle.invoke(
            SparkEnv.get().blockManager(),
            new Object[] {WATERMARKS_BLOCK_ID, empty, StorageLevel.MEMORY_ONLY(), true});
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException(e.getTargetException());
      }
    }

    @Override
    public Option<BlockResult> getRemote() {
      try {
        return Option.class.cast(
            getRemote.invoke(SparkEnv.get().blockManager(), new Object[] {WATERMARKS_BLOCK_ID}));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException(e.getTargetException());
      }
    }
  }
}
