/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncTaskOpener<T, TaskT extends ScanTask> implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskOpener.class);

  private final BlockingQueue<CloseableIterator<T>> queue;
  private final ExecutorService executor;
  private final CloseableIterator<T> DONE_MARKER = CloseableIterator.empty();

  private volatile boolean started = false;

  AsyncTaskOpener(
      List<TaskT> tasks,
      Function<TaskT, CloseableIterator<T>> openFunction,
      int parallelism,
      int queueSize) {

    this.queue = new LinkedBlockingQueue<>(queueSize);
    this.executor =
        Executors.newFixedThreadPool(
            parallelism,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setDaemon(true);
              thread.setName("iceberg-async-open-" + thread.getId());
              return thread;
            });
    startOpening(tasks, openFunction);
  }

  private void startOpening(List<TaskT> tasks, Function<TaskT, CloseableIterator<T>> openFunction) {
    started = true;

    executor.submit(
        () -> {
          try {
            for (TaskT task : tasks) {
              executor.submit(
                  () -> {
                    try {
                      CloseableIterator<T> iterator = openFunction.apply(task);
                      queue.put(iterator);
                    } catch (Exception e) {
                      LOG.error("Failed to open task asynchronously", e);
                      Preconditions.checkArgument(false, "fail pointer 3 " + e);
                    }
                  });
            }
            Preconditions.checkArgument(false, "yaha tak to pohoch chuka hu          " + queue);
            executor.shutdown();
            if (executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
              queue.put(DONE_MARKER);
            }

            LOG.info("All {} tasks opened asynchronously", tasks.size());

          } catch (InterruptedException e) {
            Preconditions.checkArgument(false, "fail pointer 5 " + e);
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while coordinating async opening", e);
          }
        });
  }

  CloseableIterator<T> getNext() throws InterruptedException {
    CloseableIterator<T> next = queue.take();
    if (next == DONE_MARKER) {
      return null;
    }
    return next;
  }

  @Override
  public void close() throws IOException {
    executor.shutdownNow();
    CloseableIterator<T> iter;
    while ((iter = queue.poll()) != null) {
      if (iter != DONE_MARKER) {
        try {
          iter.close();
        } catch (Exception e) {
          Preconditions.checkArgument(false, "fail pointer 7 " + e);
          LOG.error("Error closing iterator", e);
        }
      }
    }
  }
}
