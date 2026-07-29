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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for the manifest EagerInputFile read path across storage type and manifest size.
 *
 * <p>It complements {@link ManifestBenchmark} by adding a local-filesystem case and a size sweep
 * across the 1 MB eager-fetch gate. It uses the real eager path: the {@code
 * io.manifest.eager-fetch-enabled} property is set on the FileIO, so {@code ManifestFiles.read}
 * wraps the manifest InputFile in {@code EagerInputFile} exactly as production does.
 *
 * <p>It adds the two dimensions the original S3-only run does not cover:
 *
 * <ul>
 *   <li>{@code storage}: {@code local} (HadoopFileIO) vs {@code s3} (S3FileIO). On a local
 *       filesystem there is no object-store round trip to collapse, so this measures whether eager
 *       fetch adds overhead (a whole-file read plus copy) when it cannot help.
 *   <li>{@code entries}: a manifest-size sweep that straddles the fixed 1 MB eager gate, so the
 *       gate is exercised on both sides (above the gate, eager fetch is inactive and has no
 *       effect).
 * </ul>
 *
 * <p>The actual manifest byte size is printed per trial so the size mapping is verified, not
 * assumed.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 6)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EagerFetchScalingBenchmark {

  private static final int NUM_COLS = 50;
  // Set these to your own S3 bucket and region before running.
  private static final String S3_BUCKET = "iceberg-manifest-benchmark";
  private static final String S3_REGION = "us-east-1";

  @Param({"local", "s3"})
  private String storage;

  @Param({"4_AVRO", "4_PARQUET"})
  private String versionFormat;

  // Entry counts chosen so the manifests straddle the 1 MB eager gate (roughly a third, near, and
  // above 1 MB; exact size depends on format). The real byte size is printed per trial at setup.
  @Param({"300", "675", "1350"})
  private int entries;

  @Param({"false", "true"})
  private String zEagerFetch;

  private int formatVersion;
  private FileFormat fileFormat;
  private PartitionSpec spec;
  private Map<Integer, PartitionSpec> specsById;
  private FileIO fileIO;
  private String baseDir;
  private ManifestFile readManifest;
  private long manifestBytes;

  @Setup(Level.Trial)
  public void setupTrial() {
    String[] parts = versionFormat.split("_", 2);
    this.formatVersion = Integer.parseInt(parts[0]);
    this.fileFormat = FileFormat.fromString(parts[1]);
    this.spec = PartitionSpec.unpartitioned();
    this.specsById = ImmutableMap.of(spec.specId(), spec);

    List<DataFile> dataFiles = ManifestBenchmarkUtil.generateDataFiles(spec, entries, NUM_COLS);

    Map<String, String> props =
        ImmutableMap.of(CatalogProperties.IO_MANIFEST_EAGER_FETCH_ENABLED, zEagerFetch);

    if ("s3".equalsIgnoreCase(storage)) {
      S3FileIO s3 = new S3FileIO();
      Map<String, String> s3Props =
          ImmutableMap.of(
              "client.region",
              S3_REGION,
              CatalogProperties.IO_MANIFEST_EAGER_FETCH_ENABLED,
              zEagerFetch);
      s3.initialize(s3Props);
      this.fileIO = s3;
      this.baseDir = "s3://" + S3_BUCKET + "/eager-scaling/" + UUID.randomUUID();
    } else {
      HadoopFileIO local = new HadoopFileIO(new Configuration());
      local.initialize(props);
      this.fileIO = local;
      try {
        this.baseDir =
            java.nio.file.Files.createTempDirectory("eager-scaling-").toUri().toString();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    String manifestPath =
        String.format(
            Locale.ROOT, "%s/%s", baseDir, fileFormat.addExtension("manifest-" + UUID.randomUUID()));
    OutputFile out = fileIO.newOutputFile(manifestPath);
    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, spec, out, 1L);
    try (ManifestWriter<DataFile> w = writer) {
      for (DataFile file : dataFiles) {
        w.add(file);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.readManifest = writer.toManifestFile();
    this.manifestBytes = readManifest.length();

    boolean gateFires =
        "true".equals(zEagerFetch) && manifestBytes > 0 && manifestBytes <= 1024L * 1024L;
    System.out.println(
        "[fixture] storage="
            + storage
            + " versionFormat="
            + versionFormat
            + " entries="
            + entries
            + " manifestBytes="
            + manifestBytes
            + " eager="
            + zEagerFetch
            + " gateFires="
            + gateFires);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() {
    if ("local".equalsIgnoreCase(storage) && baseDir != null && baseDir.startsWith("file:")) {
      try {
        ManifestBenchmarkUtil.cleanDir(
            java.nio.file.Paths.get(java.net.URI.create(baseDir)).toString());
      } catch (Exception ignored) {
        // best effort
      }
    }
    try {
      fileIO.close();
    } catch (Exception ignored) {
      // best effort
    }
  }

  @Benchmark
  @Threads(1)
  public void readManifest(Blackhole blackhole) throws IOException {
    try (CloseableIterator<DataFile> it =
        ManifestFiles.read(readManifest, fileIO, specsById).iterator()) {
      while (it.hasNext()) {
        blackhole.consume(it.next());
      }
    }
  }
}
