/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.dataset.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Validates the fix for <a href="https://github.com/apache/arrow-java/issues/1067">#1067</a>: JVM
 * hangs after reading HDFS files via Arrow Dataset API due to non-daemon native threads.
 *
 * <p>When Arrow C++ accesses HDFS via libhdfs, the C library attaches native threads to the JVM via
 * {@code AttachCurrentThread}. These threads are non-daemon by default. They block on HDFS IPC
 * connections managed by cached {@code FileSystem} instances, preventing the JVM from exiting after
 * {@code main()} returns.
 *
 * <p>These tests fork a child JVM that simulates this behavior: a non-daemon thread holds an HDFS
 * connection. Without cleanup, the JVM hangs. With {@code FileSystem.close()} on the cached
 * instance (the same mechanism used by {@link FileSystemDatasetFactory#close()}), the connection is
 * closed and the thread exits, allowing the JVM to terminate.
 */
public class TestHdfsFileSystemCleanup {

  private static final int CHILD_TIMEOUT_SECONDS = 10;

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  @TempDir static File clusterDir;

  @BeforeAll
  static void startCluster() throws IOException {
    conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterDir.getAbsolutePath());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Without cleanup: a child JVM with a non-daemon thread holding an HDFS connection hangs after
   * main() returns because the non-daemon thread keeps the JVM alive.
   */
  @Test
  void testJvmHangsWithoutCleanup() throws Exception {
    Process child = forkChildProcess(false);
    boolean exited = child.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (!exited) {
      child.destroyForcibly();
    }
    assertFalse(exited, "JVM should hang when non-daemon HDFS thread is not cleaned up");
  }

  /**
   * With cleanup: closing the specific cached Hadoop FileSystem (the same mechanism used by {@link
   * FileSystemDatasetFactory#close()}) closes the HDFS connections, causing the non-daemon thread
   * to exit and allowing the JVM to terminate normally.
   */
  @Test
  void testJvmExitsWithCleanup() throws Exception {
    Process child = forkChildProcess(true);
    boolean exited = child.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (!exited) {
      child.destroyForcibly();
    }
    assertTrue(
        exited, "JVM should exit when FileSystemDatasetFactory cleanup runs before return");
    assertEquals(0, child.exitValue(), "Child process should exit cleanly (exit code 0)");
  }

  private Process forkChildProcess(boolean withCleanup) throws IOException {
    String classpath = System.getProperty("java.class.path");
    int port = cluster.getNameNodePort();
    ProcessBuilder pb =
        new ProcessBuilder(
            ProcessHandle.current().info().command().orElse("java"),
            "-cp",
            classpath,
            HdfsClientSimulator.class.getName(),
            String.valueOf(port),
            String.valueOf(withCleanup));
    pb.redirectError(ProcessBuilder.Redirect.DISCARD);
    pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
    return pb.start();
  }

  /**
   * Simulates the libhdfs behavior in a standalone JVM.
   *
   * <p>When Arrow C++ uses libhdfs to access HDFS, native threads are attached to the JVM via
   * {@code AttachCurrentThread}. These threads are non-daemon by default and block on HDFS IPC
   * connections. This class simulates that by creating a non-daemon thread that periodically polls
   * the HDFS filesystem. Without closing the cached FileSystem, the thread keeps the JVM alive
   * indefinitely after {@code main()} returns.
   */
  public static class HdfsClientSimulator {
    public static void main(String[] args) throws Exception {
      int port = Integer.parseInt(args[0]);
      boolean withCleanup = Boolean.parseBoolean(args[1]);

      Configuration conf = new Configuration();
      String hdfsUri = "hdfs://localhost:" + port;
      conf.set("fs.defaultFS", hdfsUri);

      // Get a cached FileSystem (same as libhdfs does internally)
      FileSystem fs = FileSystem.get(conf);
      fs.exists(new Path("/"));

      // Simulate a non-daemon thread attached via AttachCurrentThread that
      // blocks on the HDFS IPC connection. In the real libhdfs scenario,
      // these are native threads that process RPC responses.
      Thread connectionThread =
          new Thread(
              () -> {
                while (true) {
                  try {
                    fs.getFileStatus(new Path("/"));
                    Thread.sleep(500);
                  } catch (Exception e) {
                    // FileSystem was closed or thread interrupted — exit
                    break;
                  }
                }
              });
      connectionThread.setDaemon(false);
      connectionThread.setName("simulated-libhdfs-ipc-thread");
      connectionThread.start();

      if (withCleanup) {
        // Use the same helper invoked by FileSystemDatasetFactory.close().
        // Closing the cached FileSystem terminates IPC connections, causing
        // the non-daemon thread to get an IOException and exit.
        FileSystemDatasetFactory.closeHadoopFileSystemsIfHdfs(hdfsUri);
        connectionThread.join(5000);
      }

      // main() returns. Without cleanup, the non-daemon thread keeps the JVM alive.
    }
  }
}
