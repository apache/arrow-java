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

/** Regression test for <a href="https://github.com/apache/arrow-java/issues/1067">#1067</a>. */
public class TestHdfsFileSystemCleanup {

  private static final int CHILD_TIMEOUT_SECONDS = 10;

  private static MiniDFSCluster cluster;

  @TempDir static File clusterDir;

  @BeforeAll
  static void startCluster() throws IOException {
    Configuration conf = new Configuration();
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

  @Test
  void testJvmHangsWithoutCleanup() throws Exception {
    Process child = forkChildProcess(false);
    assertFalse(waitForExit(child), "JVM should hang when HDFS is not cleaned up");
  }

  @Test
  void testJvmExitsWithCleanup() throws Exception {
    Process child = forkChildProcess(true);
    assertTrue(
        waitForExit(child), "JVM should exit when FileSystemDatasetFactory cleanup runs");
    assertEquals(0, child.exitValue(), "Child process should exit cleanly (exit code 0)");
  }

  private boolean waitForExit(Process child) throws InterruptedException {
    boolean exited = child.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (!exited) {
      child.destroyForcibly();
    }
    return exited;
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

  /** Simulates libhdfs leaving a non-daemon thread attached to an HDFS connection. */
  public static class HdfsClientSimulator {
    public static void main(String[] args) throws Exception {
      int port = Integer.parseInt(args[0]);
      boolean withCleanup = Boolean.parseBoolean(args[1]);

      Configuration conf = new Configuration();
      String hdfsUri = "hdfs://localhost:" + port;
      conf.set("fs.defaultFS", hdfsUri);

      FileSystem fs = FileSystem.get(conf);
      fs.exists(new Path("/"));

      Thread connectionThread =
          new Thread(
              () -> {
                while (true) {
                  try {
                    fs.getFileStatus(new Path("/"));
                    Thread.sleep(500);
                  } catch (Exception e) {
                    break;
                  }
                }
              },
              "simulated-libhdfs-ipc-thread");
      connectionThread.setDaemon(false);
      connectionThread.start();

      if (withCleanup) {
        FileSystemDatasetFactory.closeHadoopFileSystemsIfHdfs(hdfsUri);
        connectionThread.join(5000);
      }
    }
  }
}
