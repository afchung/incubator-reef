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
package org.apache.reef.infra.watchdog;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.reef.infra.watchdog.example.HelloDriver;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by anchung on 3/28/2016.
 */
public final class Launcher implements AutoCloseable {
  private static final int HTTP_PORT = 8080;
  private static final int MAX_NUM_EVALUATORS_PER_JOB = 10;
  private static final Logger LOG = Logger.getLogger(Launcher.class.getName());

  private static CountDownLatch latch = new CountDownLatch(1);

  private final HttpServer server;

  @Inject
  private Launcher(final HttpServer server) {
    this.server = server;
    try {
      this.server.start();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(final String[] args) throws InterruptedException {
    // TODO[JIRA]: Get num evaluators per process.
    // TODO[JIRA]: Get Host/Port.

    final Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        runServer();
      }
    });

    serverThread.start();

    final Thread requestThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.log(Level.SEVERE, "HELLO1");
          latch.await();
          final HttpClient client = HttpClients.createDefault();
          final HttpHost host = new HttpHost(InetAddress.getLoopbackAddress().getHostAddress(), HTTP_PORT);
          final HttpPost request = new HttpPost("/submit/");
          final String configStr = Tang.Factory.getTang().newInjector().getInstance(ConfigurationSerializer.class)
              .toString(HelloDriver.getDriverConfiguration());
          request.setEntity(new ByteArrayEntity(configStr.getBytes("UTF8")));
          client.execute(host, request);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });

    requestThread.start();
    requestThread.join();

    serverThread.join();
  }

  private static void runServer() {
    final Injector injector = Tang.Factory.getTang().newInjector(
        WatchdogConfiguration.CONF
            .set(WatchdogConfiguration.MAX_NUM_EVALUATORS_PER_JOB, MAX_NUM_EVALUATORS_PER_JOB)
            .set(WatchdogConfiguration.WATCHDOG_ADDRESS,
                InetAddress.getLoopbackAddress().getHostAddress())
            .set(WatchdogConfiguration.WATCHDOG_PORT, HTTP_PORT)
            .build());
    try (final Launcher launcher = injector.getInstance(Launcher.class)){
      latch.countDown();
      launcher.join();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void join() throws InterruptedException {
    server.join();
  }

  @Override
  public void close() throws Exception {
    server.close();
  }
}
