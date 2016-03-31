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
package org.apache.reef.infra.watchdog.driver;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.reef.infra.watchdog.parameters.WatchdogJobId;
import org.apache.reef.infra.watchdog.parameters.WatchdogServerAddress;
import org.apache.reef.infra.watchdog.parameters.WatchdogServerPort;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by anchung on 3/31/2016.
 */
public final class WatchdogHeartbeatManager implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(WatchdogHeartbeatManager.class.getName());
  private final CloseableHttpClient httpClient;
  private final HttpHost watchdogHost;
  private final int watchdogJobId;

  @Inject
  private WatchdogHeartbeatManager(@Parameter(WatchdogServerAddress.class) final String serverAddress,
                                   @Parameter(WatchdogServerPort.class) final int serverPort,
                                   @Parameter(WatchdogJobId.class) final int watchdogJobId) {
    this.httpClient = HttpClients.createDefault();
    this.watchdogHost = new HttpHost(serverAddress, serverPort);
    this.watchdogJobId = watchdogJobId;
  }

  public void start() {
    final Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          final HttpGet getRequest = new HttpGet("/heartbeat/" + WatchdogHeartbeatManager.this.watchdogJobId);
          try {
            LOG.log(Level.SEVERE, "HB112");
            CloseableHttpResponse response = httpClient.execute(watchdogHost, getRequest);
            try {
              HttpEntity entity = response.getEntity();
              if (entity != null) {
                final InputStream instream = entity.getContent();
                try {
                  // do something useful
                } finally {
                  instream.close();
                }
              }
            } finally {
              response.close();
            }

            LOG.log(Level.SEVERE, "HB113");
          } catch (final IOException e) {
            e.printStackTrace();

            // TODO[MY TASK]: fail if retry too many times.
            LOG.log(Level.SEVERE, "HB111");
          } finally {
            try {
              LOG.log(Level.SEVERE, "HB114");
              Thread.sleep(5000);
              LOG.log(Level.SEVERE, "HB115");
            } catch (final InterruptedException e) {
              e.printStackTrace();
              LOG.log(Level.SEVERE, "HB111");
            }
          }

          LOG.log(Level.SEVERE, "HB!");
        }
      }
    });

    t.start();
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.SEVERE, "HB333");
  }
}
