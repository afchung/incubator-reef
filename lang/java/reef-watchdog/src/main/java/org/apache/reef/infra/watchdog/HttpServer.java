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

import org.apache.reef.infra.watchdog.parameters.WatchdogServerPort;
import org.apache.reef.infra.watchdog.server.handlers.WatchdogHeartbeatHandler;
import org.apache.reef.infra.watchdog.server.handlers.WatchdogPidRegisterHandler;
import org.apache.reef.infra.watchdog.server.handlers.WatchdogSubmitHandler;
import org.apache.reef.infra.watchdog.server.handlers.WatchdogStatusHandler;
import org.apache.reef.tang.annotations.Parameter;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;

import javax.inject.Inject;
import java.net.InetAddress;
import java.util.logging.Logger;

/**
 * Created by anchung on 3/31/2016.
 */
public final class HttpServer implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(HttpServer.class.getName());
  private final Server server;

  @Inject
  private HttpServer(final WatchdogSubmitHandler requestHandler,
                     final WatchdogHeartbeatHandler heartbeatHandler,
                     final WatchdogStatusHandler statusHandler,
                     final WatchdogPidRegisterHandler pidRegisterHandler,
                     @Parameter(WatchdogServerPort.class)final int httpPort) {
    this.server = new Server();
    final SocketConnector connector = new SocketConnector();
    connector.setHost(InetAddress.getLoopbackAddress().getHostAddress());
    connector.setPort(httpPort);
    server.addConnector(connector);

    final ContextHandler watchdogRequestHandler = new ContextHandler("/submit");
    watchdogRequestHandler.setHandler(requestHandler);

    final ContextHandler watchdogHeartbeatHandler = new ContextHandler("/heartbeat");
    watchdogHeartbeatHandler.setHandler(heartbeatHandler);

    final ContextHandler watchdogStatusHandler = new ContextHandler("/status");
    watchdogStatusHandler.setHandler(statusHandler);

    final ContextHandler watchdogPidRegisterHandler = new ContextHandler("/pid-register");
    watchdogPidRegisterHandler.setHandler(pidRegisterHandler);

    server.setHandlers(
        new Handler[] {
            watchdogRequestHandler,
            watchdogHeartbeatHandler,
            watchdogStatusHandler,
            watchdogPidRegisterHandler
        });
  }

  void start() throws Exception {
    server.start();
  }

  public void join() throws InterruptedException {
    server.join();
  }

  @Override
  public void close() throws Exception {
    server.stop();
  }
}
