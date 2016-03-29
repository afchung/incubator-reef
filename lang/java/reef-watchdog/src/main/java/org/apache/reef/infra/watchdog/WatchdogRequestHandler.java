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

import org.apache.commons.io.IOUtils;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.infra.watchdog.exceptions.JobConfigurationException;
import org.apache.reef.infra.watchdog.parameters.MaxNumEvaluatorsPerJob;
import org.apache.reef.infra.watchdog.parameters.WatchdogAddress;
import org.apache.reef.infra.watchdog.parameters.WatchdogPort;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.mortbay.jetty.HttpMethods;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by anchung on 3/28/2016.
 */
public class WatchdogRequestHandler extends AbstractHandler {
  private static final Logger LOG = Logger.getLogger(WatchdogRequestHandler.class.getName());

  private final AtomicBoolean jobSubmitted = new AtomicBoolean(false);
  private final int maxNumEvaluatorsPerJob;
  private final ConfigurationSerializer serializer;
  private final String address;
  private final int port;

  @Inject
  private WatchdogRequestHandler(final ConfigurationSerializer serializer,
                                 @Parameter(WatchdogAddress.class) final String address,
                                 @Parameter(WatchdogPort.class) final int port,
                                 @Parameter(MaxNumEvaluatorsPerJob.class) int maxNumEvaluatorsPerJob) {
    this.maxNumEvaluatorsPerJob = maxNumEvaluatorsPerJob;
    this.serializer = serializer;
    this.address = address;
    this.port = port;
  }

  @Override
  public void handle(final String requestTarget,
                     final HttpServletRequest httpServletRequest,
                     final HttpServletResponse httpServletResponse,
                     final int dispatchMode) throws IOException, ServletException {
    LOG.log(Level.SEVERE, "HELLO3");
    if (httpServletRequest.getMethod() != HttpMethods.POST) {
      // TODO: set response.
      return;
    }

    if (jobSubmitted.get()) {
      // TODO: set response.
      return;
    }

    try {
      final Configuration driverConfig = Configurations.merge(
          validateSubmitJobInput(httpServletRequest), getWatchdogConfiguration());

      if (jobSubmitted.getAndSet(true)) {
        // TODO: set response.
        // Already submitted. Temporary to only submit once. Set response.
        return;
      }

      final DriverLauncher launcher = DriverLauncher.getLauncher(getRuntimeConfiguration());
      launcher.run(driverConfig);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Configuration getWatchdogConfiguration() {
    return WatchdogConfiguration.CONF
        .set(WatchdogConfiguration.WATCHDOG_ADDRESS, address)
        .set(WatchdogConfiguration.WATCHDOG_PORT, port)
        .build();
  }

  private Configuration validateSubmitJobInput(final HttpServletRequest request)
      throws IOException, JobConfigurationException {
    final int contentLength = request.getContentLength();

    if (contentLength == 0) {
      throw new JobConfigurationException();
    }

    final String driverConfigStr = IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
    return this.serializer.fromString(driverConfigStr);
  }

  private Configuration getRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, maxNumEvaluatorsPerJob)
        .build();
  }
}
