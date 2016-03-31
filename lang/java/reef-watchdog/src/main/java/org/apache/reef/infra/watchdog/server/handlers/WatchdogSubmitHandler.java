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
package org.apache.reef.infra.watchdog.server.handlers;

import org.apache.commons.io.IOUtils;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.infra.watchdog.WatchdogJobConfiguration;
import org.apache.reef.infra.watchdog.exceptions.JobConfigurationException;
import org.apache.reef.infra.watchdog.parameters.MaxNumEvaluatorsPerJob;
import org.apache.reef.infra.watchdog.parameters.WatchdogServerAddress;
import org.apache.reef.infra.watchdog.parameters.WatchdogServerPort;
import org.apache.reef.infra.watchdog.server.Job;
import org.apache.reef.infra.watchdog.server.JobContainer;
import org.apache.reef.infra.watchdog.utils.Constants;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by anchung on 3/28/2016.
 */
public final class WatchdogSubmitHandler extends AbstractHandler {
  private static final Logger LOG = Logger.getLogger(WatchdogSubmitHandler.class.getName());

  private final JobContainer jobContainer;
  private final int maxNumEvaluatorsPerJob;
  private final ConfigurationSerializer serializer;
  private final String address;
  private final int port;

  @Inject
  private WatchdogSubmitHandler(final JobContainer jobContainer,
                                final ConfigurationSerializer serializer,
                                @Parameter(WatchdogServerAddress.class) final String address,
                                @Parameter(WatchdogServerPort.class) final int port,
                                @Parameter(MaxNumEvaluatorsPerJob.class) final int maxNumEvaluatorsPerJob) {
    this.jobContainer = jobContainer;
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
    if (!httpServletRequest.getMethod().equals(HttpMethods.POST)) {
      // TODO[JIRA]: set response.
      return;
    }

    try {
      final Configuration driverConfig = Configurations.merge(
          validateSubmitJobInput(httpServletRequest), getWatchdogJobConfiguration());

      final String id = Constants.TEMP_ID;
      if (jobContainer.addJob(id, new Job(id))) {
        // TODO[JIRA]: set response.
        // Already submitted. Temporary to only submit once. Set response.
        return;
      }

      final DriverLauncher launcher = DriverLauncher.getLauncher(getRuntimeConfiguration());
      launcher.run(driverConfig);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private Configuration getWatchdogJobConfiguration() {
    return WatchdogJobConfiguration.CONF
        .set(WatchdogJobConfiguration.WATCHDOG_JOB_ID, 1)
        .set(WatchdogJobConfiguration.WATCHDOG_ADDRESS, address)
        .set(WatchdogJobConfiguration.WATCHDOG_PORT, port)
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
