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

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.reef.infra.watchdog.avro.PidRegisterRequest;
import org.apache.reef.infra.watchdog.server.Job;
import org.apache.reef.infra.watchdog.server.JobContainer;
import org.apache.reef.infra.watchdog.server.JobStatus;
import org.mortbay.jetty.HttpMethods;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by anchung on 4/4/2016.
 */
public final class WatchdogPidRegisterHandler extends AbstractHandler {
  private final JobContainer jobContainer;

  @Inject
  private WatchdogPidRegisterHandler(final JobContainer jobContainer) {
    this.jobContainer = jobContainer;
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

    final String pidRegisterRequestStr = IOUtils.toString(httpServletRequest.getInputStream(), StandardCharsets.UTF_8);

    final PidRegisterRequest pidRegisterRequest;
    try {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          PidRegisterRequest.getClassSchema(), pidRegisterRequestStr);
      final SpecificDatumReader<PidRegisterRequest> reader = new SpecificDatumReader<>(PidRegisterRequest.class);
      pidRegisterRequest = reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    if (pidRegisterRequest == null) {
      // TODO[JIRA]: set error response.
      return;
    }

    final Job job = jobContainer.getJob(pidRegisterRequest.getJobId().toString());
    if (job == null) {
      // TODO[JIRA]: set error response.
      return;
    }

    if (!job.setStatus(JobStatus.RUNNING)) {
      // TODO[JIRA]: set error response.
      return;
    }

    // TODO[JIRA]: set success response and set up monitoring thread with PID information.
  }
}
