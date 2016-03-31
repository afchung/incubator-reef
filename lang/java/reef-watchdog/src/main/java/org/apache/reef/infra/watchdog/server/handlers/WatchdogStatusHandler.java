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

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.infra.watchdog.avro.Status;
import org.apache.reef.infra.watchdog.avro.StatusResponse;
import org.apache.reef.infra.watchdog.server.Job;
import org.apache.reef.infra.watchdog.server.JobContainer;
import org.apache.reef.infra.watchdog.utils.Constants;
import org.mortbay.jetty.HttpMethods;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by anchung on 4/4/2016.
 */
public final class WatchdogStatusHandler extends AbstractHandler {
  private final JobContainer jobContainer;

  @Inject
  private WatchdogStatusHandler(final JobContainer jobContainer) {
    this.jobContainer = jobContainer;
  }

  @Override
  public void handle(final String requestTarget,
                     final HttpServletRequest httpServletRequest,
                     final HttpServletResponse httpServletResponse,
                     final int dispatchMode) throws IOException, ServletException {
    if (!httpServletRequest.getMethod().equals(HttpMethods.GET)) {
      // TODO[JIRA]: set response.
      return;
    }

    final Job job = jobContainer.getJob(Constants.TEMP_ID);

    httpServletResponse.setContentType("application/json");

    final StatusResponse.Builder statusResponseBuilder = StatusResponse.newBuilder();

    if (job == null) {
      statusResponseBuilder.setStatus(Status.NotFound);
      return;
    }

    statusResponseBuilder.setStatus(job.getStatus().toAvroStatus());

    final StatusResponse statusResponse = statusResponseBuilder.build();
    try (final PrintWriter contentWriter = httpServletResponse.getWriter()) {
      // TODO[JIRA]: Set forwarded message.
      final DatumWriter<StatusResponse> jsonWriter = new SpecificDatumWriter<>(StatusResponse.class);
      try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(statusResponse.getSchema(), out);
        jsonWriter.write(statusResponse, encoder);
        encoder.flush();
        contentWriter.write(out.toString(Constants.JSON_CHARSET));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }

      contentWriter.close();
    }
  }
}
