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
package org.apache.reef.runtime.common.client;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.REEF;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.REEFVersion;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Default REEF implementation.
 */
@ClientSide
@Provided
@Private
public final class REEFImplementation implements REEF {

  private static final Logger LOG = Logger.getLogger(REEFImplementation.class.getName());

  private final JobSubmissionHandler jobSubmissionHandler;
  private final PackageJobSubmissionHandler packageJobSubmissionHandler;
  private final RunningJobs runningJobs;
  private final JobSubmissionHelper jobSubmissionHelper;
  private final ClientWireUp clientWireUp;
  private final LoggingScopeFactory loggingScopeFactory;
  private final Set<ConfigurationProvider> configurationProviders;
  private final REEFFileNames reefFileNames;
  private final JobJarMaker jobJarMaker;

  /**
   * @param jobSubmissionHandler
   * @param runningJobs
   * @param jobSubmissionHelper
   * @param clientWireUp
   * @param reefVersion             provides the current version of REEF.
   * @param configurationProviders
   */
  @Inject
  REEFImplementation(final JobSubmissionHandler jobSubmissionHandler,
                     final PackageJobSubmissionHandler packageJobSubmissionHandler,
                     final RunningJobs runningJobs,
                     final JobSubmissionHelper jobSubmissionHelper,
                     final ClientWireUp clientWireUp,
                     final LoggingScopeFactory loggingScopeFactory,
                     final REEFVersion reefVersion,
                     @Parameter(DriverConfigurationProviders.class)
                     final Set<ConfigurationProvider> configurationProviders,
                     final REEFFileNames reefFileNames,
                     final JobJarMaker jobJarMaker) {
    this.jobSubmissionHandler = jobSubmissionHandler;
    this.packageJobSubmissionHandler = packageJobSubmissionHandler;
    this.runningJobs = runningJobs;
    this.jobSubmissionHelper = jobSubmissionHelper;
    this.clientWireUp = clientWireUp;
    this.configurationProviders = configurationProviders;
    clientWireUp.performWireUp();
    this.loggingScopeFactory = loggingScopeFactory;
    this.reefFileNames = reefFileNames;
    this.jobJarMaker = jobJarMaker;
    reefVersion.logVersion();
  }

  @Override
  public void close() {
    this.runningJobs.closeAllJobs();
    this.clientWireUp.close();
  }

  @Override
  public void submit(final Configuration driverConf) {
    try (LoggingScope ls = this.loggingScopeFactory.reefSubmit()) {
      final Configuration driverConfiguration = createDriverConfiguration(driverConf);
      final JobSubmissionEvent submissionMessage = createJobSubmissionEvent(driverConfiguration);
      this.jobSubmissionHandler.onNext(submissionMessage);
    }
  }

  @Override
  public void submit(final Configuration driverConf, final Configuration runtimeConf) {
    submit(Configurations.merge(driverConf, runtimeConf));
  }

  @Override
  public void submit(final Configuration runtimeConf, final File applicationPackage) {
    packageJobSubmissionHandler.onNext(createJobSubmissionEvent(runtimeConf));
  }

  @Override
  public Path createApplicationPackage(final Configuration driverConf) throws IOException {
    return createApplicationPackage(driverConf, File.createTempFile(
        reefFileNames.getJobFolderPrefix(), reefFileNames.getJarFileSuffix()).toPath());
  }

  @Override
  public Path createApplicationPackage(final Configuration driverConf, final Path packagePath) throws IOException {
    final Configuration driverConfiguration = createDriverConfiguration(driverConf);
    final File packageFile = packagePath.toFile();
    if (packageFile.exists()) {
      throw new IOException("Designated package file at " + packagePath + " already exists.");
    }

    this.jobJarMaker.createJobSubmissionJAR(createJobSubmissionEvent(driverConfiguration), driverConfiguration);
    return packageFile.toPath();
  }

  private JobSubmissionEvent createJobSubmissionEvent(final Configuration driverConfiguration) {
    try {
      if (this.clientWireUp.isClientPresent()) {
        return this.jobSubmissionHelper.getJobSubmissionBuilder(driverConfiguration)
            .setRemoteId(this.clientWireUp.getRemoteManagerIdentifier())
            .build();
      } else {
        return this.jobSubmissionHelper.getJobSubmissionBuilder(driverConfiguration)
            .setRemoteId(ErrorHandlerRID.NONE)
            .build();
      }
    } catch (final Exception e) {
      throw new RuntimeException("Exception while processing driver configuration.", e);
    }
  }

  /**
   * Assembles the final Driver Configuration by merging in all the Configurations provided by ConfigurationProviders.
   *
   * @param driverConfiguration
   * @return
   */
  private Configuration createDriverConfiguration(final Configuration driverConfiguration) {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang()
        .newConfigurationBuilder(driverConfiguration);
    for (final ConfigurationProvider configurationProvider : this.configurationProviders) {
      configurationBuilder.addConfiguration(configurationProvider.getConfiguration());
    }
    return configurationBuilder.build();
  }

  /**
   * The driver remote identifier.
   */
  @NamedParameter(doc = "The driver remote identifier.")
  public static final class DriverRemoteIdentifier implements Name<String> {
  }


}
