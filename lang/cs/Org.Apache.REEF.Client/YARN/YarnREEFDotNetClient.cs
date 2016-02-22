// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Temporary client for developing and testing .NET job submission E2E.
    /// TODO: When REEF-189 is completed YARNREEFClient should be either merged or
    /// deprecated by final client.
    /// </summary>
    [Unstable("For security token support we still need to use YARNREEFClient until (REEF-875)")]
    public sealed class YarnREEFDotNetClient : IREEFClient
    {
        private const string REEFApplicationType = @"REEF";
        private static readonly Logger Log = Logger.GetLogger(typeof(YarnREEFDotNetClient));
        private readonly IYarnRMClient _yarnRMClient;
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly IJobResourceUploader _jobResourceUploader;
        private readonly IYarnJobCommandProvider _yarnJobCommandProvider;
        private readonly REEFFileNames _fileNames;
        private readonly IJobSubmissionDirectoryProvider _jobSubmissionDirectoryProvider;
        private readonly YarnREEFDotNetParamSerializer _paramSerializer;
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;

        [Inject]
        private YarnREEFDotNetClient(
            IYarnRMClient yarnRMClient,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            IJobResourceUploader jobResourceUploader,
            IYarnJobCommandProvider yarnJobCommandProvider,
            REEFFileNames fileNames,
            IJobSubmissionDirectoryProvider jobSubmissionDirectoryProvider,
            YarnREEFDotNetParamSerializer paramSerializer,
            IResourceArchiveFileGenerator resourceArchiveFileGenerator)
        {
            _jobSubmissionDirectoryProvider = jobSubmissionDirectoryProvider;
            _fileNames = fileNames;
            _yarnJobCommandProvider = yarnJobCommandProvider;
            _jobResourceUploader = jobResourceUploader;
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _yarnRMClient = yarnRMClient;
            _paramSerializer = paramSerializer;
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
        }

        public void Submit(JobRequest jobRequest)
        {
            // create local driver folder.
            var localDriverFolderPath = CreateDriverFolder(jobRequest.JobIdentifier);
            try
            {
                var appPackagePath = CreateApplicationPackage(jobRequest.AppParameters);
                Submit(jobRequest.JobParameters, appPackagePath);
            }
            finally
            {
                if (Directory.Exists(localDriverFolderPath))
                {
                    Directory.Delete(localDriverFolderPath, recursive: true);
                }
            }
        }

        public void Submit(JobParameters jobParameters, string appPackagePath)
        {
            if (!File.Exists(appPackagePath))
            {
                throw new ArgumentException("Unable to find zipped application package.");
            }

            var localDriverFolderPath = Path.GetDirectoryName(appPackagePath);
            if (localDriverFolderPath == null)
            {
                throw new ArgumentException("Invalid path for driver folder.");
            }

            var newApplication = _yarnRMClient.CreateNewApplicationAsync().GetAwaiter().GetResult();
            var applicationId = newApplication.ApplicationId;

            var jobSubmissionDirectory = _jobSubmissionDirectoryProvider.GetJobSubmissionRemoteDirectory(applicationId);

            var jobParamsFilePath =
                _paramSerializer.SerializeJobFile(jobParameters, localDriverFolderPath, jobSubmissionDirectory);

            try
            {
                var archiveResource = _jobResourceUploader.UploadArchiveResource(localDriverFolderPath,
                    jobSubmissionDirectory);

                // Path to the job args file.
                var jobArgsFilePath = Path.Combine(localDriverFolderPath, _fileNames.GetJobSubmissionParametersFile());

                var argFileResource = _jobResourceUploader.UploadFileResource(jobArgsFilePath, jobSubmissionDirectory);

                // upload prepared folder to DFS
                var jobResources = new List<JobResource> { archiveResource, argFileResource };

                // submit job
                Log.Log(Level.Verbose, @"Assigned application id {0}", applicationId);

                var submissionReq = CreateApplicationSubmissionRequest(jobParameters, applicationId, jobResources);

                var submittedApplication = _yarnRMClient.SubmitApplicationAsync(submissionReq).GetAwaiter().GetResult();
                Log.Log(Level.Info, @"Submitted application {0}", submittedApplication.Id);
            }
            finally
            {
                if (File.Exists(jobParamsFilePath))
                {
                    File.Delete(jobParamsFilePath);
                }
            }
        }

        public string CreateApplicationPackage(AppParameters appParameters, string pathToAppPackage = null)
        {
            throw new NotImplementedException();
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(JobRequest jobRequest)
        {
            throw new NotSupportedException();
        }

        [Obsolete("Deprecated in 0.14, please use Submit(JobRequest)")]
        public void Submit(IJobSubmission jobSubmission)
        {
            Submit(JobRequest.FromJobSubmission(jobSubmission));
        }

        public async Task<FinalState> GetJobFinalStatus(string appId)
        {
            var application = await _yarnRMClient.GetApplicationAsync(appId).ConfigureAwait(false);
            return application.FinalStatus;
        }

        private SubmitApplication CreateApplicationSubmissionRequest(
           JobParameters jobParameters,
           string appId,
           IReadOnlyCollection<JobResource> jobResources)
        {
            string command = _yarnJobCommandProvider.GetJobSubmissionCommand();
            Log.Log(Level.Verbose, "Command for YARN: {0}", command);
            Log.Log(Level.Verbose, "ApplicationID: {0}", appId);
            Log.Log(Level.Verbose, "MaxApplicationSubmissions: {0}", jobParameters.MaxApplicationSubmissions);
            foreach (var jobResource in jobResources)
            {
                Log.Log(Level.Verbose, "Remote file: {0}", jobResource.RemoteUploadPath);
            }

            var submitApplication = new SubmitApplication
            {
                ApplicationId = appId,
                ApplicationName = jobParameters.JobIdentifier,
                AmResource = new Resouce
                {
                    MemoryMB = jobParameters.DriverMemoryInMB,
                    VCores = 1 // keeping parity with existing code
                },
                MaxAppAttempts = jobParameters.MaxApplicationSubmissions,
                ApplicationType = REEFApplicationType,
                KeepContainersAcrossApplicationAttempts = true,
                Queue = @"default", // keeping parity with existing code
                Priority = 1, // keeping parity with existing code
                UnmanagedAM = false,
                AmContainerSpec = new AmContainerSpec
                {
                    LocalResources = CreateLocalResources(jobResources),
                    Commands = new Commands
                    {
                        Command = command
                    }
                }
            };

            return submitApplication;
        }

        private static LocalResources CreateLocalResources(IEnumerable<JobResource> jobResources)
        {
            return new LocalResources
            {
                Entries = jobResources.Select(jobResource => new RestClient.DataModel.KeyValuePair<string, LocalResourcesValue>
                {
                    Key = jobResource.Name,
                    Value = new LocalResourcesValue
                    {
                        Resource = jobResource.RemoteUploadPath,
                        Type = jobResource.ResourceType,
                        Visibility = Visibility.APPLICATION,
                        Size = jobResource.ResourceSize,
                        Timestamp = jobResource.LastModificationUnixTimestamp
                    }
                }).ToArray()
            };
        }

        /// <summary>
        /// Creates the temporary directory to hold the job submission.
        /// </summary>
        /// <returns>The path to the folder created.</returns>
        private static string CreateDriverFolder(string jobId)
        {
            return Path.GetFullPath(
                Path.Combine(Path.GetTempPath(), string.Join("-", "reef", jobId, Guid.NewGuid().ToString("N").Substring(0, 8))));
        }
    }
}