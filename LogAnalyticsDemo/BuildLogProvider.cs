namespace LogAnalyticsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kusto.Cloud.Platform.Utils;
    using Microsoft.Azure.OperationalInsights;
    using Microsoft.Rest.Azure.Authentication;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class BuildLogProvider
    {
        /// <summary>
        /// The domain.  Should probably make this driven by a keyvault secret, but this isn't too likely to change again and this'
        /// change is needed right away.
        /// </summary>
        private const string Domain = "prdtrs01.prod.outlook.com";
        /// <summary>
        /// The authentication endpoint
        /// </summary>
        private const string AuthEndpoint = "https://login.microsoftonline.com";
        /// <summary>
        /// The token audience
        /// </summary>
        private const string TokenAudience = "https://api.loganalytics.io/";

        /// <summary>
        /// The workspace identifier
        /// </summary>
        private readonly string workspaceId = null;
        /// <summary>
        /// The client identifier
        /// </summary>
        private readonly string clientId = null;
        /// <summary>
        /// The client secret
        /// </summary>
        private readonly string clientSecret = null;

        /// <summary>
        /// Initializes a new instance of the <see cref=BuildLogProvider" /> class.
        /// </summary>
        /// <param name="workspaceId">The workspace identifier.</param>
        /// <param name="clientId">The client identifier.</param>
        /// <param name="clientSecret">The client secret.</param>
        public BuildLogProvider(
            string workspaceId,
            string clientId,
            string clientSecret)
        {
            this.workspaceId = workspaceId;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }

        /// <summary>
        /// Authenticate to the Kusto Log Analytics instance for build.
        /// </summary>
        /// <returns></returns>
        private OperationalInsightsDataClient GetClient()
        {
            // Authenticate to log analytics.
            var adSettings = new ActiveDirectoryServiceSettings
            {
                AuthenticationEndpoint = new Uri(AuthEndpoint),
                TokenAudience = new Uri(TokenAudience),
                ValidateAuthority = true
            };
            var creds = ApplicationTokenProvider.LoginSilentAsync(Domain, clientId, clientSecret, adSettings).GetAwaiter().GetResult();
            var client = new OperationalInsightsDataClient(creds)
            {
                WorkspaceId = workspaceId
            };

            return client;
        }

        /// <summary>
        /// Gets all the triggers whose last run failed since the last time the logs were scanned.
        /// </summary>
        /// <param name="startTime">The first time to scan for.</param>
        /// <param name="endTime">The last time to scan for.</param>
        /// <param name="failures"> Get failures if true, successes if false.</param>
        /// <returns>IList&lt;TriggerFailureEntry&gt;.</returns>
        public IList<BuildLogEntry> GetRecentTriggerEntries(DateTime startTime, DateTime endTime, bool failures)
        {
            // Get the client to do the query.
            var client = GetClient();

            // This saves off all the trigger errors found.
            List<BuildLogEntry> triggerEntries = new List<BuildLogEntry>();

            // Construct and execute the query to get all triggers whose last run failed recently.
            string queryString = "ADFPipelineRun ";
            queryString += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
            queryString += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters) ";
            queryString += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters) ";
            queryString += $"| extend MonitoringActive = extractjson(\"$.dataCopPipelineMonitoringActive\", Parameters) ";
            queryString += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
            queryString += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
            queryString += $"| where MonitoringActive == \"True\" ";
            queryString += $"| summarize arg_max(TimeGenerated, *) by TriggerId ";
            queryString += failures ? $"| where Status == \"Failed\" " : $"| where Status == \"Succeeded\" ";
            queryString += $"| project TimeGenerated, ResourceId, PipelineName, TriggerId, RunId, BuildEntityId, WindowStart, WindowEnd, OperationName, FailureType, MaxFailuresInARow, LookbackPeriod ";
            queryString += $"| order by TimeGenerated asc";
            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();
            foreach (IDictionary<string, string> result in resultsList)
            {
                BuildLogEntry logEntry = new BuildLogEntry()
                {
                    TimeGenerated = DateTime.Parse(result["TimeGenerated"]).ToUniversalTime(),
                    BuildEntityId = result["BuildEntityId"].ValueOrDefaultIfNullOrWhitespace(""),
                    OperationName = result["OperationName"].ValueOrDefaultIfNullOrWhitespace(""),
                    FailureType = result["FailureType"].ValueOrDefaultIfNullOrWhitespace(""),
                    WindowStart = DateTime.Parse(result["WindowStart"]).ToUniversalTime(),
                    WindowEnd = DateTime.Parse(result["WindowEnd"]).ToUniversalTime(),
                    MaxFailuresInARow = int.Parse(result["MaxFailuresInARow"].ValueOrDefaultIfNullOrWhitespace("0")),
                    LookbackPeriod = TimeSpan.Parse(result["LookbackPeriod"].ValueOrDefaultIfNullOrWhitespace("0")),
                    PipelineName = result["PipelineName"].ValueOrDefaultIfNullOrWhitespace(""),
                    PipelineRunId = result["RunId"].ValueOrDefaultIfNullOrWhitespace(""),
                    ResourceId = result["ResourceId"].ValueOrDefaultIfNullOrWhitespace(""),
                    TriggerId = result["TriggerId"].ValueOrDefaultIfNullOrWhitespace(""),
                };
                logEntry.LinkToADFPipeline = $"<a href='https://ms-adf.azure.com/monitoring/pipelineruns/{logEntry.PipelineRunId}?factory={logEntry.ResourceId}'>Run Details for {logEntry.PipelineName}</a>";
                logEntry.HelpLink = $"<a href='https://aka.ms/databuild-debugpipelinefailure'>IDEAs DataBuild Pipeline Failure Debugging Guide</a>";
                logEntry.DataFactory = logEntry.ResourceId.Substring(logEntry.ResourceId.LastIndexOf('/') + 1);
                if (logEntry.BuildEntityId != null)
                {
                    // Some log entries don't have a build entity id in Parameters.  We'll skip those for now but should look into
                    // it at some point.
                    triggerEntries.Add(logEntry);
                }
            }

            return triggerEntries;
        }

        /// <summary>
        /// For every data factory or pipeline that had a recent failure, determine its failure rate over the past window of time.
        /// </summary>
        /// <param name="triggerFailures">The list of trigger failures.</param>
        /// <param name="computePipelines">True=compute pipeline failure rates; False=compute data factory failure rates</param>
        /// <param name="startTime">The first time to scan for.</param>
        /// <param name="endTime">The last time to scan for.</param>
        /// <returns>IList&lt;BuildFailureRateEntry&gt;.</returns>
        public IList<BuildLogEntry> GetFailureRates(IList<BuildLogEntry> triggerFailures, bool computePipelines, DateTime startTime, DateTime endTime)
        {
            // Get the client to do the query.
            var client = GetClient();

            // Generate a list of unique entities.
            HashSet<string> buildEntities = new HashSet<string>();
            string buildEntitiesFilter = null;
            string propertyName = computePipelines ? "PipelineName" : "ResourceId";
            foreach (BuildLogEntry entry in triggerFailures)
            {
                string entity = computePipelines ? entry.PipelineName : entry.ResourceId;
                if (!buildEntities.Contains(entity))
                {
                    buildEntities.Add(entity);
                    if (buildEntitiesFilter == null)
                    {
                        buildEntitiesFilter = $"{propertyName} == \"{entity}\"";
                    }
                    else
                    {
                        buildEntitiesFilter += $" or {propertyName} == \"{entity}\"";
                    }
                }
            }

            // This saves off all the failure rates computed.
            List<BuildLogEntry> failureRateEntries = new List<BuildLogEntry>();

            // Construct and execute the query to compute the failure rates for each entity.
            if (buildEntities.Count > 0)
            {
                string queryString = "ADFPipelineRun ";
                queryString += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
                queryString += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
                queryString += $"| where {buildEntitiesFilter} ";
                queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
                queryString += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters) ";
                queryString += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters) ";
                queryString += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
                queryString += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
                queryString += $"| summarize Total=countif(Status == \"Failed\" or Status == \"Succeeded\"), Errors=countif(Status == \"Failed\"), arg_max(TimeGenerated, *) by {propertyName} ";
                queryString += $"| extend Pct=todouble(Errors)/todouble(Total) ";
                var results = client.Query(query: queryString);
                var resultsList = results.Results.ToList();
                foreach (IDictionary<string, string> result in resultsList)
                {
                    BuildLogEntry failedRateEntry = new BuildLogEntry()
                    {
                        TotalRuns = int.Parse(result["Total"].ValueOrDefaultIfNullOrWhitespace("0")),
                        FailedRuns = int.Parse(result["Errors"].ValueOrDefaultIfNullOrWhitespace("0")),
                        FailureRate = double.Parse(result["Pct"].ValueOrDefaultIfNullOrWhitespace("0.0")),
                    };
                    if (propertyName == "ResourceId")
                    {
                        failedRateEntry.ResourceId = result[propertyName].ValueOrDefaultIfNullOrWhitespace("");
                        failedRateEntry.DataFactory = failedRateEntry.ResourceId.Substring(failedRateEntry.ResourceId.LastIndexOf('/') + 1);
                        failedRateEntry.BuildEntityId = "";
                        failedRateEntry.MaxFailuresInARow = int.Parse(result["MaxFailuresInARow"].ValueOrDefaultIfNullOrWhitespace("0"));
                        failedRateEntry.LookbackPeriod = TimeSpan.Parse(result["LookbackPeriod"].ValueOrDefaultIfNullOrWhitespace("0"));
                    }
                    else
                    {
                        failedRateEntry.PipelineName = result[propertyName].ValueOrDefaultIfNullOrWhitespace("");
                        failedRateEntry.ResourceId = result["ResourceId"].ValueOrDefaultIfNullOrWhitespace("");
                        failedRateEntry.DataFactory = failedRateEntry.ResourceId.Substring(failedRateEntry.ResourceId.LastIndexOf('/') + 1);
                        failedRateEntry.BuildEntityId = result["BuildEntityId"].ValueOrDefaultIfNullOrWhitespace("");
                        failedRateEntry.WindowStart = DateTime.Parse(result["WindowStart"]).ToUniversalTime();
                        failedRateEntry.WindowEnd = DateTime.Parse(result["WindowEnd"]).ToUniversalTime();
                        failedRateEntry.MaxFailuresInARow = int.Parse(result["MaxFailuresInARow"].ValueOrDefaultIfNullOrWhitespace("0"));
                        failedRateEntry.LookbackPeriod = TimeSpan.Parse(result["LookbackPeriod"].ValueOrDefaultIfNullOrWhitespace("0"));
                    }
                    failureRateEntries.Add(failedRateEntry);
                }
            }

            return failureRateEntries;
        }

        /// <summary>
        /// Determine how many failures in a row have occurred on the most recent runs of the given trigger.
        /// </summary>
        /// <param name="triggerId">The trigger to check.</param>
        /// <param name="startTime">The earliest time to scan for.</param>
        /// <returns>The number of failures in a row.</returns>
        public int GetLastFailuresInARow(string triggerId, DateTime startTime)
        {
            int failuresInARow = 0;

            // Get the client to do the query.
            var client = GetClient();

            // Execute the query to get the last failed activity for this run.
            string queryString = "ADFPipelineRun ";
            queryString += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            queryString += $"| where TriggerId == \"{triggerId}\" ";
            queryString += $"| order by TimeGenerated desc ";
            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();
            foreach (IDictionary<string, string> result in resultsList)
            {
                string status = result["Status"].ValueOrDefaultIfNullOrWhitespace("");
                if (status == "Failed")
                {
                    failuresInARow++;
                }
                else if (status == "Succeeded")
                {
                    // We hit a success - so we are done counting.
                    break;
                }

                // Other values (InProgress or Queued) can be ignored for the purposes of this analysis.
            }

            return failuresInARow;
        }

        /// <summary>
        /// For a pipeline that had a recent failure, determine if the most recent failed run failed due to a core build activity or
        /// not.
        /// </summary>
        /// <param name="runId">The run to examine.</param>
        /// <param name="startTime">The first time to scan for.</param>
        /// <param name="endTime">The last time to scan for.</param>
        /// <returns>The failure entry for the log.</returns>
        public BuildLogEntry GetLastFailedActivityForRun(string runId, DateTime startTime, DateTime endTime)
        {
            // Get the client to do the query.
            var client = GetClient();

            // Execute the query to get all the non-build failed activities for this run.
            bool buildFailure = true;
            string queryString = "ADFActivityRun ";
            queryString += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| where PipelineRunId == \"{runId}\" ";
            queryString += $"| where Status == \"Failed\" ";
            queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", UserProperties) ";
            queryString += $"| extend BuildFailure = extractjson(\"$.dataCopIsBuildTeamFailure\", UserProperties) ";
            queryString += $"| where BuildFailure != \"True\" ";
            queryString += $"| order by TimeGenerated desc ";
            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();
            if (resultsList.Count > 0)
            {
                // We have at least one non-build failure, so this should be routed to the team that owns the data factory.
                buildFailure = false;
            }

            // Execute the query to get the last failed activity for this run.
            BuildLogEntry buildFailureEntry = null;
            queryString = "ADFActivityRun ";
            queryString += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| where PipelineRunId == \"{runId}\" ";
            queryString += $"| where Status == \"Failed\" ";
            queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", UserProperties) ";
            queryString += $"| order by TimeGenerated desc ";
            results = client.Query(query: queryString);
            resultsList = results.Results.ToList();

            if (resultsList.Count > 0)
            {
                // We only need to return a few properties for this.
                buildFailureEntry = new BuildLogEntry()
                {
                    ActivityName = resultsList[0]["ActivityName"],
                    ActivityRunId = resultsList[0]["ActivityRunId"],
                    BuildEntityId = resultsList[0]["BuildEntityId"],
                    BuildFailure = buildFailure,
                };
            }

            return buildFailureEntry;
        }

        public IList<IDictionary<string, string>> GetADFActivityRun(DateTime startTime, DateTime endTime)
        {
            // Get the client to do the query.
            var client = GetClient();

            // Execute the query to get all the non-build failed activities for this run.

            // Execute the query to get the last failed activity for this run.
            var queryString = "ADFActivityRun ";
            queryString += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            queryString += $"| order by TimeGenerated desc ";
            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();


            return resultsList;
        }

        public IList<IDictionary<string, string>> GetADFPipelineRun(DateTime startTime, DateTime endTime)
        {
            // Get the client to do the query.
            var client = GetClient();

            // Execute the query to get all the non-build failed activities for this run.

            // Execute the query to get the last failed activity for this run.
            var queryString = "ADFPipelineRun ";
            queryString += $"| where TimeGenerated > datetime(\"{startTime}\") ";
            queryString += $"| where TimeGenerated <= datetime(\"{endTime}\") ";
            queryString += $"| order by TimeGenerated desc ";
            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();


            return resultsList;
        }

        /// <summary>
        /// Get the last pipeline run and its status based on buildEntityId.
        /// </summary>
        /// <param name="buildEntityId">The buildEntityId.</param>
        /// <param name="startTime">The first time to scan for WindowStart.</param>
        /// <param name="endTime">The last time to scan for WindowStart.</param>
        /// <param name="status">The status output.</param>
        /// <returns>The last entry for the log.</returns>
        public BuildLogEntry GetLastPipelineRun(string buildEntityId, DateTime testDate, out string status)
        {
            // Get the client to do the query.
            var client = GetClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string queryString = "ADFPipelineRun ";
            queryString += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
            if (!string.IsNullOrEmpty(buildEntityId))
            {
                queryString += $"| where BuildEntityId == \"{buildEntityId}\" ";
            }
            queryString += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters, typeof(datetime)) ";
            queryString += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters, typeof(datetime)) ";
            queryString += $"| where WindowStart == datetime(\"{testDate}\") ";
            queryString += $"| extend MonitoringActive = extractjson(\"$.dataCopPipelineMonitoringActive\", Parameters) ";
            queryString += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
            queryString += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
            queryString += $"| where MonitoringActive == \"True\" ";
            //queryString += $"| where Status != \"Succeeded\" ";
            queryString += $"| summarize arg_max(TimeGenerated, *) by TriggerId ";
            queryString += $"| project TimeGenerated, ResourceId, PipelineName, Status, TriggerId, RunId, BuildEntityId, WindowStart, WindowEnd, OperationName, FailureType, MaxFailuresInARow, LookbackPeriod ";
            queryString += $"| order by TimeGenerated desc";

            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();
            if (resultsList == null || resultsList.Count == 0)
            {
                status = string.Empty;
                return null;
            }
            BuildLogEntry logEntry = new BuildLogEntry()
            {
                TimeGenerated = DateTime.Parse(resultsList[0]["TimeGenerated"]).ToUniversalTime(),
                BuildEntityId = resultsList[0]["BuildEntityId"].ValueOrDefaultIfNullOrWhitespace(""),
                OperationName = resultsList[0]["OperationName"].ValueOrDefaultIfNullOrWhitespace(""),
                FailureType = resultsList[0]["FailureType"].ValueOrDefaultIfNullOrWhitespace(""),
                WindowStart = DateTime.Parse(resultsList[0]["WindowStart"]).ToUniversalTime(),
                WindowEnd = DateTime.Parse(resultsList[0]["WindowEnd"]).ToUniversalTime(),
                MaxFailuresInARow = int.Parse(resultsList[0]["MaxFailuresInARow"].ValueOrDefaultIfNullOrWhitespace("0")),
                LookbackPeriod = TimeSpan.Parse(resultsList[0]["LookbackPeriod"].ValueOrDefaultIfNullOrWhitespace("0")),
                PipelineName = resultsList[0]["PipelineName"].ValueOrDefaultIfNullOrWhitespace(""),
                PipelineRunId = resultsList[0]["RunId"].ValueOrDefaultIfNullOrWhitespace(""),
                ResourceId = resultsList[0]["ResourceId"].ValueOrDefaultIfNullOrWhitespace(""),
                TriggerId = resultsList[0]["TriggerId"].ValueOrDefaultIfNullOrWhitespace(""),
            };
            logEntry.LinkToADFPipeline = $"<a href='https://ms-adf.azure.com/monitoring/pipelineruns/{logEntry.PipelineRunId}?factory={logEntry.ResourceId}'>Run Details for {logEntry.PipelineName}</a>";
            logEntry.HelpLink = $"<a href='https://aka.ms/databuild-debugpipelinefailure'>IDEAs DataBuild Pipeline Failure Debugging Guide</a>";
            logEntry.DataFactory = logEntry.ResourceId.Substring(logEntry.ResourceId.LastIndexOf('/') + 1);
            status = resultsList[0]["Status"].ValueOrDefaultIfNullOrWhitespace("");
            return logEntry;
        }

        /// <summary>
        /// Get the last activity run and its status based on runId.
        /// </summary>
        /// <param name="runId">The run to examine.</param>
        /// <param name="startTime">The first time to scan for.</param>
        /// <param name="endTime">The last time to scan for.</param>
        /// <param name="status">The status output.</param>
        /// <returns>The last entry for the log.</returns>
        public BuildLogEntry GetLastActivityRun(string runId, string activityName, DateTime? testDate, out string status)
        {
            status = string.Empty;
            // Get the client to do the query.
            var client = GetClient();
            string queryString = "ADFActivityRun ";
            // Execute the query to get the last failed activity for this run.
            BuildLogEntry buildFailureEntry = null;
            queryString += $"| extend WindowStart = extractjson(\"$.extendedProperties.windowStart\", Input, typeof(datetime)) ";
            if (testDate.HasValue)
            {
                queryString += $"| where WindowStart == datetime(\"{testDate}\") ";
            }

            queryString += $"| where PipelineRunId == \"{runId}\" ";
            if (!string.IsNullOrEmpty(activityName))
            {
                queryString += $"| where ActivityName == \"{activityName}\" ";
            }
            queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", UserProperties) ";
            queryString += $"| order by TimeGenerated desc ";
            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();
            if (resultsList.Count > 0)
            {
                // We only need to return a few properties for this.
                buildFailureEntry = new BuildLogEntry()
                {
                    ActivityName = resultsList[0]["ActivityName"],
                    ActivityRunId = resultsList[0]["ActivityRunId"],
                    BuildEntityId = resultsList[0]["BuildEntityId"],
                    //BuildFailure = buildFailure,
                };
                status = resultsList[0]["Status"];
            }
            return buildFailureEntry;
        }

        /// <summary>
        /// Get the last pipeline run and its status based on buildEntityId.
        /// </summary>
        /// <param name="buildEntityId">The buildEntityId.</param>
        /// <param name="startTime">The first time to scan for WindowStart.</param>
        /// <param name="endTime">The last time to scan for WindowStart.</param>
        /// <param name="status">The status output.</param>
        /// <returns>The last entry for the log.</returns>
        public BuildLogEntry GetJobSubmissionResult(string buildEntityId, string activityName, DateTime testDate, out string status)
        {
            // Get the client to do the query.  
            var client = GetClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string queryString = "let lastPipeline = ADFPipelineRun ";
            queryString += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
            queryString += $"| where BuildEntityId == \"{buildEntityId}\" ";
            //queryString += $"| where Status != \"Succeeded\" ";
            queryString += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters, typeof(datetime)) ";
            queryString += $"| extend MonitoringActive = extractjson(\"$.dataCopPipelineMonitoringActive\", Parameters) ";
            queryString += $"| extend PipelineRunId = RunId";
            queryString += $"| where WindowStart == datetime(\"{testDate}\") ";
            queryString += $"| where MonitoringActive == \"True\" ";
            queryString += $"| top 1 by TimeGenerated desc ";
            queryString += $"| project PipelineRunId, Status; ";

            queryString += $"let succeeded = lastPipeline ";
            queryString += $"| extend ActivityRunId = \"DummyActivityRunId\"";
            queryString += $"| extend ActivityName = \"DummyActivityName\"";
            queryString += $"| where Status == \"Succeeded\" ";
            queryString += $"| project PipelineRunId, ActivityRunId, ActivityName, Status; ";

            queryString += $"let not_succeeded = lastPipeline ";
            queryString += $"| where Status != \"Succeeded\" ";
            queryString += $"| project PipelineRunId; ";

            queryString += $"let lastADFActivity = ADFActivityRun ";
            queryString += $"| where PipelineRunId in (not_succeeded) ";
            if (!string.IsNullOrEmpty(activityName))
                queryString += $"| where ActivityName == \"{activityName}\" ";
            queryString += $"| top 1 by TimeGenerated desc ";
            queryString += $"| project PipelineRunId, ActivityRunId, ActivityName, Status; ";

            queryString += $"let not_queued = lastADFActivity ";
            queryString += $"| where Status != \"Queued\" ";
            queryString += $"| project PipelineRunId, ActivityRunId, ActivityName, Status; ";

            queryString += $"succeeded ";
            queryString += $"| union not_queued ";

            var results = client.Query(query: queryString);
            var resultsList = results.Results.ToList();
            if (resultsList == null || resultsList.Count == 0)
            {
                status = string.Empty;
                return null;
            }

            BuildLogEntry logEntry = new BuildLogEntry()
            {
                //PipelineName = resultsList[0]["PipelineName"].ValueOrDefaultIfNullOrWhitespace(""),
                PipelineRunId = resultsList[0]["PipelineRunId"].ValueOrDefaultIfNullOrWhitespace(""),
                //ResourceId = resultsList[0]["ResourceId"].ValueOrDefaultIfNullOrWhitespace(""),
                ActivityRunId = resultsList[0]["ActivityRunId"].ValueOrDefaultIfNullOrWhitespace(""),
                ActivityName = resultsList[0]["ActivityName"].ValueOrDefaultIfNullOrWhitespace(""),
            };

            status = resultsList[0]["Status"].ValueOrDefaultIfNullOrWhitespace("");
            return logEntry;
        }

        // New version of query generatation for UDP job submission monitor
        public BuildLogEntry GetJobSubmissionQueryResult(string activityName, DateTime testDate, out string status)
        {
            // Get the client to do the query.  
            var client = GetClient();

            // Construct and execute the query to get all triggers whose last run failed recently.

            string queryString = this.GenerateJobSubmissionQuery(activityName, testDate);

            var results = client.Query(query: queryString.Replace("@@TestDate@@", testDate.ToString()));
            var resultsList = results.Results.ToList();
            if (resultsList == null || resultsList.Count == 0)
            {
                status = string.Empty;
                return null;
            }

            BuildLogEntry logEntry = new BuildLogEntry()
            {
                ActivityRunId = resultsList[0]["ActivityRunId"].ValueOrDefaultIfNullOrWhitespace(""),
                ActivityName = resultsList[0]["ActivityName"].ValueOrDefaultIfNullOrWhitespace(""),
            };

            status = resultsList[0]["Status"].ValueOrDefaultIfNullOrWhitespace("");
            return logEntry;
        }

        private string GenerateJobSubmissionQuery(string activityName, DateTime testDate)
        {
            string queryString = $"ADFActivityRun ";
            queryString += $"| extend WindowStart = extractjson(\"$.extendedProperties.windowStart\", Input, typeof(datetime)) ";
            queryString += $"| where WindowStart == datetime(\"@@TestDate@@\") ";
            if (!string.IsNullOrEmpty(activityName))
                queryString += $"| where ActivityName == \"{activityName}\" ";
            queryString += $"| where Status != \"Failed\" ";
            queryString += $"| project ActivityRunId, ActivityName, Status ";

            return queryString;
        }

        /// <summary>
        /// Get the last pipeline run and its status based on buildEntityId.
        /// </summary>
        /// <param name="buildEntityId">The buildEntityId.</param>
        /// <param name="startTime">The first time to scan for WindowStart.</param>
        /// <param name="endTime">The last time to scan for WindowStart.</param>
        /// <param name="status">The status output.</param>
        /// <returns>The last entry for the log.</returns>
        public List<IDictionary<string, string>> GetPipelineRuns(string pipelineRunId)
        {
            // Get the client to do the query.
            var client = GetClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string queryString = "ADFPipelineRun ";
            queryString += $"| extend PipelineRunId = RunId";
            queryString += $"| where RunId == \"{pipelineRunId}\" ";
            queryString += $"| order by TimeGenerated desc";

            var results = client.Query(query: queryString);
            return results.Results.ToList();
        }
    }
}
