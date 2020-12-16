namespace LogAnalyticsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kusto.Cloud.Platform.Utils;
    using Microsoft.Azure.OperationalInsights;
    using Microsoft.Rest.Azure.Authentication;

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
        private OperationalInsightsDataClient GetKustoClient()
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
            var client = GetKustoClient();

            // This saves off all the trigger errors found.
            List<BuildLogEntry> triggerEntries = new List<BuildLogEntry>();

            // Construct and execute the query to get all triggers whose last run failed recently.
            string kustoQuery = "ADFPipelineRun ";
            kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
            kustoQuery += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters) ";
            kustoQuery += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters) ";
            kustoQuery += $"| extend MonitoringActive = extractjson(\"$.dataCopPipelineMonitoringActive\", Parameters) ";
            kustoQuery += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
            kustoQuery += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
            kustoQuery += $"| where MonitoringActive == \"True\" ";
            kustoQuery += $"| summarize arg_max(TimeGenerated, *) by TriggerId ";
            kustoQuery += failures ? $"| where Status == \"Failed\" " : $"| where Status == \"Succeeded\" ";
            kustoQuery += $"| project TimeGenerated, ResourceId, PipelineName, TriggerId, RunId, BuildEntityId, WindowStart, WindowEnd, OperationName, FailureType, MaxFailuresInARow, LookbackPeriod ";
            kustoQuery += $"| order by TimeGenerated asc";
            var results = client.Query(query: kustoQuery);
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
            var client = GetKustoClient();

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
                string kustoQuery = "ADFPipelineRun ";
                kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
                kustoQuery += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
                kustoQuery += $"| where {buildEntitiesFilter} ";
                kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
                kustoQuery += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters) ";
                kustoQuery += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters) ";
                kustoQuery += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
                kustoQuery += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
                kustoQuery += $"| summarize Total=countif(Status == \"Failed\" or Status == \"Succeeded\"), Errors=countif(Status == \"Failed\"), arg_max(TimeGenerated, *) by {propertyName} ";
                kustoQuery += $"| extend Pct=todouble(Errors)/todouble(Total) ";
                var results = client.Query(query: kustoQuery);
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
            var client = GetKustoClient();

            // Execute the query to get the last failed activity for this run.
            string kustoQuery = "ADFPipelineRun ";
            kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            kustoQuery += $"| where TriggerId == \"{triggerId}\" ";
            kustoQuery += $"| order by TimeGenerated desc ";
            var results = client.Query(query: kustoQuery);
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
            var client = GetKustoClient();

            // Execute the query to get all the non-build failed activities for this run.
            bool buildFailure = true;
            string kustoQuery = "ADFActivityRun ";
            kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where PipelineRunId == \"{runId}\" ";
            kustoQuery += $"| where Status == \"Failed\" ";
            kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", UserProperties) ";
            kustoQuery += $"| extend BuildFailure = extractjson(\"$.dataCopIsBuildTeamFailure\", UserProperties) ";
            kustoQuery += $"| where BuildFailure != \"True\" ";
            kustoQuery += $"| order by TimeGenerated desc ";
            var results = client.Query(query: kustoQuery);
            var resultsList = results.Results.ToList();
            if (resultsList.Count > 0)
            {
                // We have at least one non-build failure, so this should be routed to the team that owns the data factory.
                buildFailure = false;
            }

            // Execute the query to get the last failed activity for this run.
            BuildLogEntry buildFailureEntry = null;
            kustoQuery = "ADFActivityRun ";
            kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where PipelineRunId == \"{runId}\" ";
            kustoQuery += $"| where Status == \"Failed\" ";
            kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", UserProperties) ";
            kustoQuery += $"| order by TimeGenerated desc ";
            results = client.Query(query: kustoQuery);
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
            var client = GetKustoClient();

            // Execute the query to get all the non-build failed activities for this run.

            // Execute the query to get the last failed activity for this run.
            var kustoQuery = "ADFActivityRun ";
            kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| order by TimeGenerated desc ";
            var results = client.Query(query: kustoQuery);
            var resultsList = results.Results.ToList();


            return resultsList;
        }

        public IList<IDictionary<string, string>> GetADFPipelineRun(DateTime startTime, DateTime endTime)
        {
            // Get the client to do the query.
            var client = GetKustoClient();

            // Execute the query to get all the non-build failed activities for this run.

            // Execute the query to get the last failed activity for this run.
            var kustoQuery = "ADFPipelineRun ";
            kustoQuery += $"| where TimeGenerated > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where TimeGenerated <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| order by TimeGenerated desc ";
            var results = client.Query(query: kustoQuery);
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
        public BuildLogEntry GetLastPipelineRun(string buildEntityId, DateTime startTime, DateTime endTime, out string status)
        {
            // Get the client to do the query.
            var client = GetKustoClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string kustoQuery = "ADFPipelineRun ";
            kustoQuery += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
            if (!string.IsNullOrEmpty(buildEntityId))
            {
                kustoQuery += $"| where BuildEntityId == \"{buildEntityId}\" ";
            }
            kustoQuery += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters, typeof(datetime)) ";
            kustoQuery += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters, typeof(datetime)) ";
            kustoQuery += $"| where WindowStart > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where WindowStart <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| extend MonitoringActive = extractjson(\"$.dataCopPipelineMonitoringActive\", Parameters) ";
            kustoQuery += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
            kustoQuery += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
            kustoQuery += $"| where MonitoringActive == \"True\" ";
            kustoQuery += $"| where Status != \"Succeeded\" ";
            kustoQuery += $"| summarize arg_max(TimeGenerated, *) by TriggerId ";
            kustoQuery += $"| project TimeGenerated, ResourceId, PipelineName, Status, TriggerId, RunId, BuildEntityId, WindowStart, WindowEnd, OperationName, FailureType, MaxFailuresInARow, LookbackPeriod ";
            kustoQuery += $"| order by TimeGenerated asc";

            var results = client.Query(query: kustoQuery);
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
        public BuildLogEntry GetLastActivityRun(string runId, DateTime startTime, DateTime endTime, out string status)
        {
            // Get the client to do the query.
            var client = GetKustoClient();
            string kustoQuery = "ADFActivityRun ";
            // Execute the query to get the last failed activity for this run.
            BuildLogEntry buildFailureEntry = null;
            kustoQuery = "ADFActivityRun ";
            kustoQuery += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters, typeof(datetime)) ";
            kustoQuery += $"| where WindowStart > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where WindowStart <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where PipelineRunId == \"{runId}\" ";
            kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", UserProperties) ";
            kustoQuery += $"| order by TimeGenerated desc ";
            var results = client.Query(query: kustoQuery);
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
            }
            status = resultsList[0]["Status"];
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
        public BuildLogEntry Test(string buildEntityId, DateTime startTime, DateTime endTime, out string status)
        {
            // Get the client to do the query.
            var client = GetKustoClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string kustoQuery = "ADFPipelineRun ";
            kustoQuery += $"| extend WindowStart = extractjson(\"$.windowStart\", Parameters, typeof(datetime)) ";
            kustoQuery += $"| extend BuildEntityId = extractjson(\"$.dataBuildEntityId\", Parameters) ";
            kustoQuery += $"| where WindowStart > datetime(\"{startTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where WindowStart <= datetime(\"{endTime.ToString("yyyy-MM-dd hh:mm:ss.ffff")}\") ";
            kustoQuery += $"| where MonitoringActive == \"True\" ";
            kustoQuery += $"| where BuildEntityId == \"{buildEntityId}\" ";
            kustoQuery += $"| extend TriggerId = extractjson(\"$.TriggerId\", SystemParameters) ";
            kustoQuery += $"| extend WindowEnd = extractjson(\"$.windowEnd\", Parameters) ";
            kustoQuery += $"| extend MonitoringActive = extractjson(\"$.dataCopPipelineMonitoringActive\", Parameters) ";
            kustoQuery += $"| extend MaxFailuresInARow = extractjson(\"$.dataCopPipelineMonitoringAlertAfterFailedAttempts\", Parameters) ";
            kustoQuery += $"| extend LookbackPeriod = extractjson(\"$.dataCopPipelineMonitoringLookBackPeriod\", Parameters) ";
            kustoQuery += $"| summarize arg_max(TimeGenerated, *) by TriggerId ";
            kustoQuery += $"| project TimeGenerated, ResourceId, PipelineName, Status, TriggerId, RunId, BuildEntityId, WindowStart, WindowEnd, OperationName, FailureType, MaxFailuresInARow, LookbackPeriod ";
            kustoQuery += $"| order by TimeGenerated desc";
            var results = client.Query(query: kustoQuery);
            var resultsList = results.Results.ToList();
            if (resultsList == null || resultsList.Count == 0)
            {
                status = string.Empty;
                return null;
            }
            BuildLogEntry logEntry = new BuildLogEntry()
            {
                TimeGenerated = DateTime.Parse(resultsList[0]["TimeGenerated"]).ToUniversalTime(),
                BuildEntityId = buildEntityId,
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
    }
}
