namespace LogAnalyticsDemo
{
    using Newtonsoft.Json;
    using System;

    public class BuildLogEntry
    {
        /// <summary>
        /// This gets or sets the title to be use for the incident.
        /// </summary>
        [JsonProperty(PropertyName = "incidentTitle")]
        public string IncidentTitle { get; set; }

        /// <summary>
        /// This gets or sets the title to use for the summary.
        /// </summary>
        [JsonProperty(PropertyName = "summaryTitle")]
        public string SummaryTitle { get; set; } = "Build Failure Details";

        /// <summary>
        /// This gets or sets the failure reason describing why this alert fired.
        /// </summary>
        [JsonProperty(PropertyName = "failureReason")]
        public string FailureReason { get; set; }

        /// <summary>
        /// This gets or sets the name of the data factory.
        /// </summary>
        [JsonProperty(PropertyName = "dataFactory")]
        public string DataFactory { get; set; }

        /// <summary>
        /// This gets or sets the name of the pipeline.
        /// </summary>
        [JsonProperty(PropertyName = "pipelineName")]
        public string PipelineName { get; set; }

        /// <summary>
        /// This gets or sets the guid of the run instance.
        /// </summary>
        [JsonProperty(PropertyName = "pipelineRunId")]
        public string PipelineRunId { get; set; }

        /// <summary>
        /// This gets or sets the start of the window slice.
        /// </summary>
        [JsonProperty(PropertyName = "windowStart")]
        public DateTime? WindowStart { get; set; }

        /// <summary>
        /// This gets or sets the end of the window slice.
        /// </summary>
        [JsonProperty(PropertyName = "windowEnd")]
        public DateTime? WindowEnd { get; set; }

        /// <summary>
        /// This gets or sets the link to the pipeline entry.
        /// </summary>
        [JsonProperty(PropertyName = "linkToADFPipeline")]
        public string LinkToADFPipeline { get; set; }

        /// <summary>
        /// This gets or sets the link to app insights logs.
        /// </summary>
        [JsonProperty(PropertyName = "appInsightsLink")]
        public string AppInsightsLink { get; set; }

        /// <summary>
        /// This gets or sets the link to help documentation on build error debugging.
        /// </summary>
        [JsonProperty(PropertyName = "helpLink")]
        public string HelpLink { get; set; }

        /// <summary>
        /// This gets or sets the timestamp when the log entry was written.
        /// </summary>
        [JsonProperty(PropertyName = "timeGenerated")]
        public DateTime TimeGenerated { get; set; }

        /// <summary>
        /// This gets or sets the resource identifier.
        /// </summary>
        [JsonProperty(PropertyName = "resourceId")]
        public string ResourceId { get; set; }

        /// <summary>
        /// This gets or sets the entity the log entry is for.
        /// </summary>
        [JsonProperty(PropertyName = "buildEntityId")]
        public string BuildEntityId { get; set; }

        /// <summary>
        /// This gets or sets the name of the activity.
        /// </summary>
        [JsonProperty(PropertyName = "activityname")]
        public string ActivityName { get; set; }

        /// <summary>
        /// This gets or sets the operation name from the log.
        /// </summary>
        [JsonProperty(PropertyName = "operationName")]
        public string OperationName { get; set; }

        /// <summary>
        /// This gets or sets the trigger identifier.
        /// </summary>
        [JsonProperty(PropertyName = "triggerId")]
        public string TriggerId { get; set; }

        /// <summary>
        /// This gets or sets the guid of the activity run instance.
        /// </summary>
        [JsonProperty(PropertyName = "activityRunId")]
        public string ActivityRunId { get; set; }

        /// <summary>
        /// This gets or sets the failure type from the log.
        /// </summary>
        [JsonProperty(PropertyName = "failureType")]
        public string FailureType { get; set; }

        /// <summary>
        /// This gets or sets a flag indicating whether this is the build team's responsibility or not.
        /// </summary>
        [JsonProperty(PropertyName = "buildFailure")]
        public bool BuildFailure { get; set; }

        /// <summary>
        /// This gets or sets the total number of runs that occurred in the given time window.
        /// </summary>
        [JsonProperty(PropertyName = "totalRuns")]
        public int? TotalRuns { get; set; }

        /// <summary>
        /// This gets or sets the number of runs that failed in the given time window.
        /// </summary>
        [JsonProperty(PropertyName = "failedRuns")]
        public int? FailedRuns { get; set; }

        /// <summary>
        /// This gets or sets the percentage of failures in the given time window.
        /// </summary>
        [JsonProperty(PropertyName = "failureRate")]
        public double? FailureRate { get; set; }

        /// <summary>
        /// This gets or sets total number of retries for the last run.
        /// </summary>
        [JsonProperty(PropertyName = "numberOfRetries")]
        public int? NumberOfRetries { get; set; }

        /// <summary>
        /// This gets or sets the maximum number of failures allowed in a row.
        /// </summary>
        [JsonProperty(PropertyName = "maxFailuresInARow")]
        public int MaxFailuresInARow { get; set; }

        /// <summary>
        /// This gets or sets the lookback period to search for errors.
        /// </summary>
        [JsonProperty(PropertyName = "lookbackPeriod")]
        public TimeSpan LookbackPeriod { get; set; }
    }
}
