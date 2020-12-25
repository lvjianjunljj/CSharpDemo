namespace KenshoDemo.QueryModels
{
    using System;
    using Newtonsoft.Json;

    /// <summary>
    /// This is the query_detect_result_anomalies request to look for new anomaly alerts.
    /// the alert.
    /// </summary>
    public class KenshoQueryDetectResultAnomaliesRequest
    {
        /// <summary>
        /// This gets or sets the Kensho-assigned Id of metric to search for anomalies.
        /// </summary>
        [JsonProperty(PropertyName = "metricGuid")]
        public Guid MetricGuid { get; set; }

        /// <summary>
        /// This gets or sets the Kensho-assigned Id of the config for the metric to search for.
        /// </summary>
        [JsonProperty(PropertyName = "detectConfigGuid")]
        public Guid DetectConfigGuid { get; set; }

        /// <summary>
        /// This gets or sets the earliest time to search for.
        /// </summary>
        [JsonProperty(PropertyName = "startTime")]
        public DateTime StartTime { get; set; }

        /// <summary>
        /// This gets or sets the latest time to search for.
        /// </summary>
        [JsonProperty(PropertyName = "endTime")]
        public DateTime EndTime { get; set; }

        /// <summary>
        /// This gets or sets the number of the page to retrieve in the case of a multi-page result.
        /// </summary>
        [JsonProperty(PropertyName = "pageNum")]
        public int PageNum { get; set; }

        /// <summary>
        /// This gets or sets the maximum number of results to include in a single page
        /// </summary>
        [JsonProperty(PropertyName = "pageSize")]
        public int PageSize { get; set; }

        /// <summary>
        /// This controls the filter for the query.
        /// </summary>
        [JsonProperty(PropertyName = "filter")]
        public KenshoQueryFilter Filter { get; set; }
    }
}

