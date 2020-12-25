namespace KenshoDemo.QueryModels
{
    using System;
    using Newtonsoft.Json;

    public class KenshoTestContent
    {
        /// <summary>
        /// Gets or sets the metric Guid to be used for this test.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("metricGuid")]
        public Guid MetricGuid { get; set; }

        /// <summary>
        /// Gets or sets the configId to be used for this test.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("configId")]
        public Guid ConfigId { get; set; }

        /// <summary>
        /// Gets or sets the minimum anomaly severity to look for when querying Kensho.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("minSeverity")]
        public int? MinSeverity { get; set; }

        /// <summary>
        /// Gets or sets the maximum anomaly severity to look for when querying Kensho.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("maxSeverity")]
        public int? MaxSeverity { get; set; }

        /// <summary>
        /// Gets or sets the test date for this run.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("testDate")]
        public DateTime TestDate { get; set; }
    }
}