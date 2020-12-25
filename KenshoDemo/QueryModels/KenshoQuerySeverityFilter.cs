namespace KenshoDemo.QueryModels
{
    using Newtonsoft.Json;

    /// <summary>
    /// This is a filter to be used for queries.
    /// </summary>
    public class KenshoQuerySeverityFilter
    {
        /// <summary>
        /// Gets or sets the minimum anomaly severity to look for when querying Kensho.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("min")]
        public string MinSeverity { get; set; }

        /// <summary>
        /// Gets or sets the maximum anomaly severity to look for when querying Kensho.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("max")]
        public string MaxSeverity { get; set; }

        /// <summary>
        /// This gets or sets the detector direction, which can be Negative, Positive, Both
        /// </summary>
        [JsonProperty(PropertyName = "direction")]
        public string AnomalyDetectorDirection { get; set; }
    }
}
