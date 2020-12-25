namespace KenshoDemo.QueryModels
{
    using Newtonsoft.Json;

    /// <summary>
    /// This is a filter to be used for queries.
    /// </summary>
    public class KenshoQueryFilter
    {
        /// <summary>
        /// This gets or sets the series filter.
        /// </summary>
        [JsonProperty(PropertyName = "seriesFilter")]
        public KenshoSeries SeriesFilter { get; set; }

        /// <summary>
        /// This gets or sets the group filter.
        /// </summary>
        [JsonProperty(PropertyName = "groupFilter")]
        public KenshoSeries GroupFilter { get; set; }

        /// <summary>
        /// This gets or sets the severity filter.
        /// </summary>
        [JsonProperty(PropertyName = "severityFilter")]
        public KenshoQuerySeverityFilter SeverityFilter { get; set; }
    }
}
