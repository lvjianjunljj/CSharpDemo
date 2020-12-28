namespace AzureSparkDemo.Models
{
    using Newtonsoft.Json;

    /// <summary>
    /// Class MetricThreshold.
    /// </summary>
    public class MetricThreshold
    {

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        [JsonProperty("name")]
        public string Name { get; set; }


        /// <summary>
        /// Gets or sets the upper bound.
        /// </summary>
        /// <value>The upper bound.</value>
        [JsonProperty("upperBound")]

        public double UpperBound { get; set; }


        /// <summary>
        /// Gets or sets the lower bound.
        /// </summary>
        /// <value>The lower bound.</value>
        [JsonProperty("lowerBound")]
        public double LowerBound { get; set; }
    }
}
