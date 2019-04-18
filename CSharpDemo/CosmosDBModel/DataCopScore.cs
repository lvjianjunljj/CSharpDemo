using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace CSharpDemo.CosmosDBModel
{
    public class DataCopScore
    {
        /// <summary>
        /// Gets the DataCopScore Id.
        /// The Id should be a combination of DataCopScore Name and ScoreTime to make sure for each workload/metric at one ScoreTime, just has one score
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id")]
        public string Id
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the DataCopScore name.
        /// The name should be workload or metric name
        /// </summary>
        /// <value>The name.</value>
        [JsonProperty("name")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets Id of the dataset against which the score is computed.
        /// </summary>
        /// <value>The dataset identifier.</value>
        [JsonProperty("datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// Gets or sets category of the dataset. Persisting this to optimize for
        /// query performance and to avoid join while returning back
        /// in REST APIs.
        /// </summary>
        /// <value>The dataset category.</value>
        [JsonProperty("datasetCategory")]
        public string DatasetCategory { get; set; }

        /// <summary>
        /// Gets or sets the scoreTime - the time when the DQ score is computed.
        /// </summary>
        /// <value>The score time.</value>
        [JsonProperty(PropertyName = "scoreTime")]
        public DateTime ScoreTime { get; set; }

        /// <summary>
        /// Gets or sets the score.
        /// </summary>
        /// <value>The score.</value>
        [JsonProperty(PropertyName = "score")]
        public double? Score { get; set; }

        /// <summary>
        /// Gets or sets the availability score.
        /// </summary>
        /// <value>The availability score.</value>
        [JsonProperty(PropertyName = "availabilityScore")]
        public double? AvailabilityScore { get; set; }

        /// <summary>
        /// Gets or sets the latency score.
        /// </summary>
        /// <value>The latency score.</value>
        [JsonProperty(PropertyName = "latencyScore")]
        public double? LatencyScore { get; set; }

        /// <summary>
        /// Gets or sets the completeness score.
        /// </summary>
        /// <value>The completeness score.</value>
        [JsonProperty(PropertyName = "completenessScore")]
        public double? CompletenessScore { get; set; }

        /// <summary>
        /// Gets or sets the correctness score.
        /// </summary>
        /// <value>The correctness score.</value>
        [JsonProperty(PropertyName = "correctnessScore")]
        public double? CorrectnessScore { get; set; }

        /// <summary>
        /// Gets or sets the test run ids.
        /// </summary>
        /// <value>The test run ids.</value>
        [JsonProperty(PropertyName = "testRunIds")]
        public List<string> TestRunIds { get; set; } = new List<string>();
    }
}
