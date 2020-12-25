namespace KenshoDemo.QueryModels
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Newtonsoft.Json;

    /// <summary>
    /// An anomaly found by a Kensho detec config.
    /// </summary>
    public class KenshoDetectAnomaly
    {
        /// <summary>
        /// This gets or sets the timestamp of when the anomaly occurred.
        /// </summary>
        [JsonProperty(PropertyName = "timestamp")]
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// This gets or sets the list of dimensions and values that identifies the series the anomaly was found on.
        /// </summary>
        [JsonProperty(PropertyName = "dimensions")]
        public Dictionary<string, string> Dimensions { get; set; }

        /// <summary>
        /// This gets or sets the value of the anomaly.
        /// </summary>
        [JsonProperty(PropertyName = "value")]
        public double AnomalyValue { get; set; }

        /// <summary>
        /// This gets or sets the properties of the anomaly.
        /// </summary>
        [JsonProperty(PropertyName = "properties")]
        public KenshoDetectAnomalyProperties Properties { get; set; }

        /// <summary>
        /// This computes the histogram for this anomalyset which shows what percentage of anomalies have each dimension value.
        /// </summary>
        /// <param name="anomalySet">The anomaly set to process.</param>
        /// <param name="minPercent">The minimum percentage to include in the histogram</param>
        /// <returns>A dictionary mapping dimensions/values with percentages.</returns>
        public static Dictionary<string, double> GetHistogram(List<KenshoDetectAnomaly> anomalySet, double minPercent)
        {
            var histogram = new Dictionary<string, double>();

            Dictionary<string, double> dimvalCounts = new Dictionary<string, double>();
            foreach (KenshoDetectAnomaly anomaly in anomalySet)
            {
                foreach (string dim in anomaly.Dimensions.Keys)
                {
                    string key = $"{dim}-{anomaly.Dimensions[dim]}";
                    if (dimvalCounts.ContainsKey(key))
                    {
                        dimvalCounts[key]++;
                    }
                    else
                    {
                        dimvalCounts.Add(key, 1);
                    }
                }
            }

            // Sort by descending count.
            List<KeyValuePair<string, double>> sortedByValue = dimvalCounts.ToList();
            sortedByValue.Sort((pair1, pair2) => pair2.Value.CompareTo(pair1.Value));
            foreach (KeyValuePair<string, double> dimval in sortedByValue)
            {
                double pct = dimval.Value / (double)anomalySet.Count;
                if (pct >= minPercent)
                {
                    histogram[dimval.Key] = Math.Round(dimval.Value / (double)anomalySet.Count, 3);
                }
            }

            return histogram;
        }
    }
}

