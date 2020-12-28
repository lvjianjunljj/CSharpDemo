
namespace AzureSparkDemo.Models
{
    using CommonLib;
    using CommonLib.IDEAs;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Class SparkMetricsTestResult.
    /// </summary>
    public class MetricsTestResult : PropertyComparable
    {
        // The name of the test
        /// <summary>
        /// Gets or sets the name of the test.
        /// </summary>
        /// <value>The name of the test.</value>
        [JsonProperty("testName")]
        public string TestName { get; set; }

        // The type of comparison to indicate which date should be used as comparison
        /// <summary>
        /// Gets or sets the type of the comparison.
        /// </summary>
        /// <value>The type of the comparison.</value>
        [JsonProperty("comparisonType")]
        [JsonConverter(typeof(StringEnumConverter))]
        public ComparisonType ComparisonType { get; set; }

        // The date of the data that the test runs on
        /// <summary>
        /// Gets or sets the date.
        /// </summary>
        /// <value>The date.</value>
        [JsonProperty("date")]
        public DateTime Date { get; set; }

        // The previous  date of the data that the test runs on
        /// <summary>
        /// Gets or sets the previous date.
        /// </summary>
        /// <value>The previous date.</value>
        [JsonProperty("previousDate")]
        public DateTime PreviousDate { get; set; }

        // The metric value for the given date
        /// <summary>
        /// The metric value
        /// </summary>
        [JsonProperty("metricValue")]
        public double MetricValue;

        // The metric value that is the baseline of the comparison
        // This could be metric value of previous date or target metric value depending on ComparisonType
        /// <summary>
        /// The baseline metric value
        /// </summary>
        [JsonProperty("baselineMetricValue")]
        public double BaselineMetricValue;

        // The percentage difference of the metrics measured
        /// <summary>
        /// Gets or sets the percent difference.
        /// </summary>
        /// <value>The percent difference.</value>
        [JsonProperty("percentDiff")]
        public double PercentDiff { get; set; }

        /// <summary>
        /// Gets or sets the upper bound threshold for the percentage difference
        /// </summary>
        /// <value>The upper bound threshold.</value>
        [JsonProperty("upperBoundThreshold")]
        public double UpperBoundThreshold { get; set; }

        /// <summary>
        /// Gets or sets the lower bound threshold for the percentage difference
        /// </summary>
        /// <value>The lower bound threshold.</value>
        [JsonProperty("lowerBoundThreshold")]
        public double LowerBoundThreshold { get; set; }

        /// <summary>
        /// Gets or sets the test run identifier.
        /// </summary>
        /// <value>The test run identifier.</value>
        [JsonProperty("testRunId")]
        public string TestRunId { get; set; }

        [JsonProperty("metricName")]
        public string MetricName { get; set; }

        /// <summary>
        /// Constructs the failure message.
        /// </summary>
        /// <param name="result">The result.</param>
        /// <returns>System.String.</returns>
        public static string ConstructFailureMessage(MetricsTestResult result)
        {
            var failureMessage = $"Date {result.Date:d}, Metric {result.MetricName}";

            if (result.PercentDiff < result.LowerBoundThreshold)
            {
                failureMessage += $"Lower bound threshold: {result.LowerBoundThreshold}%";
            }
            else if (result.PercentDiff > result.UpperBoundThreshold)
            {
                failureMessage += $"Upper bound threshold: {result.UpperBoundThreshold}%";
            }

            failureMessage += $", Breached threshold: {result.PercentDiff:0.##}%.";

            return failureMessage;
        }

        public static bool Evaluate(MetricsTestResult result, out string message)
        {
            var withinThresholds = result.LowerBoundThreshold <= result.PercentDiff && result.PercentDiff <= result.UpperBoundThreshold;
            message = withinThresholds ? $"Test for {result.MetricName} succeeded as {result.PercentDiff:0.##}% is within {result.LowerBoundThreshold:0.##}% and {result.UpperBoundThreshold:0.##}%." :
                MetricsTestResult.ConstructFailureMessage(result);
            return withinThresholds;
        }

        public static void Evaluate(
            IEnumerable<MetricsTestResult> results,
            out string accumulatedMessage,
            out bool accumulatedWithinThresholds)
        {
            accumulatedMessage = string.Empty;
            accumulatedWithinThresholds = true;
            foreach (var result in results)
            {
                // If thresholds are not set properly, default to failure
                string message;
                bool withinThresholds = MetricsTestResult.Evaluate(result, out message);
                accumulatedMessage += message;
                accumulatedWithinThresholds &= withinThresholds;
            }
        }
    }
}
