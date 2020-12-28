namespace AzureSparkDemo.Models
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Class MetricValues.
    /// </summary>
    public class MetricValues
    {
        /// <summary>
        /// Gets or sets the baseline.
        /// </summary>
        /// <value>The baseline.</value>
        public IDictionary<string, double> Baseline { get; set; }
        /// <summary>
        /// Gets or sets the current.
        /// </summary>
        /// <value>The current.</value>
        public IDictionary<string, double> Current { get; set; }

        /// <summary>
        /// Computes the percent difference.
        /// </summary>
        /// <param name="metricValues">The metric values.</param>
        /// <param name="metricName">Name of the metric.</param>
        /// <returns>System.Nullable&lt;System.Double&gt;.</returns>
        public static double ComputePercentDiff(MetricValues metricValues, string metricName)
        {
            // Math.Abs(denominator) is important to maintain the correctness of the +/- sign of the percentage difference
            return (metricValues.Current[metricName] - metricValues.Baseline[metricName]) / Math.Abs(metricValues.Baseline[metricName]) * 100;
        }
    }
}
