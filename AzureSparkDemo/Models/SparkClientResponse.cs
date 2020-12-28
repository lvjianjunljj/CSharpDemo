namespace AzureSparkDemo.Models
{
    using System;
    using Microsoft.Azure.Databricks.Client;
    using Newtonsoft.Json;

    /// <summary>
    /// Class SparkClientResponse.
    /// </summary>
    public class SparkClientResponse
    {
        /// <summary>
        /// Gets or sets the run.
        /// </summary>
        /// <value>The run.</value>
        public Run Run { get; set; }

        /// <summary>
        /// Gets or sets the cost.
        /// </summary>
        /// <value>The cost.</value>
        public double Cost { get; set; }

        /// <summary>
        /// Gets or sets the total hours.
        /// </summary>
        /// <value>The total hours.</value>
        public double TotalHours { get; set; }

        /// <summary>
        /// Gets or sets the type of the node.
        /// </summary>
        /// <value>The type of the node.</value>
        public string NodeType { get; set; }

        /// <summary>
        /// Gets or sets the cost per node.
        /// </summary>
        /// <value>The cost per node.</value>
        public double CostPerNode { get; set; }

        /// <summary>
        /// Gets or sets the number workers.
        /// </summary>
        /// <value>The number workers.</value>
        public int NumWorkersMin { get; set; }

        public int NumWorkersMax { get; set; }

        /// <summary>
        /// Gets or sets the test run identifier.
        /// </summary>
        /// <value>The test run identifier.</value>
        public string TestRunId { get; set; }

        public string RunOutput { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        public bool IsRunSuccess()
        {
            return this.Run?.State?.ResultState == RunResultState.SUCCESS;
        }
    }
}
