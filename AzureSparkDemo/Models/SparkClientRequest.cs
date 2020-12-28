namespace AzureSparkDemo.Models
{
    using Microsoft.Azure.Databricks.Client;
    using System.Collections.Generic;

    /// <summary>
    /// Class SparkClientRequest.
    /// </summary>
    public class SparkClientRequest
    {
        /// <summary>
        /// Gets or sets the notebook path.
        /// </summary>
        /// <value>The notebook path.</value>
        public string NotebookPath { get; set; }
        /// <summary>
        /// Gets or sets the notebook parameters.
        /// </summary>
        /// <value>The notebook parameters.</value>
        public Dictionary<string, string> NotebookParameters { get; set; }
        /// <summary>
        /// Gets or sets the libraries.
        /// </summary>
        /// <value>The libraries.</value>
        public List<Library> Libraries { get; set; }

        /// <summary>
        /// Gets or sets the maximum workers.
        /// </summary>
        /// <value>The maximum workers.</value>
        public int NumWorkersMin { get; set; }

        public int NumWorkersMax { get; set; }

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
        /// Gets or sets the test run identifier.
        /// </summary>
        /// <value>The test run identifier.</value>
        public string TestRunId { get; set; }
        public int TimeoutSeconds { get; set; }
    }
}
