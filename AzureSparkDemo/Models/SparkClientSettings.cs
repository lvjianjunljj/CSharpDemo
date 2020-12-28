namespace AzureSparkDemo.Models
{
    using Microsoft.Azure.Databricks.Client;
    using System.Collections.Generic;

    /// <summary>
    /// Class SparkClientSettings.
    /// </summary>
    public class SparkClientSettings
    {
        /// <summary>
        /// Gets or sets the libraries.
        /// </summary>
        /// <value>The libraries.</value>
        public List<Library> Libraries { get; set; }
        /// <summary>
        /// Gets or sets the type of the node.
        /// </summary>
        /// <value>The type of the node.</value>
        public string NodeType { get; set; }

        /// <summary>
        /// Gets or sets the client identifier.
        /// </summary>
        /// <value>The client identifier.</value>
        public string ClientId { get; set; }

        /// <summary>
        /// Gets or sets the client key.
        /// </summary>
        /// <value>The client key.</value>
        public string ClientKey { get; set; }

        /// <summary>
        /// Gets or sets the node types.
        /// </summary>
        /// <value>The node types.</value>
        public List<NodeTypeExpression> NodeTypes { get; set; }

        /// <summary>
        /// Gets or sets the number workers.
        /// </summary>
        /// <value>The number workers.</value>
        public int NumWorkersMin { get; set; }

        public int NumWorkersMax { get; set; }

        public int MaxDegreeOfParallelism { get; set; }
        public int TimeoutSeconds { get; set; }
    }
}
