namespace AzureSparkDemo.Models
{
    using System.Collections.Generic;

    /// <summary>
    /// Class SparkWorkerMessageTag.
    /// </summary>
    public class SparkWorkerMessageTag
    {
        /// <summary>
        /// Gets or sets the mount point to parquet file.
        /// </summary>
        /// <value>The mount point to parquet file.</value>
        public Dictionary<string, string> MountPointToParquetFile { get; set; }
    }
}
