namespace AzureSparkDemo.Models
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CommonLib.IDEAs;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;

    public class SparkMetricsTestContent
    {
        /// <summary>
        /// Gets or sets the notebook parameters.
        /// This needs to be a Dictionary{string,string} for compatibility with the DataBricks API.
        /// </summary>
        /// <value>The notebook parameters.</value>
        [JsonProperty("notebookParameters")]
        public Dictionary<string, string> NotebookParameters { get; set; }

        /// <summary>
        /// Gets or sets the type of the comparison.
        /// </summary>
        /// <value>The type of the comparison.</value>
        [JsonProperty("comparisonType")]
        [JsonConverter(typeof(StringEnumConverter))]
        public ComparisonType ComparisonType { get; set; }

        /// <summary>
        /// Gets or sets the thresholds.
        /// </summary>
        /// <value>The thresholds.</value>
        [JsonProperty("thresholds")]
        public List<MetricThreshold> Thresholds { get; set; }

        /// <summary>
        /// Gets or sets the data lake store.
        /// </summary>
        /// <value>The data lake store.</value>
        [JsonProperty("dataLakeStore")]
        public string DataLakeStore { get; set; }

        /// <summary>
        /// Gets or sets the stream path.
        /// </summary>
        /// <value>The stream path.</value>
        [JsonProperty("streamPath")]
        public string StreamPath { get; set; }

        /// <summary>
        /// Gets or sets the date.
        /// </summary>
        /// <value>The date.</value>
        [JsonProperty("date")]
        public DateTime Date { get; set; }

        /// <summary>
        /// Gets or sets the frequency.
        /// </summary>
        /// <value>The frequency.</value>
        [JsonProperty("frequency")]
        [JsonConverter(typeof(StringEnumConverter))]
        public Grain Frequency { get; set; }

        /// <summary>
        /// Gets or sets the notebook path.
        /// </summary>
        /// <value>The notebook path.</value>
        [JsonProperty("notebookPath")]
        public string NotebookPath { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether if true, performs parquet conversion.
        /// </summary>
        /// <value>The notebook parameters.</value>
        [JsonProperty("convertToParquet")]
        public bool ConvertToParquet { get; set; }

        /// <summary>
        /// Gets or sets the target stream path.
        /// </summary>
        /// <value>The target stream path.</value>
        [JsonProperty("targetStreamPath")]
        public string TargetStreamPath { get; set; }

        /// <summary>
        /// Gets or sets the target data lake store.
        /// </summary>
        /// <value>The target data lake store</value>
        [JsonProperty("targetDataLakeStore")]
        public string TargetDataLakeStore { get; set; }

        /// <summary>
        /// Gets the current stream path.
        /// </summary>
        /// <returns>System.String.</returns>
        public string GetCurrentStreamPath()
        {
            return StreamSetUtils.GenerateStreamsetPaths(StreamPath, Date, Date, null, null, Frequency).First();
        }

        public string GetCurrentTargetStreamPath()
        {
            return StreamSetUtils.GenerateStreamsetPaths(this.TargetStreamPath, Date, Date, null, null, Frequency).First();
        }


        /// <summary>
        /// Gets the previous stream path.
        /// </summary>
        /// <returns>System.String.</returns>
        public string GetPreviousStreamPath()
        {
            var date = GetPreviousDate();
            return StreamSetUtils.GenerateStreamsetPaths(StreamPath, date, date, null, null, Frequency).First();
        }

        /// <summary>
        /// Gets the previous date.
        /// </summary>
        /// <returns>DateTime.</returns>
        public DateTime GetPreviousDate()
        {
            return DateUtils.GetPreviousDate(this.Date, this.ComparisonType);
        }

        public string GetDatalakeStore()
        {
            return this.TargetDataLakeStore ?? this.DataLakeStore;
        }
    }
}
