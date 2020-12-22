namespace KenshoDemo.Models
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json;

    /// <summary>
    /// This is the update_datafeed request to change the properties on an existing feed in Kensho.
    /// </summary>
    public class KenshoUpdateDatafeedRequest
    {
        /// <summary>
        /// This gets or sets the Kensho assigned GUID for this datafeed.
        /// </summary>
        [JsonProperty(PropertyName = "datafeedId")]
        public Guid DatafeedId { get; set; }

        /// <summary>
        /// This gets or sets the unique name of this Kensho datafeed.
        /// </summary>
        [JsonProperty(PropertyName = "datafeedName")]
        public string DatafeedName { get; set; }

        /// <summary>
        /// This gets or sets the first time for Kensho to pull data for this datafeed.
        /// </summary>
        [JsonProperty(PropertyName = "dataStartFrom")]
        public DateTime DataStartFrom { get; set; }


        /// <summary>
        /// This gets or sets the amount of time to wait for a new datafeed to arrive before alerting about missing data.
        /// </summary>
        [JsonProperty(PropertyName = "gracePeriodInSeconds")]
        public int GracePeriodInSeconds { get; set; }

        /// <summary>
        /// This gets or sets the names of the columns in the datafeed schema that represent metric values to be monitored.
        /// </summary>
        [JsonProperty(PropertyName = "metrics")]
        public List<string> Metrics { get; set; }

        /// <summary>
        /// This gets or sets the display names to be used for the columns in the datafeed.  The format of this field is a mapping of column names to display names.
        /// If a value is not supplied for a column, then the column name itself will be used as the display name.  
        /// </summary>
        [JsonProperty(PropertyName = "displayColumns")]
        public Dictionary<string, string> DisplayColumns { get; set; }

        /// <summary>
        /// This gets or sets the time to delay before starting to pull data after the update.  
        /// </summary>
        [JsonProperty(PropertyName = "scheduleIngestionDelayInSeconds")]
        public int ScheduleIngestionDelayInSeconds { get; set; }

        /// <summary>
        /// This gets or sets the parameters required by the specific data source specified, in the form of parameter names mapped to parameter values.  The
        /// parameters are (note that only Cosmos is currently supported):
        ///   Cosmos, KVP: DataSource
        ///   SQLServer, Cube, Kusto, PostgreSql: ConnectionStr, Query
        ///   iScope: VC, Script
        ///   xCard: StepId, StudyId
        ///   AzureBlob: ConnectionStr, ContainerName, BlobTemplate
        ///   AzureTable: ConnectionStr, TableName, Query
        ///   MDM: Account, Namespace, MetricsName, SamplingType, DimensionInclusive, DimensionExclusive, AggregationType
        /// </summary>
        [JsonProperty(PropertyName = "parameterList")]
        public Dictionary<string, string> ParameterList { get; set; }
    }
}

