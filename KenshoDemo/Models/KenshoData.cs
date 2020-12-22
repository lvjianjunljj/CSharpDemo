namespace KenshoDemo.Models
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// This is a high level class that contains all the Kensho-releated data in a Dataset.  Note that this includes the actual contents of the create_datafeed
    /// API request as well as the full create_datafeed response plus data from other related functions (like create_detection_config).  It may be considered
    /// a little odd to store these as is in the database, but it is matter of convenience (it allows the contructs to be easily serializable and deserializable
    /// directly from the database.
    /// </summary>
    public class KenshoData : PropertyComparable
    {
        /// <summary>
        /// The dataset Id the Kensho. Is not used outside of onboarding / recording keeping processes.
        /// </summary>
        [JsonProperty(PropertyName = "datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// This gets or sets a flag that determines whether a change that requires the Kensho datafeed to be deleted and recreated will be allowed or rejected.
        /// </summary>
        [JsonProperty(PropertyName = "allowRecreate")]
        public bool? AllowRecreate { get; set; }

        /// <summary>
        /// This gets or sets the id of the AlertSetting entry to use for testing.  That entry determines the severity of IcM alerts, the team
        /// to receive the alert, etc.
        /// </summary>
        [JsonProperty(PropertyName = "alertSettingId")]
        public string AlertSettingId { get; set; }

        /// <summary>
        /// This gets or sets the unique name of this Kensho datafeed.
        /// </summary>
        [JsonProperty(PropertyName = "datafeedName")]
        public string DatafeedName { get; set; }

        /// <summary>
        /// This gets or sets the first time for Kensho to pull data for this datafeed.
        /// </summary>
        [JsonProperty(PropertyName = "dataStartFrom")]
        public DateTime? DataStartFrom { get; set; }

        /// <summary>
        /// This gets or sets the type of data source for Kensho to pull.  See KenshoDataSourceTypeId for details.
        /// </summary>
        [JsonProperty(PropertyName = "dataSourceTypeId")]
        [JsonConverter(typeof(StringEnumConverter))]
        public KenshoDataSourceTypeId? DataSourceTypeId { get; set; }

        /// <summary>
        /// This gets or sets the name of the column in the datafeed schema that represents the timestamp.
        /// </summary>
        [JsonProperty(PropertyName = "timestampColumn")]
        public string TimestampColumn { get; set; }

        /// <summary>
        /// This gets or sets the amount of time to wait for a new datafeed to arrive before alerting about missing data.
        /// </summary>
        [JsonProperty(PropertyName = "gracePeriodInSeconds")]
        public int? GracePeriodInSeconds { get; set; }

        /// <summary>
        /// This gets or sets the amount of time to wait before starting daata ingestion.
        /// </summary>
        [JsonProperty(PropertyName = "scheduleIngestionDelayInSeconds")]
        public int? ScheduleIngestionDelayInSeconds { get; set; }

        /// <summary>
        /// This gets or sets the roll up analysis method (a role up is basically an aggregate for a dimension that includes all values):
        ///    0 = no roll up is necessary
        ///    1 = roll up according to the roleUpMethod
        ///    2 = roll up is already supplied by the value specified in dimensionValueRollup
        /// </summary>
        [JsonProperty(PropertyName = "rollUpAnalysis")]
        public int? RollUpAnalysis { get; set; }

        /// <summary>
        /// This gets or sets the roll up method (see rollUpAnalysis description):
        ///    0 = no roll up is necessary
        ///    1 = role up should be a sum across all values
        ///    2 = roll up should be the max of all values
        ///    3 = roll up should be the min of all values
        /// </summary>
        [JsonProperty(PropertyName = "rollUpMethod")]
        public int? RollUpMethod { get; set; }

        /// <summary>
        /// This gets or sets the value that is used for roll ups for each dimension (see rollUpAnalysis description).
        /// </summary>
        [JsonProperty(PropertyName = "dimensionValueForRollup")]
        public string DimensionValueForRollup { get; set; }

        /// <summary>
        /// This gets or sets the property that tells Kensho how to fill in missing values for a dimension:
        ///    0 = fill with previous data
        ///    1 = fill with fillMissingPointForAdValue
        /// </summary>
        [JsonProperty(PropertyName = "fillMissingPointForAd")]
        public int? FillMissingPointForAd { get; set; }

        /// <summary>
        /// This is the value to use to fill in missing values for a dimension if FillMissingPointForAdd is set to 1.
        /// </summary>
        [JsonProperty(PropertyName = "fillMissingPointForAdValue")]
        public string FillMissingPointForAdValue { get; set; }

        /// <summary>
        /// This gets or sets the granularity of the data.  See KenshoGranTypeId for details.
        /// </summary>
        [JsonProperty(PropertyName = "granTypeId")]
        public KenshoGranTypeId? GranTypeId { get; set; }

        /// <summary>
        /// This gets or sets the mapping of country codes when configuring holiday presets.  This maps countrCodeDimension values to
        /// standard country code values.
        /// </summary>
        [JsonProperty(PropertyName = "holidayCountryCodeMapping")]
        public Dictionary<string, string> HolidayCountryCodeMapping { get; set; }

        /// <summary>
        /// This gets or sets the name of the property that contains country distinctions for holiday presets.
        /// </summary>
        [JsonProperty(PropertyName = "holidayCountryCodeDimension")]
        public string HolidayCountryCodeDimension { get; set; }

        /// <summary>
        /// This gets or sets the number of days before a holiday to suppress alerts for.
        /// </summary>
        [JsonProperty(PropertyName = "holidayDaysToExpandBeforeHoliday")]
        public int? HolidayDaysToExpandBeforeHoliday { get; set; }

        /// <summary>
        /// This gets or sets the number of days after a holiday to suppress alerts for.
        /// </summary>
        [JsonProperty(PropertyName = "holidayDaysToExpandAfterHoliday")]
        public int? HolidayDaysToExpandAfterHoliday { get; set; }

        /// <summary>
        /// This gets or sets the strategy to use:
        ///   1=treat holiday as weekend
        ///   2=suppress holiday
        /// </summary>
        [JsonProperty(PropertyName = "holidayStrategy")]
        public int? HolidayStrategy { get; set; }

        /// <summary>
        /// This gets or sets the strategy to use:
        ///   0=only non-PTO holidays
        ///   1=only PTO holidays
        ///   2=all holidays
        /// </summary>
        [JsonProperty(PropertyName = "holidayOption")]
        public int? HolidayOption { get; set; }

        /// <summary>
        /// This gets or sets whether holiday processing should be enabled or not.
        /// </summary>
        [JsonProperty(PropertyName = "holidayEventEnabled")]
        public bool? HolidayEventEnabled { get; set; }

        /// <summary>
        /// This gets or sets information about each Kensho-configured metric for the dataset.
        /// </summary>
        [JsonProperty(PropertyName = "metrics")]
        public Dictionary<string, KenshoMetric> Metrics { get; set; }

        /// <summary>
        /// This gets or sets the names of the columns in the datafeed schema that represent dimension values to determine the time series.
        /// </summary>
        [JsonProperty(PropertyName = "dimensions")]
        public List<string> Dimensions { get; set; }

        /// <summary>
        /// This gets or sets the display names to be used for the columns in the datafeed.  The format of this field is a mapping of column names to display names.
        /// If a value is not supplied for a column, then the column name itself will be used as the display name.  
        /// </summary>
        [JsonProperty(PropertyName = "displayColumns")]
        public Dictionary<string, string> DisplayColumns { get; set; }

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

        /// <summary>
        /// This gets or sets the list of DL's who's members should have admin access to this feed.
        /// </summary>
        [JsonProperty(PropertyName = "adminGroups")]
        public List<string> AdminGroups { get; set; }

        /// <summary>
        /// This gets or sets the list of DL's who's members should have read-only access to this feed.
        /// </summary>
        [JsonProperty(PropertyName = "viewerGroups")]
        public List<string> ViewerGroups { get; set; }

        /// <summary>
        /// This gets or sets the unique ID of this feed assigned by Kensho.
        /// </summary>
        [JsonProperty(PropertyName = "datafeedId")]
        public Guid? DatafeedId { get; set; }
    }

    public enum KenshoDataSourceTypeId
    {
        SQLServer = 2,
        Cosmos = 25,
        iScope = 26,
    }

    public enum KenshoGranTypeId
    {
        Yearly = 1,
        Monthly = 2,
        Weekly = 3,
        Daily = 4,
        Hourly = 5,
    }
}
