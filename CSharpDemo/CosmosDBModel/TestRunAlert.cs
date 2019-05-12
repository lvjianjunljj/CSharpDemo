namespace CSharpDemo.CosmosDBModel
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    /// <summary>
    /// Class TestRunAlert.
    /// </summary>
    public class TestRunAlert
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TestRunAlert" /> class.
        /// Default ctor for Deserialize
        /// </summary>
        public TestRunAlert()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TestRunAlert"/> class.
        /// </summary>
        /// <param name="testRun">The test run.</param>
        public TestRunAlert(TestRun testRun)
        {
            this.Id = new Guid(testRun.Id);
            this.DatasetTestId = testRun.DatasetTestId;
            this.DatasetId = testRun.DatasetId;
            this.AlertSettingId = testRun.AlertSettingId;
            this.Title = $"{testRun.TestName}: {testRun.Message}";
            this.TestCategory = testRun.TestCategory;
            this.TestRunId = testRun.Id;
            this.TestDate = testRun.TestDate;
            this.Status = testRun.Status;
            this.ErrorCode = testRun.ErrorCode;
        }

        /// <summary>
        /// Gets or sets the id of test run alert feed
        /// Should be equal to the Guid of the test run, as one test run can only have one test run alert feed/type
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty("id")]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets the status.
        /// </summary>
        /// <value>The status.</value>
        [JsonProperty("status")]
        public string Status { get; set; }

        /// <summary>
        /// Gets or sets the alertType.
        /// We will save this message in service level CustomField in IcM with name "AlertType".
        /// </summary>
        /// <value>The status.</value>
        [JsonProperty("alertType")]
        public string AlertType { get; set; }

        /// <summary>
        /// Gets or sets the alias of owning contact
        /// </summary>
        /// <value>The alias of owning contact.</value>
        [JsonProperty(PropertyName = "owningContactAlias")]
        public string OwningContactAlias { get; set; }

        /// <summary>
        /// Gets or sets the business owner
        /// We will save this message in service level CustomField in IcM with name "BusinessOwner".
        /// </summary>
        /// <value>The business owner</value>
        [JsonProperty(PropertyName = "businessOwner")]
        public string BusinessOwner { get; set; }

        /// <summary>
        /// Gets or sets the id for this test type
        /// </summary>
        /// <value>The dataset test identifier.</value>
        [JsonProperty(PropertyName = "datasetTestId")]
        public string DatasetTestId { get; set; }

        /// <summary>
        /// Gets or sets the test category for the dataset
        /// </summary>
        /// <value>The dataset test category.</value>
        [JsonProperty(PropertyName = "testCategory")]
        public string TestCategory { get; set; }

        /// <summary>
        /// Gets or sets the id for the dataset of this test
        /// We will save this message in service level CustomField in IcM with name "DatasetId".
        /// </summary>
        /// <value>The dataset identifier.</value>
        [JsonProperty(PropertyName = "datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// Gets or sets the name for the dataset of this test
        /// </summary>
        /// <value>The dataset name.</value>
        [JsonProperty(PropertyName = "datasetName")]
        public string DatasetName { get; set; }

        /// <summary>
        /// Gets or sets the string of impacted date list.
        /// We save the impacted date list in a long string and separate them with semicolon
        /// </summary>
        /// <value>The string of impacted date list.</value>
        [JsonProperty(PropertyName = "impactedDates")]
        public ISet<DateTime> ImpactedDates { get; set; }

        /// <summary>
        /// Gets or sets the test date.
        /// </summary>
        /// <value>The test date.</value>
        [JsonProperty(PropertyName = "testDate")]
        public DateTime? TestDate { get; set; }

        /// <summary>
        /// Gets or sets the string of other test run ids list that got suppressed due to the same alert.
        /// We save the test run id list in a long string and separate them with semicolon
        /// </summary>
        /// <value>The string of other test run ids list that got suppressed due to the same alert.</value>
        [JsonProperty(PropertyName = "suppressedAlertTestRunIds")]
        public string SuppressedAlertTestRunIds { get; set; }

        /// <summary>
        /// Gets or sets the alert setting id, equals the test id and used to get the alert settings
        /// </summary>
        /// <value>The alert setting identifier.</value>
        [JsonProperty("alertSettingId")]
        public string AlertSettingId { get; set; }

        /// <summary>
        /// Gets or sets the target icm container publicId.
        /// </summary>
        /// <value>The target icm container publicId.</value>
        [JsonProperty("containerPublicId")]
        public Guid ContainerPublicId { get; set; }

        /// <summary>
        /// Gets or sets the service level custom field name list.
        /// </summary>
        /// <value>The service level custom field name list.</value>
        [JsonProperty("serviceCustomFieldNames")]
        public string[] ServiceCustomFieldNames { get; set; }

        /// <summary>
        /// Gets or sets the severity.
        /// </summary>
        /// <value>The severity.</value>
        [JsonProperty("severity")]
        public int? Severity { get; set; }

        /// <summary>
        /// Gets or sets the title.
        /// </summary>
        /// <value>The title.</value>
        [JsonProperty("title")]
        public string Title { get; set; }

        /// <summary>
        /// Gets or sets the testRun id.
        /// </summary>
        /// <value>The testRunId.</value>
        [JsonProperty("testRunId")]
        public string TestRunId { get; set; }

        /// <summary>
        /// Gets or sets the error code.
        /// </summary>
        /// <value>The error code.</value>
        [JsonProperty("errorCode")]
        public HttpStatusCode? ErrorCode { get; set; }

        /// <summary>
        /// Gets or sets the content shown in Surface.
        /// We will save this message in service level CustomField in IcM with name "DisplayInSurface".
        /// </summary>
        /// <value>The content shown in Surface.</value>
        [JsonProperty(PropertyName = "displayInSurface")]
        public string DisplayInSurface { get; set; }

        /// <summary>
        /// Gets or sets the timestamp.
        /// </summary>
        /// <value>The timestamp.</value>
        [JsonProperty("timestamp")]
        public DateTime? Timestamp { get; set; }

        /// <summary>
        /// Gets the timestamp ticks.
        /// </summary>
        /// <value>The timestamp ticks.</value>
        [JsonProperty("timestampTicks")]
        public long TimestampTicks
        {
            get { return this.Timestamp.HasValue ? this.Timestamp.Value.Ticks : 0; }
        }

        /// <summary>
        /// Gets or sets the issuedOnDate.
        /// </summary>
        /// <value>The issuedOnDate.</value>
        [JsonProperty("issuedOnDate")]
        public DateTime? IssuedOnDate { get; set; }

        /// <summary>
        /// Gets or sets the acknowledgeDate.
        /// </summary>
        /// <value>The acknowledgeDate.</value>
        [JsonProperty("acknowledgeDate")]
        public DateTime? AcknowledgeDate { get; set; }

        /// <summary>
        /// Gets or sets the mitigation date.
        /// </summary>
        /// <value>The mitigation date.</value>
        [JsonProperty("mitigationDate")]
        public DateTime? MitigationDate { get; set; }

        /// <summary>
        /// Gets or sets the resolvedDate.
        /// </summary>i
        /// <value>The resolvedDate.</value>
        [JsonProperty("resolvedDate")]
        public DateTime? ResolvedDate { get; set; }

        /// <summary>
        /// Gets or sets the lastUpdateDate.
        /// </summary>
        /// <value>The lastUpdateDate.</value>
        [JsonProperty("lastUpdateDate")]
        public DateTime LastUpdateDate { get; set; }

        /// <summary>
        /// Gets or sets the lastUpdate content.
        /// </summary>
        /// <value>The lastUpdate content.</value>
        [JsonProperty("lastUpdate")]
        public string LastUpdate { get; set; }

        /// <summary>
        /// Gets or sets the last sync from IcM date.
        /// </summary>
        /// <value>The lastUpdateDate.</value>
        [JsonProperty("lastSyncDate")]
        public DateTime LastSyncDate { get; set; }

        /// <summary>
        /// Gets or sets the incident identifier.
        /// </summary>
        /// <value>The incident identifier.</value>
        [JsonProperty("incidentId")]
        public long? IncidentId { get; set; }

        /// <summary>
        /// Gets or sets the routing identifier.
        /// </summary>
        /// <value>The routing identifier.</value>
        [JsonProperty("routingId")]
        public string RoutingId { get; set; }

        /// <summary>
        /// Gets or sets the owning team identifier.
        /// </summary>
        /// <value>The owning team identifier.</value>
        [JsonProperty("owningTeamId")]
        public string OwningTeamId { get; set; }

        /// <summary>
        /// Gets or sets the alertStatus.
        /// </summary>
        /// <value>The alertStatus.</value>
        [JsonProperty("alertStatus")]
        public string AlertStatus { get; set; }

        /// <summary>
        /// Gets or sets the on call playbook link.
        /// </summary>
        /// <value>The on call playbook link.</value>
        [JsonProperty("onCallPlaybookLink")]
        public string OnCallPlaybookLink { get; set; }

        /// <summary>
        /// Gets or sets the datacop portal link.
        /// </summary>
        /// <value>The datacop portal link.</value>
        [JsonProperty("dataCopPortalLink")]
        public string DataCopPortalLink { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            // For more info, see https://www.newtonsoft.com/json/help/html/T_Newtonsoft_Json_NullValueHandling.htm
            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            return JsonConvert.SerializeObject(this, jsonSerializerSettings);
        }
    }
}
