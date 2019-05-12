namespace CSharpDemo.CosmosDBModel
{
    using System;
    using System.Net;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;

    public class TestRun
    {
        /// <summary>
        /// Gets or sets the id for this test run
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the request identifier.
        /// </summary>
        /// <value>The request identifier.</value>
        [JsonProperty(PropertyName = "requestId")]
        public string RequestId { get; set; }

        /// <summary>
        /// Gets or sets the id for this test type
        /// </summary>
        /// <value>The dataset test identifier.</value>
        [JsonProperty(PropertyName = "datasetTestId")]
        public string DatasetTestId { get; set; }

        /// <summary>
        /// Gets or sets the alert setting identifier.
        /// </summary>
        /// <value>The alert setting identifier.</value>
        [JsonProperty(PropertyName = "alertSettingId")]
        public string AlertSettingId { get; set; }

        /// <summary>
        /// Gets or sets the name of the test.
        /// </summary>
        /// <value>The name of the test.</value>
        [JsonProperty(PropertyName = "testName")]
        public string TestName { get; set; }

        /// <summary>
        /// Gets or sets the create time.
        /// </summary>
        /// <value>The create time.</value>
        [JsonProperty(PropertyName = "createTime")]
        public DateTime CreateTime { get; set; }

        /// <summary>
        /// Gets or sets StartTime field.
        /// Time in which the worker started picking up the run.
        /// </summary>
        /// <value>The start time.</value>
        [JsonProperty(PropertyName = "startTime")]
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets the ticks of startTime - used to get the latest test run from DB
        /// </summary>
        /// <value>The start time ticks.</value>
        [JsonProperty("startTimeTicks")]
        public long StartTimeTicks
        {
            get { return this.StartTime.Ticks; }
        }

        /// <summary>
        /// Gets or sets the end time.
        /// </summary>
        /// <value>The end time.</value>
        [JsonProperty(PropertyName = "endTime")]
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets the status.
        /// Note: For AlertHtmlStyle input, Dictionary is not a valid attribute parameter type, so we need to set the BgColorMap by inputing a string array then iterate over the data into the Dictionary.
        /// </summary>
        /// <value>The status.</value>
        [JsonProperty(PropertyName = "status")]
        public string Status { get; set; }

        /// <summary>
        /// Gets or sets the dataset identifier.
        /// </summary>
        /// <value>The dataset identifier.</value>
        [JsonProperty(PropertyName = "datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// Gets or sets the data fabric.
        /// </summary>
        /// <value>The data fabric.</value>
        [JsonProperty(PropertyName = "dataFabric")]
        public string DataFabric { get; set; }

        /// <summary>
        /// Gets or sets the test result expiration time, which determines how
        /// long the result is considered fresh.
        /// </summary>
        /// <value>The test run cadence.</value>
        [JsonProperty(PropertyName = "resultExpirePeriod")]
        public TimeSpan ResultExpirePeriod { get; set; }

        /// <summary>
        /// Gets or sets the test category.
        /// </summary>
        /// <value>The test category.</value>
        [JsonProperty(PropertyName = "testCategory")]
        public string TestCategory { get; set; }

        /// <summary>
        /// Gets or sets the test date.
        /// </summary>
        /// <value>The test date.</value>
        [JsonProperty(PropertyName = "testDate")]
        public DateTime TestDate { get; set; }

        /// <summary>
        /// Gets the ticks of testDate
        /// </summary>
        /// <value>The test date ticks.</value>
        [JsonProperty("testDateTicks")]
        public long TestDateTicks
        {
            get { return this.TestDate.Ticks; }
        }

        /// <summary>
        /// Gets or Sets the Time To Live in seconds for this db entry.
        /// The time to live for this test run entry in cosmos db, in seconds.
        /// The default of -1 ensures the entry will not expire.
        /// Utilize the default value in all cases unless needed.
        /// </summary>
        [JsonProperty("ttl")]
        public int TimeToLive { get; set; } = -1;

        /// <summary>
        /// Gets or sets the content of the test.
        /// Note: For AlertHtmlStyle input, Func is not a valid attribute parameter type, so we need to set the ParseFunction by inputing a string then using reflection to get the target function.
        /// </summary>
        /// <value>The content of the test.</value>
        [JsonProperty(PropertyName = "testContent")]
        public JToken TestContent { get; set; }

        /// <summary>
        /// Gets or sets error code of the test run
        /// </summary>
        [JsonProperty(PropertyName = "errorCode")]
        public HttpStatusCode? ErrorCode { get; set; }

        /// <summary>
        /// Gets or sets the details of the test run result.
        /// </summary>
        /// <value>The details of the test run result.</value>
        [JsonProperty(PropertyName = "testRunDetails")]
        public JToken TestRunDetails { get; set; }

        /// <summary>
        /// Gets or sets the message.
        /// </summary>
        /// <value>The message.</value>
        [JsonProperty(PropertyName = "message")]
        public string Message { get; set; }

        /// <summary>
        /// Gets or sets the type of the test content.
        /// </summary>
        /// <value>The type of the test content.</value>
        [JsonProperty(PropertyName = "testContentType")]
        public string TestContentType { get; set; }

        /// <summary>
        /// If not provided, worker assumes test returning a single boolean result
        /// </summary>
        /// <value>The test result schema.</value>
        [JsonProperty("testResultSchema")]
        public string[] TestResultSchema { get; set; }

        /// <summary>
        /// This is optional property
        /// </summary>
        /// <value>The type of the test status checker.</value>
        [JsonProperty("testStatusCheckerType")]
        public string TestStatusCheckerType { get; set; }

        /// <summary>
        /// This is optional property
        /// </summary>
        /// <value>The test status checker.</value>
        [JsonProperty("testStatusChecker")]
        public JToken TestStatusChecker { get; set; }

        /// <summary>
        /// Sets the Result Expire Period Old Name
        /// </summary>
        /// <value>The old name of the result expire period.</value>
        [JsonProperty(PropertyName = "testRunCadence")]
        private TimeSpan ResultExpirePeriodOldName
        {
            set { this.ResultExpirePeriod = value; }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Document" /> to <see cref="TestRun" />.
        /// </summary>
        /// <param name="doc">The document.</param>
        /// <returns>The result of the conversion.</returns>
        public static explicit operator TestRun(Document doc)
        {
            TestRun testRun = JsonConvert.DeserializeObject<TestRun>(doc.ToString());
            return testRun;
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
