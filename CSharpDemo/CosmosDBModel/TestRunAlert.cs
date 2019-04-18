using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.CosmosDBModel
{
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

        /// <summary>
        /// Gets or sets the id of test run alert feed
        /// Should be equal to the Guid of the test run, as one test run can only have one test run alert feed/type
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty("id")]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets the alias of owning contact
        /// </summary>
        /// <value>The alias of owning contact.</value>
        [JsonProperty(PropertyName = "owningContactAlias")]
        public string OwningContactAlias { get; set; }
        [JsonProperty(PropertyName = "description")]
        public string Description { get; set; }
        /// <summary>
        /// Gets or sets the id for this test type
        /// </summary>
        /// <value>The dataset test identifier.</value>
        [JsonProperty(PropertyName = "datasetTestId")]
        public string DatasetTestId { get; set; }

        /// <summary>
        /// Gets or sets the id for the dataset of this test
        /// </summary>
        /// <value>The dataset identifier.</value>
        [JsonProperty(PropertyName = "datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// Gets or sets the alert setting id, equals the test id and used to get the alert settings
        /// </summary>
        /// <value>The alert setting identifier.</value>
        [JsonProperty("alertSettingId")]
        public string AlertSettingId { get; set; }

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
        /// Gets or sets the content shown in Surface
        /// </summary>
        /// <value>The content shown in Surface.</value>
        [JsonProperty(PropertyName = "showInSurface")]
        public string ShowInSurface { get; set; }

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
        /// Gets or sets the impactedDate.
        /// </summary>
        /// <value>The impactedDate.</value>
        [JsonProperty("impactedDate")]
        public DateTime? ImpactedDate { get; set; }

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
        /// Gets or sets the environment.
        /// </summary>
        /// <value>The environment.</value>
        [JsonProperty("environment")]
        public string Environment { get; set; }

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
