namespace CSharpDemo.CosmosDBModel
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json;

    /// <summary>
    /// Class TestRunAlertSettings.
    /// </summary>
    public class TestRunAlertSettings
    {
        /// <summary>
        /// Gets or sets the alert settings Id. It should be equal to the test Id
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty("id")]
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the target icm connector.
        /// This property is required and can't be null during deserialization
        /// </summary>
        /// <value>The target icm connector.</value>
        [JsonProperty("targetIcMConnector", Required = Required.Always)]
        public Guid TargetIcMConnector { get; set; }

        /// <summary>
        /// Gets or sets the target icm container publicId.
        /// This property is for the setting of custom fields.
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
        public int Severity { get; set; }

        /// <summary>
        /// Gets or sets the number of alerts that need to fire before an action is executed
        /// </summary>
        /// <value>The alert threshold.</value>
        [JsonProperty("alertThreshold")]
        public int AlertThreshold { get; set; }

        /// <summary>
        /// Gets or sets the interval in minutes where the threshold must be reached
        /// </summary>
        /// <value>The alert interval mins.</value>
        [JsonProperty("alertIntervalMins")]
        public int AlertIntervalMins { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is alert suppression enabled.
        /// </summary>
        /// <value><c>true</c> if this instance is alert suppression enabled; otherwise, <c>false</c>.</value>
        public bool IsAlertSuppressionEnabled { get; set; }

        /// <summary>
        /// Gets or sets the where itself and it's children alerts will be suppressed in minutes
        /// </summary>
        /// <value>The suppression mins.</value>
        [JsonProperty("suppressionMins")]
        public int SuppressionMins { get; set; }

        /// <summary>
        /// Gets or sets the routing identifier.
        /// This property is required and can't be null during deserialization
        /// </summary>
        /// <value>The routing identifier.</value>
        [JsonProperty("routingId", Required = Required.Always)]
        public string RoutingId { get; set; }

        /// <summary>
        /// Gets or sets the owning team identifier.
        /// This property is required and can't be null during deserialization
        /// </summary>
        /// <value>The owning team identifier.</value>
        [JsonProperty("owningTeamId", Required = Required.Always)]
        public string OwningTeamId { get; set; }

        /// <summary>
        /// Gets or sets the environment.
        /// </summary>
        /// <value>The environment.</value>
        [JsonProperty("environment")]
        public string Environment { get; set; }

        /// <summary>
        /// Gets or sets the dependencies.
        /// </summary>
        /// <value>The dependencies.</value>
        [JsonProperty("dependencies")]
        public List<Guid> Dependencies { get; set; }

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
        /// Convert the object to string
        /// </summary>
        /// <returns>string</returns>
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
