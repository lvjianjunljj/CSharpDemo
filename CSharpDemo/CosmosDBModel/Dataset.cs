using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.CosmosDBModel
{
    public class Dataset
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the create time.
        /// </summary>
        /// <value>The create time.</value>
        [JsonProperty(PropertyName = "createTime")]
        public DateTime CreateTime { get; set; }

        /// <summary>
        /// Gets or sets the last modified time.
        /// </summary>
        /// <value>The last modified time.</value>
        [JsonProperty(PropertyName = "lastModifiedTime")]
        public DateTime LastModifiedTime { get; set; }

        /// <summary>
        /// Gets or sets the created by.
        /// </summary>
        /// <value>The created by.</value>
        [JsonProperty(PropertyName = "createdBy")]
        public string CreatedBy { get; set; }

        /// <summary>
        /// Gets or sets the last modified by.
        /// </summary>
        /// <value>The last modified by.</value>
        [JsonProperty(PropertyName = "lastModifiedBy")]
        public string LastModifiedBy { get; set; }

        /// <summary>
        /// Gets or sets the connection information.
        /// </summary>
        /// <value>The connection information.</value>
        [JsonProperty(PropertyName = "connectionInfo")]
        public JToken ConnectionInfo { get; set; }

        // TODO - Possible enum value.

        /// <summary>
        /// Gets or sets the state.
        /// </summary>
        /// <value>The state.</value>
        [JsonProperty(PropertyName = "state")]
        public string State { get; set; }

        /// <summary>
        /// Gets or sets the data fabric.
        /// </summary>
        /// <value>The data fabric.</value>
        [JsonProperty(PropertyName = "dataFabric")]
        public string DataFabric { get; set; }

        /// <summary>
        /// Gets or sets the type.
        /// </summary>
        /// <value>The type.</value>
        [JsonProperty(PropertyName = "type")]
        public string Type { get; set; }

        /// <summary>
        /// Gets or sets the category.
        /// </summary>
        /// <value>The category.</value>
        [JsonProperty(PropertyName = "category")]
        public string Category { get; set; }

        /// <summary>
        /// Gets or sets the start date.
        /// </summary>
        /// <value>The start date.</value>
        [JsonProperty(PropertyName = "startDate")]
        public string StartDate { get; set; }

        /// <summary>
        /// Gets or sets the end date.
        /// </summary>
        /// <value>The end date.</value>
        [JsonProperty(PropertyName = "endDate")]
        public string EndDate { get; set; }

        /// <summary>
        /// Gets or sets the start serial number.
        /// </summary>
        /// <value>The start serial number.</value>
        [JsonProperty(PropertyName = "startSerialNumber")]
        public int? StartSerialNumber { get; set; }

        /// <summary>
        /// Gets or sets the end serial number.
        /// </summary>
        /// <value>The end serial number.</value>
        [JsonProperty(PropertyName = "endSerialNumber")]
        public int? EndSerialNumber { get; set; }

        /// <summary>
        /// Gets or sets the rolling window.
        /// </summary>
        /// <value>The rolling window.</value>
        [JsonProperty(PropertyName = "rollingWindow")]
        public TimeSpan? RollingWindow { get; set; }

        /// <summary>
        /// Gets or sets the grain.
        /// </summary>
        /// <value>The grain.</value>
        [JsonProperty(PropertyName = "grain")]
        public string Grain { get; set; }

        /// <summary>
        /// Gets or sets the sla.
        /// </summary>
        /// <value>The sla.</value>
        [JsonProperty(PropertyName = "sla")]
        public string SLA { get; set; }

        /// <summary>
        /// Gets or sets the schema.
        /// </summary>
        /// <value>The schema.</value>
        [JsonProperty(PropertyName = "schema")]
        public string Schema { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is enabled.
        /// </summary>
        /// <value><c>true</c> if this instance is enabled; otherwise, <c>false</c>.</value>
        [JsonProperty("isEnabled")]
        public bool IsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the children.
        /// The list contains dataset Ids that the dataset depends on
        /// </summary>
        /// <value>The children.</value>
        /// Hashset is not directly serialisable. Since the size of the list will be small, using list is fine here.
        [JsonProperty("children")]
        public List<string> Children { get; set; }
    }
}
