namespace CosmosDemo.CosmosView
{
    using System;
    using Newtonsoft.Json;

    /// <summary>
    /// The class contains the info to define a Cosmos View parameter
    /// </summary>
    public class CosmosViewParameter
    {
        /// <summary>
        /// Gets or sets the name of the view parameter
        /// </summary>
        [JsonProperty("name")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the data type of the parameter
        /// </summary>
        [JsonProperty("type")]
        public Type Type { get; set; }

        /// <summary>
        /// Gets or sets the value of the parameter in string format.
        /// If the value start with @@, it means in the DataCop auto generated script, its value will be assigned by an external parameter on flight.
        /// </summary>
        [JsonProperty("value")]
        public string Value { get; set; }
    }
}