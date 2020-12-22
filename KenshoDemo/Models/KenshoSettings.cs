namespace KenshoDemo.Models
{
    /// <summary>
    /// Class KenshoSettings.
    /// </summary>
    public class KenshoSettings
    {
        /// <summary>
        /// Gets or sets the name of VC that DataCop's Cosmos data streams are in.
        /// </summary>
        /// <value>Keyvault key name.</value>
        public string Environment { get; set; }

        /// <summary>
        /// Gets or sets the name of the secret name for Kensho, which contains the name of the API tenant key for keyVault.
        /// </summary>
        /// <value>Keyvault key name.</value>
        public string APITenantKey { get; set; }

        /// <summary>
        /// Gets or sets the name of VC that DataCop's Cosmos data streams are in.
        /// </summary>
        /// <value>Keyvault key name.</value>
        public string CosmosVC { get; set; }

        /// <summary>
        /// Gets or sets the name of the URL root for Kensho UI.
        /// </summary>
        /// <value>Partial URL.</value>
        public string UIURL { get; set; }

        /// <summary>
        /// Gets or sets the name of the URL root for Kensho API commands.
        /// </summary>
        /// <value>Partial URL.</value>
        public string APIURL { get; set; }

        /// <summary>
        /// Gets or sets the name of the Keyvault entry that contains the backend API key.
        /// </summary>
        /// <value>Partial URL.</value>
        public string BackendAPITenantKey { get; set; }

        /// <summary>
        /// Gets or sets the name of the URL the backend API (used for on the fly anomaly detection).
        /// </summary>
        /// <value>Partial URL.</value>
        public string BackendAPIURL { get; set; }
    }
}
