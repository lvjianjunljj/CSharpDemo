namespace AzureLib.KeyVault
{
    using Microsoft.Azure.KeyVault;
    using Microsoft.Azure.KeyVault.Models;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Class KeyVaultHelper.
    /// </summary>
    class KeyVaultHelper
    {
        /// <summary>
        /// Gets the key vault client identifier.
        /// </summary>
        /// <value>The key vault client identifier.</value>
        public string KeyVaultClientId { get; private set; }
        /// <summary>
        /// Gets the key vault secret.
        /// </summary>
        /// <value>The key vault secret.</value>
        public string KeyVaultSecret { get; private set; }
        /// <summary>
        /// Gets the key vault DNS.
        /// </summary>
        /// <value>The key vault DNS.</value>
        public Uri KeyVaultDns { get; private set; }

        /// <summary>
        /// The client
        /// </summary>
        private KeyVaultClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyVaultHelper"/> class.
        /// </summary>
        /// <param name="clientId">The client identifier.</param>
        /// <param name="clientSecret">The client secret.</param>
        /// <param name="keyVaultDns">The key vault DNS.</param>
        public KeyVaultHelper(string clientId, string clientSecret, string keyVaultDns)
        {
            this.KeyVaultClientId = clientId;
            this.KeyVaultSecret = clientSecret;
            this.KeyVaultDns = new Uri(keyVaultDns);

            client = new KeyVaultClient(GetToken);
        }


        /// <summary>
        /// Gets the token.
        /// </summary>
        /// <param name="authority">The authority.</param>
        /// <param name="resource">The resource.</param>
        /// <param name="scope">The scope.</param>
        /// <returns>Task&lt;System.String&gt;.</returns>
        /// <exception cref="InvalidOperationException">Failed to obtain the JWT token</exception>
        private async Task<string> GetToken(string authority, string resource, string scope)
        {
            var authContext = new AuthenticationContext(authority);
            ClientCredential clientCred = new ClientCredential(KeyVaultClientId, KeyVaultSecret);

            // Note: An exception here can indicate that the local cert has become corrupted. Please first try and install it again.
            AuthenticationResult result = await authContext.AcquireTokenAsync(resource, clientCred);

            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            return result.AccessToken;
        }

        /// <summary>
        /// Gets the secret.
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <returns>Task&lt;SecretBundle&gt;.</returns>
        /// <exception cref="Exception"></exception>
        public async Task<SecretBundle> GetSecret(string identifier)
        {
            var uri = new Uri(this.KeyVaultDns, "secrets/" + identifier);
            string secretIdentifier = uri.OriginalString;

            try
            {
                var result = await client.GetSecretAsync(secretIdentifier);
                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Unable to retreive secret identifier '{secretIdentifier}' from Key Vault . Error: {ex.Message}");
            }
        }
    }
}
