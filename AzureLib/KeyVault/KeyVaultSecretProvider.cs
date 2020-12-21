namespace AzureLib.KeyVault
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Azure.KeyVault;
    using Microsoft.Azure.KeyVault.Models;
    using Microsoft.Azure.Services.AppAuthentication;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class KeyVaultSecretProvider : ISecretProvider
    {
        private const string JsonFilePath = @"D:\data\company_work\M365\IDEAs\secrets.json";

        private static Lazy<KeyVaultSecretProvider> secretProvider = new Lazy<KeyVaultSecretProvider>(() => new KeyVaultSecretProvider());

        private KeyVaultSecretProvider()
        {
        }

        public static KeyVaultSecretProvider Instance => secretProvider.Value;

        /// <summary>
        /// Get secret from Azure Key Vault using the managed crediential.
        /// Details see https://github.com/Azure-Samples/app-service-msi-keyvault-dotnet
        /// </summary>
        /// <param name="keyVaultName"></param>
        /// <param name="secretName"></param>
        /// <returns></returns>
        public async Task<string> GetSecretAsync(string keyVaultName, string secretName)
        {
            try
            {
                AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
                KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
                string vaultUri = $"https://{keyVaultName}.vault.azure.net/";
                SecretBundle secret = await keyVaultClient.GetSecretAsync(vaultUri, secretName);
                return secret.Value;
            }
            //catch (AzureServiceTokenProviderException ae)
            //{
            //    if (ae.Message.Contains("[No connection string specified]"))
            //    {

            //        return GetSecretByJson(keyVaultName, secretName);
            //    }

            //    throw;
            //}
            catch (Exception e)
            {
                Console.WriteLine($"Get secret '{secretName}' from keyvault '{keyVaultName}' throw exception.");
                return GetSecretByJson(keyVaultName, secretName);
            }
        }

        private string GetSecretByJson(string keyVaultName, string secretName)
        {
            string jsonContent = File.ReadAllText(JsonFilePath, Encoding.UTF8);
            var jarray = JArray.Parse(jsonContent);
            foreach (var keyVaultJson in jarray)
            {
                if (keyVaultJson["keyVaultName"].ToString().Equals(keyVaultName))
                {
                    foreach (var secretJson in keyVaultJson["secrets"].ToArray())
                    {
                        if (secretJson["secretName"].ToString().Equals(secretName))
                        {
                            return secretJson["secretValue"].ToString();
                        }
                    }
                }
            }

            throw new Exception($"Cannot get the secret '{keyVaultName} - {secretName}' from local json file, please update the file.");
        }
    }
}
