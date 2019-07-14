using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace CSharpDemo.Azure
{
    // OneNote link: https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Azure.one%7C82D4B6EF-4D8B-4376-9D6B-EE7003D7D902%2FKey-vault%7C7FFD6EA2-D081-4426-849C-60C3DB2A6B1C%2F%29
    class AzureKeyVault
    {

        // string connectionString = AzureKeyVault.GetSecret("datacopdev", "ServiceBusConnectionString");
        // string connectionString = AzureKeyVault.GetSecret("csharpmvcwebapikeyvault", "AppSecret");


        // It is just lie a online key-value NoSql.

        // It is using managed identities for Azure resources. 
        // So there is no other certification when get the secret from keyvault in Azure.
        public static string GetSecret(string secretName)
        {
            // It is using managed identities for Azure resources. 
            // So there is no other certification when get the secret from keyvault in Azure.
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            string vaultUri = "https://csharpmvcwebapikeyvault.vault.azure.net/";
            SecretBundle secret = keyVaultClient.GetSecretAsync(vaultUri, secretName).Result;
            return secret.Value;
        }
        public async Task<string> GetSecretAsync(string secretName)
        {
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            string vaultUri = "https://csharpmvcwebapikeyvault.vault.azure.net/";
            SecretBundle secret = await keyVaultClient.GetSecretAsync(vaultUri, secretName);

            return secret.Value;
        }

        public static string GetSecret(string keyVaultName, string secretName)
        {
            // It is using managed identities for Azure resources. 
            // So there is no other certification when get the secret from keyvault in Azure.
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            string vaultUri = $"https://{keyVaultName}.vault.azure.net/";
            SecretBundle secret = keyVaultClient.GetSecretAsync(vaultUri, secretName).Result;
            return secret.Value;
        }

        public static void GetAllFromKeyVault(string vaultName, string appId, string appSecret)
        {
            if (string.IsNullOrWhiteSpace(vaultName))
            {
                return;
            }

            KeyVaultClient keyClient;

            try
            {
                DelegatingHandler[] handler = { };

                keyClient = new KeyVaultClient(
                    async (authority, resource, scope) =>
                    {
                        var adCredential = new ClientCredential(appId, appSecret);
                        var authenticationContext = new AuthenticationContext(authority, null);
                        var result = await authenticationContext.AcquireTokenAsync(resource, adCredential);
                        return result.AccessToken;
                    },
                    handler);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return;
            }

            string vaultUri = $"https://{vaultName.Trim()}.vault.azure.net";

            try
            {
                // Actually for now, what I need is just the clientSecret
                //var clientSecret = keyClient.GetSecretAsync("MetagraphDataIngestionAppKatanaClientSecret");
                var secretsResponseMessage = keyClient.GetSecretsAsync(vaultUri).Result;

                while (true)
                {
                    using (var enumerator = secretsResponseMessage.GetEnumerator())
                    {
                        while (enumerator.MoveNext())
                        {
                            string secretUri = enumerator.Current.Id;
                            try
                            {
                                var retrievedSecret = keyClient.GetSecretAsync(secretUri).Result;
                                //this.AddSecret(retrievedSecret, vaultName);
                                //Console.WriteLine(string.Format("{0}\t{1}", retrievedSecret, vaultName));
                                ShowSecret(retrievedSecret, vaultName);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Error: " + e.Message);
                            }
                        }
                    }

                    if (string.IsNullOrEmpty(secretsResponseMessage.NextPageLink))
                    {
                        break;
                    }

                    secretsResponseMessage = keyClient.GetSecretsNextAsync(secretsResponseMessage.NextPageLink).Result;
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }

        }

        private static void ShowSecret(SecretBundle secret, string vaultName)
        {
            //// if (string.Equals(secret.ContentType, "application/x-pkcs12", StringComparison.OrdinalIgnoreCase))
            if (!string.IsNullOrEmpty(secret.ContentType))
            {
                //this.AddCert(secret, vaultName);
            }
            else
            {
                string secretName = secret.SecretIdentifier.Name;
                Console.WriteLine(string.Format("{0}\t{1}", secretName, secret.Value));
                //if (!this.secretsMap.ContainsKey(secretName))
                //{
                //    this.secretsMap.Add(secretName, secret.Value);
                //}
            }
        }

        public static void MainMethod()
        {
            AzureKeyVault AzureKeyVaultClass = new AzureKeyVault();
            Task<string> azureKeyVaultTask = AzureKeyVaultClass.GetSecretAsync("AppSecret");
            Console.WriteLine(azureKeyVaultTask.Result);
            string appSecretValue = GetSecret("AppSecret");
            Console.WriteLine(appSecretValue);

            // There are four ways to get the secret from Azure keyvault and this is the way according to the appId and AppSecret.
            //  This code is to get all the secrets for Substrate Portal.


            // This three parameter is for Substrate Portal, you can get it from the team members.
            //string vaultName = "";
            //string appId = "";
            //string appSecret = "";
            //GetAllFromKeyVault(vaultName, appId, appSecret);
        }
    }
}
