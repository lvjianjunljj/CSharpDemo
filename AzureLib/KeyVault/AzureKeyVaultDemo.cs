// OneNote link: https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Azure.one%7C82D4B6EF-4D8B-4376-9D6B-EE7003D7D902%2FKey-vault%7C7FFD6EA2-D081-4426-849C-60C3DB2A6B1C%2F%29
namespace AzureLib.KeyVault
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.Azure.KeyVault;
    using Microsoft.Azure.KeyVault.Models;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Newtonsoft.Json;

    public class AzureKeyVaultDemo
    {
        // string connectionString = AzureKeyVault.GetSecret("datacopdev", "ServiceBusConnectionString");
        // string connectionString = AzureKeyVault.GetSecret("csharpmvcwebapikeyvault", "AppSecret");

        public static void MainMethod()
        {
            //FailedGetAllSecretDemo();
            MigrateDataBuildSecrets();
        }

        public static void FailedGetAllSecretDemo()
        {
            // I can get a specific secret but cannot get all secret by the function GetSecretsAsync(string vaultBaseUrl)
            var secret = GetSecret("databuild2-ppe", "CloudScopePPEServiceBusReadWriteKey");
            Console.WriteLine(secret);

            try
            {
                var secretDict = GetAllFromKeyVault("databuild2-ppe");
                var secretDictStr = JsonConvert.SerializeObject(secretDict);
                Console.WriteLine(secretDictStr);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Getting all secrets failed: {e.Message}");
            }
        }

        public static void MigrateDataBuildSecrets()
        {
            string sourcekeyVaultName = "databuild2-ppe";
            string targetkeyVaultName = "cloudscopeppe";

            IList<string> secretNames = new List<string>(){"CloudScopeCosmosDbConnectionString",
                                "CloudScopeHcmAgentMachineUsername",
                                "CloudScopeMonitorDataPushStorageConnectionString",
                                "CloudScopePPECosmosDbConnectionString",
                                "CloudScopePPEHcmAgentMachinePassword",
                                "CloudScopePPEHcmAgentMachineUsername",
                                "CloudScopePPEServiceBusReadWriteKey",
                                "DataBuildCosmosDbConnectionString",
                                "DataBuildDeploySingleTenantClientSecret",
                                "DataBuildServiceBusConnectionString",
                                "DataBuildStoragePpeConnectionString",
                                "DataBuildVNextStorageConnectionString",
                                "DataCopCosmosDbAuthKey",
                                "DataCopCosmosDbEndpoint",
                                "DBSamplesBatchKey",
                                "DBSamplesSqlDbConnectionString",
                                "DeployStorageConnectionString",
                                "IDEAsModelScoringAccessKey",
                                "IDEAsTestModelScoringAccessKey",
                                "LogStorageConnectionString",
                                "MultiTenantAppClientSecret",
                                "SensitiveProdDatabricksAccessKey",
                                "SensitiveTestDatabricksAccessKey",
                                "TrackerStorageConnectionString"};

            //IList<string> secretValues = GetSecrets(sourcekeyVaultName, secretNames);
            //string secretValuesStr = JsonConvert.SerializeObject(secretValues);
            string secretValuesStr = "[\"DefaultEndpointsProtocol=https;AccountName=cloudscope;AccountKey=2CrNyIgnRrIUFkttrlCO0ntIjPXJ8Wwt4x8Hi9aPwyniGFk9FAnJ2kr70UeLzxbNJuwShbsdlcSDvwtjqMWJ8A==;TableEndpoint=https://cloudscope.table.cosmos.azure.com:443/;\",\"databuildvnext\",\"DefaultEndpointsProtocol=https;AccountName=cloudscopestatemonitor;AccountKey=7ivbqkBALcpne4vLWSCPfy4H6yDiIu4QbxteWJDs1Q8g86+Ed9MkbMMKe09+Ct4EzFFjChrquHG0F64ycuzKDg==;BlobEndpoint=https://cloudscopestatemonitor.blob.core.windows.net/;TableEndpoint=https://cloudscopestatemonitor.table.core.windows.net/;QueueEndpoint=https://cloudscopestatemonitor.queue.core.windows.net/;FileEndpoint=https://cloudscopestatemonitor.file.core.windows.net/\",\"DefaultEndpointsProtocol=https;AccountName=cloudscopeppe;AccountKey=Oiul2jgwek4kmGlSrAxX6IXQni8l7DMLSArpjU1ECcsGaGau3URXqwh9iHnFqrYfJhUm8olkusPlDr2wO1y1bg==;TableEndpoint=https://cloudscopeppe.table.cosmos.azure.com:443/;\",\"Zodeb?swud!9l2lg\",\"databuildvnext\",\"Endpoint=sb://cloudscopeservicebus-ppe.servicebus.windows.net/;SharedAccessKeyName=ReadWriteKey;SharedAccessKey=Bjl3W9B9ETDKrA21G/zi2CRWnpZ583peYRLCcLiVoOE=\",\"DefaultEndpointsProtocol=https;AccountName=databuild-ppe;AccountKey=JP6zCmtkDydbtDK1GBPruFiJNNBrzJnEPKmTFifcDjAlC2toCcZBBuYlq602FGLn6vWgKCl89wfLkroE7Nx59g==;TableEndpoint=https://databuild-ppe.table.cosmos.azure.com:443/;\",\"kWn_A23Kqrjc8-0-rU.G-P3xZrHQqOKWF9\",\"Endpoint=sb://databuild-ppe.servicebus.windows.net/;SharedAccessKeyName=DataBuildDeployReadWriteAccess;SharedAccessKey=MsBTp9PTASwWBxEx3GeX9S+zzOF2k1xA7DwkLRcOxuQ=\",\"DefaultEndpointsProtocol=https;AccountName=databuild2ppe;AccountKey=rXQ0S+ULvAMtd19cTkAtM4V79HM1hbBpXiih98iriFj5yP9XucI66ZHL7DvSLpGr3aHc9qG458KTHQMtp7JB+w==;EndpointSuffix=core.windows.net\",\"DefaultEndpointsProtocol=https;AccountName=databuilddeployppe;AccountKey=XzF6G+3GS9GyRnOAO5EFzPoO/ULJirHOmkL72M6TZmA3V8AnUh/OCcjuc0Np6K5J+gGVvZjV7SP7Mk0018Ak1A==;EndpointSuffix=core.windows.net\",\"4OGFKJLdwmwEZd30rYOPcVeeWyhF9Ig7fb4tQPJoCMWGnfRVaCP17N0bBtFnGi7Nn7dIydvlmsVfRurvYZxVpw==\",\"https://datacopppe.documents.azure.com:443/\",\"mK+5hUUqIXzZMfJKj1eFHpRCjNgGOH1708vVAlvXk5qlUs2tACMLm6rHxrok4++yWobXR/h1aLOLb3mT6pvCXg==\",\"Server=tcp:databuild2test.database.windows.net,1433;Initial Catalog=CountedActionTestDb;Persist Security Info=False;User ID=DataBuildSqlUser;Password=z?,dmHEXB1zz,LtYmam/He{ied}^9gw=;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;\",\"DefaultEndpointsProtocol=https;AccountName=databuilddeployppe;AccountKey=XzF6G+3GS9GyRnOAO5EFzPoO/ULJirHOmkL72M6TZmA3V8AnUh/OCcjuc0Np6K5J+gGVvZjV7SP7Mk0018Ak1A==;EndpointSuffix=core.windows.net\",\"placeholder\",\"dapic6b657b377fa9a40ea8d819cb02a54e0\",\"DefaultEndpointsProtocol=https;AccountName=databuildlogsppe;AccountKey=mnUx3E6O0VZvoNtXvezijEHWEv+lT83Y15um3eZr1YbuKEu2u4a8dgPn0r0cb1HcKFUzvgU2z2ufsOfsLJW6Ew==;EndpointSuffix=core.windows.net\",\"1i8FrkBL_?RL5cAKsKaWBl:[npMOW933\",\"placeholder\",\"placeholder\",\"DefaultEndpointsProtocol=https;AccountName=databuildtrackersppe;AccountKey=TkPjjppFigSA42TaLB5JKNH26/+OHmrOnCtekNZdEqh1tGdiXmTUANJaXzxBkNVCQS/DlWBABMEAYhQBjS0TVw==;EndpointSuffix=core.windows.net\"]";
            IList<string> secretValues = JsonConvert.DeserializeObject<IList<string>>(secretValuesStr);
            for (int i = 0; i < secretNames.Count; i++)
            {
                SetSecret(targetkeyVaultName, secretNames[i], secretValues[i]);
            }
        }

        public static void GetAllFromKeyVaultDemo()
        {
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

        public static IList<string> GetSecrets(string keyVaultName, IList<string> secretNames)
        {
            IList<string> secretValues = new List<string>();
            foreach (var secretName in secretNames)
            {
                secretValues.Add(GetSecret(keyVaultName, secretName));
            }

            return secretValues;
        }

        public static Dictionary<string, string> GetAllFromKeyVault(string keyVaultName)
        {
            var secretDict = new Dictionary<string, string>();
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            string vaultUri = $"https://{keyVaultName}.vault.azure.net/";
            var secrets = keyVaultClient.GetSecretsAsync(vaultUri).Result;
            foreach (var secret in secrets)
            {
                secretDict.Add(secret.Identifier.Name, keyVaultClient.GetSecretAsync(vaultUri, secret.Identifier.Name).Result.Value);
            }
            return secretDict;
        }

        public static void SetSecret(string keyVaultName, string secretName, string secretValue)
        {
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            string vaultUri = $"https://{keyVaultName}.vault.azure.net/";
            keyVaultClient.SetSecretAsync(vaultUri, secretName, secretValue).Wait();
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
    }
}
