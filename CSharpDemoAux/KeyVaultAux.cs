using System;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;
namespace CSharpDemoAux
{
    public class KeyVaultAux
    {
        /*
         * Just update the dependentAssembly tab for Microsoft.IdentityModel.Clients.ActiveDirectory in 
         * App.config in project CSharpDemo and the call of this function will work.
         */
        public static void TestKeyVault()
        {
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            string vaultUri = @"https://datacopdev.vault.azure.net/";
            SecretBundle secret = keyVaultClient.GetSecretAsync(vaultUri, "CosmosDBAuthKey").Result;

            Console.WriteLine(secret.Value);
        }
    }
}
