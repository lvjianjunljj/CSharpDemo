namespace AzureLib.KeyVault
{
    using System;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading.Tasks;

    public class AppIdKeyVaultSecretProvider : ISecretProvider
    {
        string appId, appSecret;

        public AppIdKeyVaultSecretProvider(string appId, string appSecret)
        {
            this.appId = appId;
            this.appSecret = appSecret;
        }


        public async Task<string> GetSecretAsync(string keyVaultName, string secretName)
        {
            var keyVaultDns = $"https://{keyVaultName}.vault.azure.net";
            var helper = new KeyVaultHelper(this.appId, this.appSecret, keyVaultDns);
            var bundle = await helper.GetSecret(secretName);
            return bundle.Value;
        }

        public async Task<X509Certificate2> GetCertificateAsync(string keyVaultName, string secretName)
        {
            var bundle = await this.GetSecretAsync(keyVaultName, secretName);
            byte[] privateKeyBytes = Convert.FromBase64String(bundle);
            return new X509Certificate2(privateKeyBytes, (string)null, X509KeyStorageFlags.MachineKeySet);
        }
    }
}
