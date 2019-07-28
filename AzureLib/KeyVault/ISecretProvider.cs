namespace AzureLib.KeyVault
{
    using System.Threading.Tasks;

    public interface ISecretProvider
    {
        Task<string> GetSecretAsync(string keyVaultName, string secretName);
    }
}
