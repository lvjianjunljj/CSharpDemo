using System.Threading.Tasks;

namespace CSharpDemo.Application
{
    interface ISecretProvider
    {
        Task<string> GetSecretAsync(string keyVaultName, string secretName);
    }
}
