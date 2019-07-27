using System.Threading.Tasks;

namespace CSharpDemo.Application
{
    public interface ISecretProvider
    {
        Task<string> GetSecretAsync(string keyVaultName, string secretName);
    }
}
