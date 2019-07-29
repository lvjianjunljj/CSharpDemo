using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdlsDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string endpoint = secretProvider.GetSecretAsync(AzureCosmosDB.KeyVaultName, "CosmosDBEndPoint").Result;
            string key = secretProvider.GetSecretAsync(AzureCosmosDB.KeyVaultName, "CosmosDBAuthKey").Result;
        }
    }
}
