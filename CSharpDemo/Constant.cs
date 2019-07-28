using AzureLib.KeyVault;
using System;

namespace CSharpDemo
{
    class Constant
    {
        public const string STORAGE_ACCOUNT_NAME = "csharpmvcwebapistorage";
        public const string SQL_SERVER_NAME = "csharpmvcwebapidatabaseserver";
        public const string SQL_DATABASE_NAME = "CSharpMVCWebAPIDatabase";
        private static Lazy<Constant> constantProvider = new Lazy<Constant>(() => new Constant());
        private Constant()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            this.SQLAccountUserId = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "SQLAccountUserId").Result;
            this.SQLAccountPassword = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "SQLAccountPassword").Result;
            this.StorageAccountKey = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "StorageAccountKey").Result;
        }
        public static Constant Instance => constantProvider.Value;

        /* We want to get the value from Azure Keyvault or other source in construction method, cannot be set to const
         * So we cannot use Constant.TEST to get the value like other const field(such as Constant.LOGGER_ACCOUNT_KEY)
         * Here we use single instance to save memory, for saving some const field, just using one instance is enough.
         * We can get the TEST value like this: Constant.Instance.TEST
         */
        public string SQLAccountUserId { get; }
        public string SQLAccountPassword { get; }
        public string StorageAccountKey { get; }
    }
}
