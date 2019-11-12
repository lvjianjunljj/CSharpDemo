namespace CosmosDemo
{
    using System.Security.Cryptography;
    using System.Security.Cryptography.X509Certificates;

    class CertificateGenerator
    {
        private const string thumbprint = "7C3B9FAC23D24DB1313E7F985BB820FEF862A284";
        // <summary>
        /// Gets the certificate by thumbprint.
        /// </summary>
        /// <param name="thumbprint">The thumbprint.</param>
        /// <returns>X509Certificate2.</returns>
        /// <exception cref="CryptographicException"></exception>
        public static X509Certificate2 GetCertificateByThumbprint(StoreLocation storeLocation = StoreLocation.CurrentUser)
        {
            X509Store store = null;
            try
            {
                store = new X509Store(StoreName.My, storeLocation);
                store.Open(OpenFlags.ReadOnly);
                X509Certificate2Collection certificates = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false);
                if (certificates.Count > 0)
                {
                    return certificates[0];
                }
                else
                {
                    throw new CryptographicException(string.Format("Cannot find certificate from CurrentUser with thumbprint {0}", thumbprint));
                }
            }
            finally
            {
                if (store != null)
                {
                    store.Close();
                }
            }
        }
    }
}
