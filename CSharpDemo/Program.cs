using System;
using System.Collections.Generic;
using System.ComponentModel;
using CSharpDemo.Application;
using CSharpDemo.Azure;
using CSharpDemo.IcMTest;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //IcMKustoDemo.MainMethod();
            //AzureCosmosDB.MainMethod();
            QueryIncidents.MainMethod();

            //ReferenceDemoAux referenceDemoAux = new ReferenceDemoAux();
            //referenceDemoAux.TestReferenceError();

            //ConvertDemo.MainMethod();


            //AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            //KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            //string vaultUri = @"https://csharpmvcwebapikeyvault.vault.azure.net/";
            //SecretBundle secret = keyVaultClient.GetSecretAsync(vaultUri, "AppSecret").Result;

            //Console.WriteLine(secret.Value);
            Console.ReadKey();
        }
    }
}

