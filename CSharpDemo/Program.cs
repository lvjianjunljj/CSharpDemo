using System;
using Microsoft.WindowsAzure.Storage; // Namespace for Storage Client Library
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Azure Blobs
using Microsoft.WindowsAzure.Storage.File; // Namespace for Azure Files

namespace CSharpDemo
{

    class Program
    {
        static void Main(string[] args)
        {
            StorageAccountDemo.MainMethod();



            Console.ReadKey();
        }
    }
}
