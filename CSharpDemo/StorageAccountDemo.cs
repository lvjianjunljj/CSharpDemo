using System;
using Microsoft.WindowsAzure.Storage; // Namespace for Storage Client Library
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Azure Blobs
using Microsoft.WindowsAzure.Storage.File; // Namespace for Azure Files

namespace CSharpDemo
{
    class StorageAccountDemo
    {
        /*
         * GetAppendBlobReference is removed from Microsoft.WindowsAzure.Storage.Blob.CloudBlobContainer
         * So we cannot release the log function based on the append function from Microsoft.WindowsAzure.Storage.
         * stackoverflow: https://stackoverflow.com/questions/48411359/getappendblobreference-is-removed-from-microsoft-windowsazure-storage-blob-cloud
         */
        public static void MainMethod()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                @"DefaultEndpointsProtocol=https;AccountName=csharpmvcwebapistorage;AccountKey=CP5Ss5prdGOsnKB8yljQWOuydXLxXaRcK+ibb4gxpWGiJjvxhlVEF5quK3XJNhesefliBjWpLke5z5ofafc3QA==");
            // Create a CloudFileClient object for credentialed access to Azure Files.
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();

            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference("logs");

            // If the share does not exist, create it.
            if (!share.Exists())
            {
                share.Create();
            }
            // Get a reference to the root directory for the share.
            CloudFileDirectory rootDir = share.GetRootDirectoryReference();

            // Get a reference to the directory we created previously.
            CloudFileDirectory sampleDir = rootDir.GetDirectoryReference("TestLogs");
            if (!sampleDir.Exists())
            {
                sampleDir.Create();
            }

            // Get a reference to the file we created previously.
            CloudFile file = sampleDir.GetFileReference("Log1.txt");

            // Ensure that the file exists.
            if (file.Exists())
            {
                // Write the contents of the file to the console window.
                Console.WriteLine(file.DownloadTextAsync().Result);
                file.UploadText("12345");
                Console.WriteLine(file.DownloadTextAsync().Result);
            }
            else
            {
                file.UploadText("1234");
            }

            Console.ReadKey();
        }
    }
}
