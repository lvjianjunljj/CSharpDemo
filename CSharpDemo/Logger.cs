using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage; // Namespace for Storage Client Library
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Azure Blobs
using Microsoft.WindowsAzure.Storage.File; // Namespace for Azure Files

namespace CSharpDemo
{
    public enum LogType
    {
        Error,
        Warning,
        Info
    }

    class Logger
    {

        private enum WriteWay
        {
            Cover,
            Append
        }
        private string className;
        public Logger(string className)
        {
            this.className = className;
        }
        //Logger logger = new Logger("Test.class");
        //logger.WriteLog(LogType.Error, "Error Test");
        public void WriteLog(LogType logType, string logContent)
        {
            string dateInfo = DateTime.Now.ToString("yyyy-MM-dd");
            string writeLogLine = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "[" + logType + "]" +
                this.className + ": " + logContent;
            WriteLogLine(WriteWay.Append, writeLogLine, "logs", "TestLogs", dateInfo + ".log");
        }
        static string invalidExistLogFilePath = "[Error] log file path input is invalid";
        private void WriteLogLine(WriteWay writeWay, string writeLogLine, params string[] logFilePath)
        {
            if (logFilePath.Length < 2)
            {
                Console.WriteLine(invalidExistLogFilePath);
                return;
            }

            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={Constant.STORAGE_ACCOUNT_NAME};AccountKey={Constant.Instance.StorageAccountKey};EndpointSuffix=core.windows.net";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();

            CloudFileShare share = fileClient.GetShareReference(logFilePath[0]);

            if (!share.Exists())
            {
                share.Create();
            }
            CloudFileDirectory sampleDir = share.GetRootDirectoryReference();

            for (int i = 1; i < logFilePath.Length - 1; i++)
            {
                CloudFileDirectory nextLevelDir = sampleDir.GetDirectoryReference("TestLogs");
                if (!sampleDir.Exists())
                {
                    sampleDir.Create();
                }
                sampleDir = nextLevelDir;
            }

            CloudFile file = sampleDir.GetFileReference(logFilePath[logFilePath.Length - 1]);

            lock ("")
            {
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer blobContainer = blobClient.GetContainerReference("logs");
                blobContainer.CreateIfNotExistsAsync();
                CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference("testBlob");

                List<string> blockIds = new List<string>();
                DateTime before = DateTime.Now;

                blockIds.AddRange(blockBlob.DownloadBlockList(BlockListingFilter.Committed).Select(b => b.Name));
                DateTime after = DateTime.Now;
                TimeSpan ts = after.Subtract(before);
                Console.WriteLine(ts.Seconds + "_" + ts.Milliseconds);

                var newId = Convert.ToBase64String(Encoding.Default.GetBytes(blockIds.Count.ToString()));
                blockBlob.PutBlock(newId, new MemoryStream(Encoding.Default.GetBytes(writeLogLine + "\n")), null);
                blockIds.Add(newId);
                blockBlob.PutBlockList(blockIds);

                string writenLineContent = "";
                if (file.Exists())
                {
                    if (writeWay == WriteWay.Cover)
                    {
                    }
                    else if (writeWay == WriteWay.Append)
                    {
                        writenLineContent = file.DownloadTextAsync().Result;
                    }
                }
                file.UploadText(writenLineContent + writeLogLine + "\n");
            }
        }

        // Logger.OutputLogContent("logs", "TestLogs", "2019-02-28.log");
        public static void OutputLogContent(params string[] logFilePath)
        {
            if (logFilePath.Length < 2)
            {
                Console.WriteLine(invalidExistLogFilePath);
                return;
            }
            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={Constant.STORAGE_ACCOUNT_NAME};AccountKey={Constant.Instance.StorageAccountKey};EndpointSuffix=core.windows.net";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();

            CloudFileShare share = fileClient.GetShareReference(logFilePath[0]);

            if (!share.Exists())
            {
                Console.WriteLine(invalidExistLogFilePath);
                return;
            }
            CloudFileDirectory sampleDir = share.GetRootDirectoryReference();

            for (int i = 1; i < logFilePath.Length - 1; i++)
            {
                CloudFileDirectory nextLevelDir = sampleDir.GetDirectoryReference(logFilePath[i]);
                if (!sampleDir.Exists())
                {
                    Console.WriteLine(invalidExistLogFilePath);
                    return;
                }
                sampleDir = nextLevelDir;
            }

            CloudFile file = sampleDir.GetFileReference(logFilePath[logFilePath.Length - 1]);

            if (file.Exists())
            {
                Console.WriteLine(file.DownloadTextAsync().Result);
            }
            else
            {
                Console.WriteLine();
            }
        }
    }
}
