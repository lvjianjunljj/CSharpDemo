namespace CSharpDemo.IDEAs
{
    using CSharpDemo.FileOperation;
    using Newtonsoft.Json.Linq;
    using System;

    class DatasetJsonFileOperation
    {
        private static string FolderPath = @"D:\IDEAs\Ibiza\Source\DataCopMonitors\PROD";


        public static void MainMethod()
        {
            UpdateDatasetJsonForMergingADLSCosmos();
        }

        public static void UpdateDatasetJsonForMergingADLSCosmos()
        {
            var folders = ReadFile.GetFolderSubPaths(FolderPath, ReadType.Directory, PathType.Absolute);
            foreach (var folder in folders)
            {
                System.Console.WriteLine(folder);
                string subFolderPath = folder + @"\Datasets";
                try
                {
                    var datasetJsonFiles = ReadFile.GetFolderSubPaths(subFolderPath, ReadType.File, PathType.Absolute);
                    foreach (var filePath in datasetJsonFiles)
                    {
                        //System.Console.WriteLine(filePath);
                        string datasetString = ReadFile.ThirdMethod(filePath);
                        JObject datasetJObject = JObject.Parse(datasetString);
                        //Console.WriteLine(datasetJObject["dataFabric"]);
                        if (datasetJObject["dataFabric"].ToString() == "ADLS")
                        {
                            // Do something
                            // I don't write the code because there is not ADLS dataset in the folder
                        }
                    }

                }
                catch (Exception e)
                {
                    System.Console.WriteLine(e.Message);
                }
            }
        }
    }
}
