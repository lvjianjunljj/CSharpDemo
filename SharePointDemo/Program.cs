namespace SharePointDemo
{
    using Microsoft.SharePoint;
    using System;
    using System.IO;

    class Program
    {
        /// <summary>
        /// Error message:
        /// An unhandled exception of type 'System.IO.FileNotFoundException' occurred in mscorlib.dll
        /// Additional information: Could not load file or assembly 'Microsoft.SharePoint.Library, 
        /// Version=15.0.0.0, Culture=neutral, PublicKeyToken=71e9bce111e9429c' 
        /// or one of its dependencies.The system cannot find the file specified.
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            Console.WriteLine(12345);
            String fileToUpload = @"D:\test.txt";
            String sharePointSite = "http://yoursite.com/sites/Research/";
            sharePointSite = @"https://microsoft.sharepoint.com/teams/Office365CoreSharedDataEngineeringManagers/";
            String documentLibraryName = @"Data Build\StreamAvailabilityCheck\test.txt";

            using (SPSite oSite = new SPSite(sharePointSite))
            {
                using (SPWeb oWeb = oSite.OpenWeb())
                {
                    if (!File.Exists(fileToUpload))
                        throw new FileNotFoundException("File not found.", fileToUpload);

                    SPFolder myLibrary = oWeb.Folders[documentLibraryName];

                    // Prepare to upload
                    Boolean replaceExistingFiles = true;
                    String fileName = System.IO.Path.GetFileName(fileToUpload);
                    FileStream fileStream = File.OpenRead(fileToUpload);

                    // Upload document
                    SPFile spfile = myLibrary.Files.Add(fileName, fileStream, replaceExistingFiles);

                    // Commit 
                    myLibrary.Update();
                }
            }
        }
    }
}
