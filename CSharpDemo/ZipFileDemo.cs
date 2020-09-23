namespace CSharpDemo
{
    using System.IO.Compression;

    public class ZipFileDemo
    {
        public static void MainMethod()
        {
            string zipPath = @"C:\Users\jianjlv\AppData\Local\Temp\dc62baf8-91b7-45c9-8d39-3bc0b4490c9f/Pipeline.zip";
            zipPath = @"D:\ttttt.zip";
            zipPath = @"D:\Pipeline.zip";
            string outputDirectory = @"D:\test";
            ZipFile.ExtractToDirectory(zipPath, outputDirectory);
        }
    }
}
