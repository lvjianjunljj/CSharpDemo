namespace CSharpDemo
{
    using System;
    using System.Text.RegularExpressions;

    public class RegexDemo
    {
        public static void MainMethod()
        {
            Regex ZipFilePathRegex = new Regex(@"(?<zipFullPath>.+[\\/](?<zipFileName>[^/\\]+\.zip))[/\\](?<scriptRelativePath>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            Console.WriteLine(ZipFilePathRegex.Match(@"zipFullPath\zipFileName.zip\scriptRelativePath").Success);
        }
    }
}
