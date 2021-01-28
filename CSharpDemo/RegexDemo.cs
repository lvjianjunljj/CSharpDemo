namespace CSharpDemo
{
    using System;
    using System.Text.RegularExpressions;

    public class RegexDemo
    {
        public static void MainMethod()
        {
            //ZipFilePathRegexDemo();
            BuildPathRegexDemo();
        }

        private static void ZipFilePathRegexDemo()
        {
            Regex ZipFilePathRegex = new Regex(@"(?<zipFullPath>.+[\\/](?<zipFileName>[^/\\]+\.zip))[/\\](?<scriptRelativePath>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            Console.WriteLine(ZipFilePathRegex.Match(@"zipFullPath\zipFileName.zip\scriptRelativePath").Success);
        }

        private static void BuildPathRegexDemo()
        {
            string relativePath = @"local/Scheduled/Datasets/Public/Profiles/Subscriptions/Consumer/v3/2020/10/10/";
            string relativePathTemplate = @"local/Scheduled/Datasets/Public/Profiles/Subscriptions/Consumer/v3/%Y/%m/%d/SubscriptionEventHistory_%Y_%m_%d.ss";

            var relativePathRegex = BuildPathRegexWith(relativePathTemplate);
            Console.WriteLine(relativePathRegex);
            var match = relativePathRegex.Match(relativePath);
            Console.WriteLine(match.Success);
        }

        private static Regex BuildPathRegexWith(string pathTemplate)
        {
            string yearRegex = @"(?<year>((20)[1-2][0-9]))";
            string monthRegex = @"(?<month>(0[1-9]|1[012]))";
            string dayRegex = @"(?<day>(0[1-9]|[12][0-9]|3[01]))";
            string hourRegex = @"(?<hour>(2[0-3]|[01]?[0-9]))";
            string seriesRegex = @"([\d]+)";

            var relativePathRegex = new Regex(
                                        pathTemplate.Trim(new char[] { '/', ' ' }).
                                            Replace("%Y", yearRegex).
                                            Replace("%m", monthRegex).
                                            Replace("%d", dayRegex).
                                            Replace("%h", hourRegex).
                                            Replace("%n", seriesRegex),
                                        RegexOptions.Compiled);

            return relativePathRegex;
        }
    }
}
