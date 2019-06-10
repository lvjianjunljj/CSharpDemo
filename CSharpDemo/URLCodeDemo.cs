namespace CSharpDemo
{
    using System;

    class URLCodeDemo
    {
        static void MainMethod()
        {
            string codeString = "%5B1%2C2%5D";
            string decodeResult = System.Web.HttpUtility.UrlDecode(codeString);
            Console.WriteLine($"decodeResult: '{decodeResult}'");

            string decodeString = "[1,2]";
            string codeResult = System.Web.HttpUtility.UrlEncode(decodeString);
            Console.WriteLine($"codeResult: '{codeResult}'");
        }
    }
}
