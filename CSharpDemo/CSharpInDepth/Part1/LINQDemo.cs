using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.CSharpInDepth.Part1
{
    /*
     * Doc link: https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/index
     * 
     * have never heard of LINQ before reading this book.
     * I feel that this kind of expansion is both amazing and nondescript.
     * 
     */
    class LINQDemo
    {
        public static void MainMethod()
        {
            Console.WriteLine("LINQDemo");

            string[] strs = new[] { "AAA", "BBB", "CCC" };

            IEnumerable<string> stringQuery =
            from str in strs
            where str.StartsWith("A") || str.EndsWith("C")
            orderby str
            select str.ToUpper();
            foreach (string str in stringQuery)
            {
                Console.WriteLine(str);
            }
        }
    }
}
