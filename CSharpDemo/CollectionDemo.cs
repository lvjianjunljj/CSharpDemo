namespace CSharpDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    class CollectionDemo
    {
        public static void MainMethod()
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("1", "1");
            // Cannot work, will throw exception
            //Console.WriteLine(dict["2"] ?? "2");

            // Check if the elements in a list are the sames as another one
            // This is a better way compared with foreach...
            List<int> a = new List<int>() { 1, 2, 3 };
            List<int> b = new List<int>() { 1, 2, 3 };
            Console.WriteLine(a.All(b.Contains) && b.All(a.Contains));
        }
    }
}
