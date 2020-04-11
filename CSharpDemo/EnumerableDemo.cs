namespace CSharpDemo
{
    using System;
    using System.Collections.Generic;

    public class EnumerableDemo
    {
        public static void MainMethod()
        {
            //TryGetValueDemo();
            //UnionWithDemo();
        }

        private static void TryGetValueDemo()
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            try
            {
                Console.WriteLine(dict["1"]);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine(dict.TryGetValue("1", out string ss));
            Console.WriteLine(ss);
        }

        private static void UnionWithDemo()
        {
            HashSet<string> has = new HashSet<string>(new string[] { "1", "3" });
            has.UnionWith(new HashSet<string>(new string[] { "1", "2" }));

            foreach (var item in has)
            {
                Console.WriteLine(item);
            }
        }
    }
}
