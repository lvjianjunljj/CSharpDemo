namespace CSharpDemo.LINQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ToIEnumerableDemo
    {
        public static void MainMethod()
        {
            ToDictionaryDemo();
        }

        private static void ToDictionaryDemo()
        {
            IList<string> list = new List<string>();
            list.Add("11");
            list.Add("22");
            list.Add("33");
            var dict = list.ToDictionary(l => l.Substring(0, 1));
            foreach (var item in dict)
            {
                Console.WriteLine($"{item.Key}\t{item.Value}");
            }

            Console.WriteLine(dict.TryGetValue("12", out string ss));
            Console.WriteLine(ss);
        }
    }
}
