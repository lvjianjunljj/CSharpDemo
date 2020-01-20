namespace CSharpDemo
{
    using System;
    using System.Collections.Generic;

    class CloneDemo
    {
        public static void MainMethod()
        {
            ConleTestClassA a = new ConleTestClassA();
            a.ConleTestClassB = new ConleTestClassB
            {
                A = "BA"
            };
            a.Dict = new Dictionary<string, string>();
            a.Dict.Add("A", "A");

            var aa = a.GetClone();
            aa.ConleTestClassB.A = "AA";
            aa.Dict["A"] = "B";
            Console.WriteLine(a.ConleTestClassB.A);
            Console.WriteLine(a.Dict["A"]);

            Dictionary<string, string> dict = new Dictionary<string, string>();
            Console.WriteLine(dict.ContainsKey(""));

            // It will throw System.ArgumentNullException
            //  Message: Value cannot be null.
            //Console.WriteLine(dict.ContainsKey(null));
        }
        private class ConleTestClassA
        {
            public string StringA { get; set; }
            public string StringB { get; set; }
            public ConleTestClassB ConleTestClassB { get; set; }
            public Dictionary<string, string> Dict { get; set; }
            public ConleTestClassA GetClone()
            {
                return (ConleTestClassA)this.MemberwiseClone();
            }
        }

        private class ConleTestClassB
        {
            public string A { get; set; }
        }
    }
}
