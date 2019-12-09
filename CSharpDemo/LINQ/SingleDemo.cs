namespace CSharpDemo.LINQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class SingleDemo
    {

        public static void MainMethod()
        {
            SingleExceptionDemo();
        }

        static void SingleExceptionDemo()
        {
            IList<TestClassForLINQ> tcs = new List<TestClassForLINQ>();
            tcs.Add(new TestClassForLINQ()
            {
                StringA = "a"
            });
            tcs.Add(new TestClassForLINQ()
            {
                StringA = "a"
            });
            tcs.Add(new TestClassForLINQ()
            {
                StringA = "b"
            });

            try
            {

                Console.WriteLine(tcs.Single(t => t.StringA == "a"));
            }
            catch (InvalidOperationException e)
            {
                Console.WriteLine("Using single throw exception when there is multi results");
                Console.WriteLine(e.Message);
            }

            Console.WriteLine(tcs.Single(t => t.StringA == "b"));

            try
            {
                Console.WriteLine(tcs.Single(t => t.StringA == "c"));
            }
            catch (InvalidOperationException e)
            {
                Console.WriteLine("Using single throw exception when there is no result");
                Console.WriteLine(e.Message);
            }
        }
    }
}
