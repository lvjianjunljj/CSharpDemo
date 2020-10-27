namespace AdlsDemo
{
    using System;
    using System.Diagnostics;

    class Program
    {
        static void Main(string[] args)
        {
            AdlsDemoClass.MainMethod();

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
