namespace CSharpDemo
{
    using System;

    class WriteLineDemo
    {
        public static void MainMethod()
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = ConsoleColor.Blue;
            Console.WriteLine("WriteLine Demo...");
            Console.ResetColor();
            Console.WriteLine("WriteLine Demo...");
        }
    }
}
