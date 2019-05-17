namespace CSharpDemo
{
    using System;
    class IDisposableImplementDemo
    {
        public static void MainMethod()
        {
            using (SqlHelper s = new SqlHelper())
            {
                s.Process();
            }
            //  In the ending of using(...), main thread will run the Dispose function of the class
        }
    }

    // The implementation class of interface IDisposable
    class SqlHelper : IDisposable
    {
        public void Process()
        {
            Console.WriteLine("run function Process()");
        }
        public void Dispose()
        {
            Console.WriteLine("run function Dispose()");
        }
    }
}
