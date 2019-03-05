using System;
using System.Collections.Generic;
using System.Threading;

namespace CSharpDemo
{
    class ThreadDemo
    {
        static void MainMethod()
        {

            List<Thread> threadList = new List<Thread>();
            for (int i = 0; i < 10; i++)
            {
                Thread thread = new Thread(Run);
                threadList.Add(thread);
            }
            for (int i = 0; i < threadList.Count; i++)
            {
                threadList[i].Start(i);
            }
            for (int i = 0; i < threadList.Count; i++)
            {
                threadList[i].Join();
            }
            string dateInfo = DateTime.Now.ToString("yyyy-MM-dd");
            Logger.OutputLogContent("logs", "TestLogs", dateInfo + ".log");
            Console.ReadKey();
        }
        static void Run(object i)
        {
            Thread.Sleep(1000);
            Console.WriteLine(i);

            Logger logger = new Logger("Test.class");
            logger.WriteLog(LogType.Error, i + "\nError Test................................................\n" + i);

        }
    }
}
