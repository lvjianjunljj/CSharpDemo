using System;
using System.Collections.Generic;
using System.Threading;

namespace CSharpDemo.Parallel
{
    // Summary: Task is better than thread for this scenarios
    class ThreadDemo
    {
        public static void MainMethod()
        {
            List<Thread> threadList1 = new List<Thread>();
            for (int i = 0; i < 10; i++)
            {
                Thread thread = new Thread(Run1);
                threadList1.Add(thread);
            }
            for (int i = 0; i < threadList1.Count; i++)
            {
                threadList1[i].Start(i);
            }
            for (int i = 0; i < threadList1.Count; i++)
            {
                threadList1[i].Join();
            }
            //string dateInfo = DateTime.Now.ToString("yyyy-MM-dd");
            //Logger.OutputLogContent("logs", "TestLogs", dateInfo + ".log");
            Console.WriteLine($@"index: {10}");

            List<Thread> threadList2 = new List<Thread>();
            ParameterizedThreadStart parameterizedThreadStartWithoutOutput = t =>
            {
                Run2((int)t);
            };
            for (int i = 0; i < 10; i++)
            {
                Thread thread = new Thread(parameterizedThreadStartWithoutOutput);
                // Actually we do not need to define the ParameterizedThreadStart and just write like this.
                //Thread thread = new Thread(t =>
                //{
                //    Run2((int)t);
                //});

                threadList2.Add(thread);
            }
            for (int i = 0; i < threadList2.Count; i++)
            {
                threadList2[i].Start(i);
            }
            for (int i = 0; i < threadList2.Count; i++)
            {
                threadList2[i].Join();
            }
            //string dateInfo = DateTime.Now.ToString("yyyy-MM-dd");
            //Logger.OutputLogContent("logs", "TestLogs", dateInfo + ".log");
            Console.WriteLine($@"index: {10}");


            List<Thread> threadWithReturnList = new List<Thread>();
            ParameterizedThreadStart parameterizedThreadStart = t =>
            {
                string Rvalue = FirstFun((int)t);
                CallBackFun();
                Console.WriteLine("Result{0}", Rvalue);
            };

            for (int i = 0; i < 10; i++)
            {
                Thread thread = new Thread(parameterizedThreadStart);
                threadWithReturnList.Add(thread);
            }
            for (int i = 0; i < threadWithReturnList.Count; i++)
            {
                threadWithReturnList[i].Start(i);
            }
            for (int i = 0; i < threadWithReturnList.Count; i++)
            {
                threadWithReturnList[i].Join();
            }
            Console.WriteLine($@"index: {10}");

        }

        // The type of parameter must be object
        static void Run1(object i)
        {
            //if ((int)i == 3)
            //{
            //    Thread.Sleep(1000);
            //    //int b = 1 / ((int)i - 3);
            //    throw new Exception("Test Exception!!!!");
            //}
            Thread.Sleep(1000);
            Console.WriteLine($@"index1: {i}");


            // this string append method(download, update and then upload) is not a good to be used as logger when multi-thread.
            //Logger logger = new Logger("Test.class");
            //logger.WriteLog(LogType.Error, i + "\nError Test................................................\n" + i);
        }

        static void Run2(int i)
        {
            Thread.Sleep(1000);
            Console.WriteLine($@"index2: {i}");
        }

        // Delegation function
        public static string FirstFun(int i)
        {
            Thread.Sleep(1000);
            Console.WriteLine("Delegation function, current therad ID{0}", Thread.CurrentThread.ManagedThreadId);
            return i.ToString();
        }
        // Callback function
        public static void CallBackFun()
        {
            Thread.Sleep(1000);
            Console.WriteLine("Callback function, current therad ID{0}", Thread.CurrentThread.ManagedThreadId);
        }

    }
}
