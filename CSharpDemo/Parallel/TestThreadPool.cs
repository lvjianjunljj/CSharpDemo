using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace CSharpDemo.Parallel
{
    class TestThreadPool
    {
        public static void SampleMethod()
        {
            ThreadPool.SetMaxThreads(5, 5);
            ThreadPool.SetMinThreads(5, 5);
            //ThreadPool.GetMinThreads(out int max1,out int max2);
            Thread.Sleep(3000);
            //Console.WriteLine(max1 + "\t" + max2);
            for (int i = 0; i < 100; i++)
            {
                ThreadPool.QueueUserWorkItem(new WaitCallback(TaskMethod), new SomeState(i));
            }


        }
        public static void TaskMethod(Object state)
        {
            Console.WriteLine(DateTime.Now + "\t" + ((SomeState)state).Cookie);
            DateTime cur = DateTime.Now;
            Thread.Sleep(500);
            DateTime ccur = DateTime.Now;
            int cost = (ccur - cur).Milliseconds;
            //Console.WriteLine("task method!!! cost:\t" + cost + "\t" + ((SomeState)state).Cookie);
        }


        public static void MainMethod()
        {
            Console.WriteLine("Thread Pool Sample:");

            bool W2K = false;

            // 允许线程池中运行最多 10 个线程 
            int MaxCount = 10;

            // 新建 ManualResetEvent 对象并且初始化为无信号状态 
            ManualResetEvent eventX = new ManualResetEvent(false);

            Console.WriteLine("Queuing {0} items to Thread Pool", MaxCount);

            // 注意初始化 oAlpha 对象的 eventX 属性 
            Alpha oAlpha = new Alpha(MaxCount);
            oAlpha.eventX = eventX;
            Console.WriteLine("Queue to Thread Pool 0");
            try
            {
                // 将工作项装入线程池 
                // 这里要用到 Windows 2000 以上版本才有的 API，所以可能出现 NotSupp ortException 异常 
                ThreadPool.QueueUserWorkItem(new WaitCallback(oAlpha.Beta), new SomeState(0));
                W2K = true;
            }
            catch (NotSupportedException)
            {
                Console.WriteLine("These API's may fail when called on a non-Wind ows 2000 system.");
                W2K = false;
            }
            if (W2K) // 如果当前系统支持 ThreadPool 的方法. 
            {
                for (int iItem = 1; iItem < MaxCount; iItem++)
                {
                    // 插入队列元素 
                    Console.WriteLine("Queue to Thread Pool {0}", iItem);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(oAlpha.Beta), new SomeState(iItem));
                }
                Console.WriteLine("Waiting for Thread Pool to drain");

                // 等待事件的完成，即线程调用 ManualResetEvent.Set() 方法 
                eventX.WaitOne(Timeout.Infinite, true);

                // WaitOne() 方法使调用它的线程等待直到 eventX.Set() 方法被调用 
                Console.WriteLine("Thread Pool has been drained (Event fired)");
                Console.WriteLine();
                Console.WriteLine("Load across threads");
                foreach (object o in oAlpha.HashCount.Keys)
                {
                    Console.WriteLine("{0} {1}", o, oAlpha.HashCount[o]);
                }
            }
            Console.ReadLine();
        }
    }


    public class SomeState
    {
        public int Cookie;
        public SomeState(int iCookie)
        {
            Cookie = iCookie;
        }
    }

    public class Alpha
    {
        public Hashtable HashCount;
        public ManualResetEvent eventX;
        public static int iCount = 0;
        public static int iMaxCount = 0;
        public Alpha(int MaxCount)
        {
            HashCount = new Hashtable(MaxCount);
            iMaxCount = MaxCount;
        }

        /// <summary>
        /// 线程池里的线程将调用 Beta()方法
        /// </summary>
        /// <param name="state"></param> 
        public void Beta(Object state)
        {
            // 输出当前线程的 hash 编码值和 Cookie 的值 
            Console.WriteLine(" {0} {1} :", Thread.CurrentThread.GetHashCode(), ((SomeState)state).Cookie);
            Console.WriteLine("HashCount.Count=={0}, Thread.CurrentThread.GetHash Code()=={1}", HashCount.Count,
                Thread.CurrentThread.GetHashCode());
            lock (HashCount)
            {
                // 如果当前的 Hash 表中没有当前线程的 Hash 值，则添加之 
                if (!HashCount.ContainsKey(Thread.CurrentThread.GetHashCode()))
                    HashCount.Add(Thread.CurrentThread.GetHashCode(), 0);
                HashCount[Thread.CurrentThread.GetHashCode()] = ((int)HashCount[Thread.CurrentThread.GetHashCode()]) + 1;
            }

            Thread.Sleep(2000);
            // Interlocked.Increment() 操作是一个原子操作，具体请看下面说明 
            Interlocked.Increment(ref iCount);
            if (iCount == iMaxCount)
            {
                Console.WriteLine();
                Console.WriteLine("Setting eventX ");
                eventX.Set();
            }
        }
    }
}
