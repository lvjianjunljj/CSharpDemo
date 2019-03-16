using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo.Parallel
{
    class TaskVsThread
    {
        // 1.Task is suitable for multi-processor and i-series multi-processor.
        // 2.Thread is suitable for all processors with higher real-time performance.
        // But for current scenarios
        // Note: Debug.WriteLine is useful function to reloace output in CMD for showing debug information.
        public static void MainMethod()
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            Thread threadTest1 = new Thread(() =>
            {
                Thread.Sleep(2000);
                Debug.WriteLine("Thread 1 ends cost time: {0}", watch.ElapsedMilliseconds);
            });
            threadTest1.Start();
            Thread threadTest2 = new Thread(() =>
            {
                Thread.Sleep(2000);
                Debug.WriteLine("Thread 2 ends cost time: {0}", watch.ElapsedMilliseconds);
            });
            threadTest2.Start();
            Thread threadTest3 = new Thread(() =>
            {
                Thread.Sleep(2900);
                Debug.WriteLine("Thread 3 ends cost time: {0}", watch.ElapsedMilliseconds);
            });
            threadTest3.Start();


            //var Task1 = Task.Factory.StartNew(() =>
            //           {
            //               Thread.Sleep(2500);
            //               Console.WriteLine("Thread 1 ends cost time: {0}", watch.ElapsedMilliseconds);
            //           });

            //var Task2 = Task.Factory.StartNew(() =>
            //{
            //    Thread.Sleep(2700);
            //    Console.WriteLine("Thread 2 ends cost time: {0}", watch.ElapsedMilliseconds);
            //});

            //var Task3 = Task.Factory.StartNew(() =>
            //{
            //    Thread.Sleep(2900);
            //    Console.WriteLine("Thread 3 ends cost time: {0}", watch.ElapsedMilliseconds);
            //});

            //while (watch.ElapsedMilliseconds <= 3000)
            //{
            //    //if (!threadTest.IsAlive && !threadTest1.IsAlive)
            //    if (Task1.IsCompleted && Task2.IsCompleted && Task3.IsCompleted)
            //    {
            //        Console.WriteLine("Monitoring end cost time:{0}", watch.ElapsedMilliseconds);
            //        break;
            //    }
            //    else
            //        Thread.Sleep(1);
            //}
        }
    }
}
