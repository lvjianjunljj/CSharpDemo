using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo.Parallel
{
    class TaskDemo
    {
        public static void MainMethod()
        {
            //TestTaskFunctionWithoutAwait1();
            NewTaskDemo();
        }

        public static void TestTaskFunctionWithoutAwait1()
        {
            TaskDemo taskDemo = new TaskDemo();
            Task[] tasks = new Task[10];
            for (int i = 0; i < 10; i++)
            {
                tasks[i] = taskDemo.AsyncFunction(i);
            }
            for (int i = 0; i < 10; i++)
            {
                // If in a async function, you can use this:
                //await tasks[i];
                tasks[i].Wait();
            }
            Console.WriteLine(10);
        }

        public static void NewTaskDemo()
        {
            TaskDemo instance = new TaskDemo();
            Task task = instance.NewTaskAsyncFunction();
            Console.WriteLine("Main therad start...");
            task.Wait(); // Start may not be called on a promise-style task.
            Console.WriteLine("Main therad end...");
            /*Output:
             * Main therad start...
             * Task start...
             * Task end...
             * Main therad end...
             */

            Console.WriteLine("====================");

            Task task2 = new Task(async () =>
            {
                Console.WriteLine("Task start...");
                await Task.Delay(TimeSpan.FromSeconds(2));
                Console.WriteLine("Task end...");
            });

            Console.WriteLine("Main therad start...");
            // Make sure why it does not work like we think.
            // We can see the difference through running the build result in CMD.
            task2.Start();
            task2.Wait();
            Console.WriteLine("Main therad end...");

            /*Output:
             * Main therad start...
             * Task start...
             * Main therad end...
             * Task end...
             */
            Console.WriteLine("====================");

            // This is what we want
            Task task3 = Task.Run(async () =>
            {
                Console.WriteLine("Task start...");
                await Task.Delay(TimeSpan.FromSeconds(2));
                Console.WriteLine("Task end...");
            });

            Console.WriteLine("Main therad start...");
            // Exception: 
            // System.InvalidOperationException
            // Message=Start may not be called on a promise-style task.
            //task3.Start();

            task3.Wait();
            Console.WriteLine("Main therad end...");

            /*Output:
             * Main therad start...
             * Task start...
             * Task end...
             * Main therad end...
             */
            Console.WriteLine("====================");

            // This is what we want.
            Console.WriteLine("Main therad start...");
            Func<Task> task4 = async () =>
            {
                Console.WriteLine("Task start...");
                await Task.Delay(TimeSpan.FromSeconds(2));
                Console.WriteLine("Task end...");
            };
            task4().Wait();
            Console.WriteLine("Main therad end...");
            /*Output:
             * Main therad start...
             * Task start...
             * Task end...
             * Main therad end...
             */
        }

        // The several Test1 functions is running in serial(They have the same Current Thread ID) 
        // and several Test2 functions is running in parallel(They have different Current Thread ID).
        public static void TestTaskFunctionWithoutAwait2()
        {
            Task[] tasks = new Task[10];
            TaskDemo p = new TaskDemo();
            tasks[0] = p.Test1();
            tasks[1] = p.Test2();
            tasks[2] = p.Test1();
            tasks[3] = p.Test2();
            tasks[4] = p.Test1();
            tasks[5] = p.Test2();
            tasks[6] = p.Test1();
            tasks[7] = p.Test2();
            tasks[8] = p.Test1();
            tasks[9] = p.Test3();
            for (int i = 0; i < 10; i++)
            {
                tasks[i].Wait();
            }
        }
        public async Task AsyncFunction(int index)
        {
            // I think this is a good funciton for test
            await Task.Delay(TimeSpan.FromSeconds(4));
            Console.WriteLine(index);
        }


        // This async method lacks 'await' operations and will run synchronously. 
        // Consider using the 'await' operator to await non-blocking API calls, 
        // or 'await Task.Run(…)' to do CPU-bound work on a background thread.
        public async Task Test1()
        {
            Console.WriteLine(1111);
            Console.WriteLine(Thread.CurrentThread.ManagedThreadId.ToString());
        }
        public async Task Test2()
        {
            await Task.Delay(TimeSpan.FromSeconds(2));
            lock ("1234")
            {
                Console.WriteLine(2222);
                Console.WriteLine(Thread.CurrentThread.ManagedThreadId.ToString());
            }
        }
        public async Task Test3()
        {
            Console.WriteLine(3333);
            Console.WriteLine(Thread.CurrentThread.ManagedThreadId.ToString());
        }

        public async Task NewTaskAsyncFunction()
        {
            Console.WriteLine("Task start...");
            await Task.Delay(TimeSpan.FromSeconds(2));
            Console.WriteLine("Task end...");
        }
    }
}
