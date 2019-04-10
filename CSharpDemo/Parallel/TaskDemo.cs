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


        // The several Test1 functions is running in serial(They have the same Current Thread ID) 
        // and several Test2 functions is running in parallel(They have different Current Thread ID).
        public static void TestTaskFunctionWithoutAwait()
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
            await Task.Delay(TimeSpan.FromSeconds(1));
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
    }
}
