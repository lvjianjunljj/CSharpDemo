using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        public async Task AsyncFunction(int index)
        {
            // I think this is a good funciton for test
            await Task.Delay(TimeSpan.FromSeconds(1));
            Console.WriteLine(index);
        }
    }
}
