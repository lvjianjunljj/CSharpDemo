namespace CSharpDemo.ReflectionDemo
{
    using System;
    using System.Threading.Tasks;

    class ActionDemo
    {
        public static void MainMethod()
        {
            ActionDemo actionDemo = new ActionDemo();
            Action syncAction = actionDemo.TestSyncMethod;
            Console.WriteLine("Start TestSyncMethod...");
            syncAction.Invoke();
            Console.WriteLine("Start TestSyncMethod...");


            Task asyncTask = actionDemo.TestAsyncMethod();
            Console.WriteLine("Start TestAsyncMethod...");
            asyncTask.Wait();
            Console.WriteLine("Start TestAsyncMethod...");
        }

        public async Task TestAsyncMethod()
        {
            await Task.Delay(1);
            Console.WriteLine("Run async test method");
        }

        public void TestSyncMethod()
        {
            System.Console.WriteLine("Run snc test method");
        }
    }
}
