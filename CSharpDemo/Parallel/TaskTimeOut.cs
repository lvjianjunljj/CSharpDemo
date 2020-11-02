namespace CSharpDemo.Parallel
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    class TaskTimeOut
    {
        public static void MainMethod()
        {
            //FailedDemo();
            //Successfulemo();
            //Test();
            //Test2();
            TestTryCatch();
        }

        static void Successfulemo()
        {
            var tokenSource2 = new CancellationTokenSource();
            CancellationToken ct = tokenSource2.Token;

            var task = Task.Run(() =>
            {
                // Were we already canceled?
                ct.ThrowIfCancellationRequested();

                bool moreToDo = true;
                while (moreToDo)
                {
                    // Poll on this property if you have to do
                    // other cleanup before throwing.
                    // This if check in not necessary.
                    if (ct.IsCancellationRequested)
                    {
                        // Clean up here, then...
                        ct.ThrowIfCancellationRequested();
                    }
                }
            }, tokenSource2.Token); // Pass same token to Task.Run.

            tokenSource2.Cancel();

            // Just continue on this thread, or await with try-catch:
            try
            {
                task.Wait();
            }
            catch (OperationCanceledException e)
            {
                Console.WriteLine($"{nameof(OperationCanceledException)} thrown with message: {e.Message}");
            }
            finally
            {
                tokenSource2.Dispose();
            }
        }

        static void Test()
        {
            int timeoutSeconds = 3;
            var tokenSource2 = new CancellationTokenSource();
            CancellationToken ct = tokenSource2.Token;

            string input = "input";
            var task = Task.Run(() => TestMethod(input), tokenSource2.Token);
            if (task.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
            {
                Console.WriteLine($"result: {task.Result}");
            }
            else
            {
                Console.WriteLine("Timed out");
                tokenSource2.Cancel();
            }
        }

        static void TestTryCatch()
        {
            int timeoutSeconds = 3;
            var tokenSource2 = new CancellationTokenSource();
            CancellationToken ct = tokenSource2.Token;

            string input = "input";
            var task = Task.Run(() => TestMethod(input), tokenSource2.Token);
            try
            {
                if (task.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
                {
                    Console.WriteLine($"result: {task.Result}");
                }
                else
                {
                    Console.WriteLine("Timed out");
                    tokenSource2.Cancel();
                    throw new Exception("TimeOut");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception message: {e.Message}");
            }
        }

        static void Test2()
        {
            int timeoutSeconds = 3;
            var tokenSource2 = new CancellationTokenSource();
            CancellationToken ct = tokenSource2.Token;

            // I don't think we can just kill the testMethod thread by Task.
            // We can just let it go, and we run the main thread after timeout period.
            var task = Task.Run(() =>
            {
                var a = TestMethod("input");
                // Were we already canceled?
                ct.ThrowIfCancellationRequested();

                int count = 0;
                while (true)
                {
                    Console.WriteLine($"count: {count++}");
                    // Poll on this property if you have to do
                    // other cleanup before throwing.
                    // Clean up here, then...
                    ct.ThrowIfCancellationRequested();
                }
            }, ct); // Pass same token to Task.Run.


            // Just continue on this thread, or await with try-catch:
            try
            {
                if (task.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
                {
                    Console.WriteLine($"Thread finished...");
                }
                else
                {
                    tokenSource2.Cancel();
                    Console.WriteLine("Timed out");
                }
            }
            catch (OperationCanceledException e)
            {
                Console.WriteLine($"{nameof(OperationCanceledException)} thrown with message: {e.Message}");
            }
            finally
            {
                tokenSource2.Dispose();
            }
        }

        static void FailedDemo()
        {
            int timeoutSeconds = 3;
            string input = "input";
            var task = Task.Run(() => TestMethod(input));
            if (task.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
            {
                Console.WriteLine($"result: {task.Result}");
            }
            else
            {
                Console.WriteLine("Timed out");
                // We cannot kill the task, it still will run.
            }
        }

        /// <summary>
        /// Outout(Ont what we want):
        /// Timed out
        /// TestMethod Finished...
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        static string TestMethod(string input)
        {
            Console.WriteLine("TestMethod Start...");
            Thread.Sleep(6000);
            Console.WriteLine("TestMethod Finished...");
            return input + "_output";
        }
    }
}
