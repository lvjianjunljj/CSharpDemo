using System;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class TimerDemo
    {
        public static void MainMethod()
        {
            //TestSyncMethod();
            TestAsyncMethod();
        }

        public static void TestSyncMethod()
        {
            Console.WriteLine($"Sync Method Test. Time: {DateTime.Now}");
            TimerDemo t = new TimerDemo();
            Timer serviceHeartbeatTimer = new Timer(t.TestSync);
            // The two input is the duration between start time and now, the period.
            serviceHeartbeatTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        public static void TestAsyncMethod()
        {
            Console.WriteLine($"Async Method Test. Time: {DateTime.Now}");
            TimerDemo t = new TimerDemo();
            Timer serviceHeartbeatTimer = new Timer(t.TestAsync);
            // The two input is the duration between start time and now, the period.
            serviceHeartbeatTimer.Change(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
        }

        public void TestSync(object state)
        {
            Console.WriteLine($"Start Test. Time: {DateTime.Now}");
            Thread.Sleep(3000);
            Console.WriteLine($"End Test. Time: {DateTime.Now}");

            // We can stop the time in the call function with state.
            ((Timer)state).Dispose();
            Console.WriteLine("Stop the time...");
        }

        public async void TestAsync(object state)
        {
            Console.WriteLine($"Start TestAsync. Time: {DateTime.Now}");
            await Task.Delay(TimeSpan.FromSeconds(3));
            Console.WriteLine($"End TestAsync. Time: {DateTime.Now}");

            // We can stop the time in the call function with state.
            ((Timer)state).Dispose();
            Console.WriteLine("Stop the TestAsync...");
        }
    }
}
