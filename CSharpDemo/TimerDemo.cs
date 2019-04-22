using System;
using System.Threading;

namespace CSharpDemo
{
    class TimerDemo
    {
        public static void MainMethod()
        {
            Console.WriteLine(DateTime.Now);
            TimerDemo t = new TimerDemo();
            Timer serviceHeartbeatTimer = new Timer(t.Test);
            // The two input is the duration between start time and now, the period.
            serviceHeartbeatTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }
        public void Test(object state)
        {
            Console.WriteLine(1);
            Console.WriteLine(state);
            Console.WriteLine(DateTime.Now);
        }
    }
}
