/*
 * class AutoResetEvent is to make several methods of multi-threaded execution can be executed sequentially
 * 
 * This demo is based on the solution of LeetCode 1114. Print in Order
 * link: https://leetcode.com/problems/print-in-order/
 * 
 * Suppose we have a class:

public class Foo {
  public void first() { print("first"); }
  public void second() { print("second"); }
  public void third() { print("third"); }
}
The same instance of Foo will be passed to three different threads. Thread A will call first(), thread B will call second(), and thread C will call third(). Design a mechanism and modify the program to ensure that second() is executed after first(), and third() is executed after second().

 

Example 1:

Input: [1,2,3]
Output: "firstsecondthird"
Explanation: There are three threads being fired asynchronously. The input [1,2,3] means thread A calls first(), thread B calls second(), and thread C calls third(). "firstsecondthird" is the correct output.
Example 2:

Input: [1,3,2]
Output: "firstsecondthird"
Explanation: The input [1,3,2] means thread A calls first(), thread B calls third(), and thread C calls second(). "firstsecondthird" is the correct output.
 

Note:

We do not know how the threads will be scheduled in the operating system, even though the numbers in the input seems to imply the ordering. The input format you see is mainly to ensure our tests' comprehensiveness.
 * 
 * 
 * class Foo is the solution class.
 */
namespace CSharpDemo.Parallel
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class AutoResetEventDemo
    {
        public static void MainMethod()
        {
            Foo foo = new Foo();
            Action printSecond = new Action(() => Console.WriteLine("Second!!!"));
            Action printThird = new Action(() => Console.WriteLine("Third!!!"));
            Action printFirst = new Action(() => Console.WriteLine("First!!!"));

            // We need run these three function asynchronously
            //Thread[] threads = new Thread[3];
            //threads[0] = new Thread(() => foo.Second(printSecond));
            //threads[1] = new Thread(() => foo.Third(printThird));
            //threads[2] = new Thread(() => foo.First(printFirst));

            //threads[0].Start();
            //threads[1].Start();
            //threads[2].Start();

            //threads[0].Join();
            //threads[1].Join();
            //threads[2].Join();
            //Console.WriteLine("End!!!");

            // Both of using Thread and using Task are OK.
            Task[] tasks = new Task[3];
            tasks[0] = new Task(() => foo.Second(printSecond));
            tasks[1] = new Task(() => foo.Third(printThird));
            tasks[2] = new Task(() => foo.First(printFirst));

            tasks[0].Start();
            tasks[1].Start();
            tasks[2].Start();

            tasks[0].Wait();
            tasks[1].Wait();
            tasks[2].Wait();

            Console.WriteLine("End!!!");


        }
    }

    class Foo
    {
        EventWaitHandle evnt1 = null;
        EventWaitHandle evnt2 = null;
        public Foo()
        {
            evnt1 = new AutoResetEvent(false);
            evnt2 = new AutoResetEvent(false);
        }

        public void First(Action printFirst)
        {

            // printFirst() outputs "first". Do not change or remove this line.
            printFirst();
            evnt1.Set();
        }

        public void Second(Action printSecond)
        {

            evnt1.WaitOne();
            // printSecond() outputs "second". Do not change or remove this line.
            printSecond();
            evnt2.Set();
        }

        public void Third(Action printThird)
        {
            evnt2.WaitOne();
            // printThird() outputs "third". Do not change or remove this line.
            printThird();
            evnt2.Set();
        }
    }
}
