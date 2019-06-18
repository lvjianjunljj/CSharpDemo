using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class DestructorDemo
    {
        /*
         * When we run the build result CSharpDemo.exe in CMD, we can see the result:
            
            Constructors function
            Main method: A.a1234
            Dispose function 1234
            Constructors function
            Test method: A.a4321
            Dispose function 4321
            Finalize 4321
            Finalize 1234
         
         *  For this, now I don't know how to call the destructor function before the meain thread ended.
         */

        public static void MainMethod()
        {
            DestructorDemo destructorDemo = new DestructorDemo();
            using (A a = new A())
            {
                a.a = "1234";
                Console.WriteLine($"Main method: A.a{a.a}");
            }
            destructorDemo.Test();
        }
        public void Test()
        {
            using (A a = new A())
            {
                a.a = "4321";
                Console.WriteLine($"Test method: A.a{a.a}");
            }
        }
    }
    class A : IDisposable
    {
        public string a;

        public A()
        {
            Console.WriteLine("Constructors function " + a);
        }
        ~A()
        {
            // We cant see this line when we debug in VS, but we can see this line when we run the build result CSharpDemo.exe in CMD
            Console.WriteLine("Finalize " + a);
        }

        public void Dispose()
        {
            Console.WriteLine("Dispose function " + a);
        }
    }
}
