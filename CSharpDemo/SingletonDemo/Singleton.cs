using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.SingletonDemo
{
    public class SingleDemo
    {
        public static void MainMethond()
        {
            // The outputs in release and debug are different, so we can know that:
            // Static members are initialized at any time before the class is first used 
            // (determined by CLR intelligence)
            Console.WriteLine(111);
            StaticSingleton s = StaticSingleton.getInstance();
            Console.WriteLine(333);
        }
    }
    // using .NET 4's Lazy<T> type
    // I think it is the best method to implement singleton mode 
    public sealed class Singleton    {
        private static readonly Lazy<Singleton>
            lazy =
            new Lazy<Singleton>
                (() => new Singleton());
        public static Singleton Instance { get { return lazy.Value; } }
        private Singleton()
        {
        }
    }

    // not quite as lazy, but thread-safe without using locks
    public sealed class Singleton1    {
        private static readonly Singleton1 instance = new Singleton1();
        // Explicit static constructor to tell C# compiler
        // not to mark type as beforefieldinit

        // I don't know what's the fucntion of this static construction method
        static Singleton1()
        {
            Console.WriteLine(1234);
            Debug.WriteLine(12121212);

        }
        private Singleton1()
        {
            Console.WriteLine(1234);
            Debug.WriteLine(12121212);
        }
        public static Singleton1 Instance
        {
            get
            {
                return instance;
            }
        }    }

    // fully lazy instantiation
    public sealed class Singleton2    {
        private Singleton2()
        {
            Console.WriteLine(12341234);
        }
        public static Singleton2 Instance { get { return Nested.instance; } }
        private class Nested
        {
            // Explicit static constructor to tell C# compiler
            // not to mark type as beforefieldinit
            static Nested()
            {
                Console.WriteLine(12344);
            }
            internal static readonly Singleton2 instance = new Singleton2();
        }    }

    public class EagerSingleton
    {
        private static EagerSingleton instance = GetEagerSingleton();


        private EagerSingleton() { Console.WriteLine(123123123123); }
        private EagerSingleton(int i) { Console.WriteLine(11111); }
        private static EagerSingleton GetEagerSingleton()
        {
            Console.WriteLine("static get EagerSingleton function");
            return new EagerSingleton();
        }

        public static EagerSingleton GetInstance()
        {
            return instance;
        }
    }

    /// <summary>
    /// 静态内部类单例模式，线程安全
    /// </summary>
    public class StaticSingleton
    {
        private class InnerInstance
        {
            /// <summary>
            /// 当一个类有静态构造函数时，它的静态成员变量不会被beforefieldinit修饰
            /// 就会确保在被引用的时候才会实例化，而不是程序启动的时候实例化
            /// </summary>
            internal static StaticSingleton instance = new StaticSingleton();
        }
        private StaticSingleton()
        {
            Console.WriteLine(111222333);
        }
        public static StaticSingleton getInstance()
        {
            return InnerInstance.instance;
        }
    }

}
