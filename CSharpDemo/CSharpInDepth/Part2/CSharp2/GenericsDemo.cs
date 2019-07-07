using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.CSharpInDepth.Part2.CSharp2
{
    public class GenericsDemo
    {
        public static void MainMethod()
        {
            // This is different from the content in the book "C# IN DEPTH".
            // We can use generics in the property of a class.
            TestGenericsClass<string> testGenericsClass = new TestGenericsClass<string>();
            testGenericsClass.GenericsDemoList.Add("1");
            testGenericsClass.GenericsDemoList.Add("2");
            testGenericsClass.GenericsDemoList.Add("3");
            testGenericsClass.GenericsDemoList.Add("4");
            testGenericsClass.GenericsDemo = "GenericsDemo";
            foreach (var item in testGenericsClass.GenericsDemoList)
            {
                Console.WriteLine(item);
            }
            Console.WriteLine(testGenericsClass.GenericsDemo);


            // There is an error in this line.
            //TestGenericsIComparableStringClass ts1 = new TestGenericsIComparableStringClass();
            //TestGenericsIComparableStringClass ts2 = new TestGenericsIComparableStringClass();
            //TestGenericsMethod<TestGenericsIComparableStringClass>(ts1, ts2);


            // This is the right demo.
            TestGenericsIComparableClass t1 = new TestGenericsIComparableClass(2);
            TestGenericsIComparableClass t2 = new TestGenericsIComparableClass(4);
            TestGenericsMethod<TestGenericsIComparableClass>(t1, t2);


            List<TestGenericsIComparableClass> testGenericsIComparableClassList = new List<TestGenericsIComparableClass>
            {
                new TestGenericsIComparableClass(4),
                new TestGenericsIComparableClass(2),
                new TestGenericsIComparableClass(6),
                new TestGenericsIComparableClass(9),
                new TestGenericsIComparableClass(8),
                new TestGenericsIComparableClass(7)
            };
            testGenericsIComparableClassList.Sort();
            foreach (var testGenericsIComparableClass in testGenericsIComparableClassList)
            {
                Console.WriteLine($"testGenericsIComparableClass.Value: {testGenericsIComparableClass.Value}");
            }
            //2 4 6 7 8 9 from small to big.
        }

        // contains generics
        private static void TestGenericsMethod<T>(T t1, T t2) where T : IComparable<T>
        {
            Console.WriteLine(t1.CompareTo(t2));
        }

    }

    class TestGenericsClass<T>
    {
        public readonly List<T> GenericsDemoList = new List<T>();
        public T GenericsDemo;
    }

    class TestGenericsIComparableClass : IComparable<TestGenericsIComparableClass>
    {
        public readonly int Value;
        public TestGenericsIComparableClass(int value)
        {
            this.Value = value;
        }


        public int CompareTo(TestGenericsIComparableClass obj)
        {
            return this.Value - obj.Value;
        }
    }

    class TestGenericsIComparableStringClass : IComparable<string>
    {
        public int CompareTo(string str)
        {
            return -this.CompareTo(str);
        }
    }


}