namespace CSharpDemo
{
    using System;

    class SerializelassDemo
    {
        public static void MainMethod()
        {
            // test1 is the same as test2

            // This writing will call the constructor of Test2 without input
            TestNewClass test1 = new TestNewClass
            {
                T2 = "T2"
            };
            Console.WriteLine(test1.T1);

            TestNewClass test2 = new TestNewClass()
            {
                T2 = "T2"
            };
            Console.WriteLine(test2.T1);
        }
    }

    class TestNewClass
    {
        public string T1 { get; set; }
        public string T2 { get; set; }
        public TestNewClass()
        {
            Console.WriteLine("1111");
            this.T1 = "T1";
        }
    }
}
