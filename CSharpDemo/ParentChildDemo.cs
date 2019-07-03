namespace CSharpDemo
{
    using Newtonsoft.Json;
    using System;
    class ParentChildDemo
    {
        static void MainMethod()
        {
            Parent p = Test();
            // Here we cannot access the property C and D in child class
            // But we can get the property value by JsonConvert.SerializeObject function.
            Console.WriteLine(JsonConvert.SerializeObject(p));
        }
        static Parent Test()
        {

            Parent p = new Parent();
            p.A = "A";
            p.B = "B";
            // We cannot convert a parent class to a child class.
            // If we want this, we need to define function by ourselves.
            //Child c = (Child)p;
            Child c = new Child();
            c.C = "C";
            c.D = "D";
            return c;
        }

    }
    class Parent
    {
        public string A { get; set; }
        public string B { get; set; }
    }
    class Child : Parent
    {
        public string C { get; set; }
        public string D { get; set; }
    }
}
