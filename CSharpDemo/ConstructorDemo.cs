namespace CSharpDemo
{
    using Newtonsoft.Json;
    using System;

    public class ConstructorDemo
    {
        public static void MainMethod()
        {
            ConstructorTestClass2 c2 = new ConstructorTestClass2()
            {
                String1 = "String1",
                String2 = "String2"
            };
            ConstructorTestClass1 c1 = new ConstructorTestClass1(c2);
            // Error

            string c1String = JsonConvert.SerializeObject(c1);
            Console.WriteLine(c1String);

            /*
             * If we need to deserialize a string to target class, we need a empty constructor in the class.
             * Or it will throw the exception:
             * System.NullReferenceException: 'Object reference not set to an instance of an object.'
             * t2 was null.
             */
            c1 = JsonConvert.DeserializeObject<ConstructorTestClass1>(c1String);

            // We cannot use new ConstructorTestClass1() to instantiate an object if there is not an empty constructor in the class 
            //c1 = new ConstructorTestClass1();
        }
    }
    class ConstructorTestClass1
    {
        /*
         * If there is not another constructor function with input parameters in the class, 
         * there is an empty constructor by default in the class.
         * But if there is another constructor function with input parameters in the class，
         * we need to define the empty constructor if we need it.
         */
        //public ConstructorTestClass1()
        //{

        //}
        public ConstructorTestClass1(ConstructorTestClass2 t2)
        {
            this.String1 = t2.String1;
            this.String2 = t2.String2;
        }
        public string String1 { get; set; }
        public string String2 { get; set; }
    }
    class ConstructorTestClass2
    {
        public string String1 { get; set; }
        public string String2 { get; set; }
    }
}
