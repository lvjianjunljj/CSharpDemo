namespace CSharpDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class SerializableMarkDemo
    {
        public static void MainMethod()
        {
            Customer customer = new Customer();

            customer.CustomerID = 1;
            customer.Name = "John";

            // If we use this line in the WPF Application project, You will throw an Exception:
            // Type 'Customer' in Assembly '..., Version=0.0.0.0, Culture=neutral, PublicKeyToken=null' is not marked as serializable.
            //ViewState["Customer"] = customer;
            //Session["Customer"] = customer;
        }
    }


    [Serializable]
    public class MyBase
    {

    }

    public class Customer : MyBase
    {
        private int customerID;
        private string name;

        public int CustomerID
        {
            get { return customerID; }
            set { customerID = value; }
        }

        public string Name
        {
            get { return name; }
            set { name = value; }
        }
    }
}
