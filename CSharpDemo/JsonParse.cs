using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace CSharpDemo
{
    class JsonParse
    {
        public void MainMethod()
        {
            // The result is the same.
            // string apiText = @"{""1234"":""ava"", ""list"":[""1"",""2"",""3""]}";
            string apiText = @"{1234:""ava"", list:[1,2,3]}";
            try
            {
                JObject jsonObj = JObject.Parse(apiText);
                Console.WriteLine(jsonObj["1234"]);
                JArray jlist = JArray.Parse(jsonObj["list"].ToString());
                for (int i = 0; i < jlist.Count; i++)
                    Console.WriteLine((int)jlist[i]);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
