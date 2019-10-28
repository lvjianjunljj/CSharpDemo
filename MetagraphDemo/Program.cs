using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace MetagraphDemo
{
    class Program
    {
        // The OneNote for access the metagraph data:


        static void Main(string[] args)
        {
            //GetOperation.MainMethod();
            //PostOperation.MainMethod();
            //PutOperation.MainMethod();

            JObject dataInterface = GetOperation.GetIDEAsDataInterface("23f2244b-3567-41fc-bb3c-7ab7c6728a34");
            Console.WriteLine(dataInterface);
            Console.ReadKey();
        }
    }
}
