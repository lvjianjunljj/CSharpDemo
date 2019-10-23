using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
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
            PutOperation.MainMethod();
            Console.ReadKey();
        }

    }
}
