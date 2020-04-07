namespace CSharpDemo
{
    using AzureLib.KeyVault;
    using CSharpDemo.Application;
    using CSharpDemo.Azure;
    using CSharpDemo.Azure.CosmosDB;
    using CSharpDemo.CSharpInDepth.Part1;
    using CSharpDemo.CSharpInDepth.Part2.CSharp2;
    using CSharpDemo.FileOperation;
    using CSharpDemo.IcMTest;
    using CSharpDemo.IDEAs;
    using CSharpDemo.Json;
    using CSharpDemo.LINQ;
    using CSharpDemo.Parallel;
    using CSharpDemo.ReflectionDemo;
    using CSharpDemo.RetrierDir;
    using CSharpDemoAux;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.ServiceBus;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Reflection;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            //AzureCosmosDBClientOperation.MainMethod();
            //AzureServiceBus.MainMethod();
            //AzureCosmosDB.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DatasetJsonFileOperation.MainMethod();
            //AzureActiveDirectoryToken.MainMethod();
            //TaskDemo.MainMethod();

            //ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            //// I cannot get the secret in project AdlsDemo.
            //string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            //string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;
            //Console.WriteLine(clientId);
            //Console.WriteLine(clientKey);

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("1234");
            sb.AppendLine("1234");
            sb.AppendLine("1234");
            Console.WriteLine(sb.ToString());

            IList<string> list = new List<string>();
            list.Add("11");
            list.Add("22");
            list.Add("33");
            var dict = list.ToDictionary(l => l.Substring(0, 1));
            foreach (var item in dict)
            {
                Console.WriteLine($"{item.Key}\t{item.Value}");
            }

            Console.ReadKey();
        }

        static int[][] GenerateAllArrays()
        {
            int[][] result = new int[120][];
            //int index = 0;
            //for (int i = 10000; i < 60000; i++)
            //{
            //    var chars = (i + "").ToCharArray().Select(c => c + "").ToArray();
            //    if (chars[0] == "0" || chars[1] == "0" || chars[2] == "0" || chars[3] == "0" || chars[4] == "0") continue;
            //    if (int.Parse(chars[0]) > 5 || int.Parse(chars[1]) > 5 || int.Parse(chars[2]) > 5 || int.Parse(chars[3]) > 5 || int.Parse(chars[4]) > 5) continue;
            //    HashSet<string> set = new HashSet<string>(chars);
            //    if (set.Count < 5) continue;
            //    Console.WriteLine($"result[{index++}] =  new int[] " + "{" + string.Join(",", chars.ToArray()) + "};");
            //}
            result[0] = new int[] { 1, 2, 3, 4, 5 };
            result[1] = new int[] { 1, 2, 3, 5, 4 };
            result[2] = new int[] { 1, 2, 4, 3, 5 };
            result[3] = new int[] { 1, 2, 4, 5, 3 };
            result[4] = new int[] { 1, 2, 5, 3, 4 };
            result[5] = new int[] { 1, 2, 5, 4, 3 };
            result[6] = new int[] { 1, 3, 2, 4, 5 };
            result[7] = new int[] { 1, 3, 2, 5, 4 };
            result[8] = new int[] { 1, 3, 4, 2, 5 };
            result[9] = new int[] { 1, 3, 4, 5, 2 };
            result[10] = new int[] { 1, 3, 5, 2, 4 };
            result[11] = new int[] { 1, 3, 5, 4, 2 };
            result[12] = new int[] { 1, 4, 2, 3, 5 };
            result[13] = new int[] { 1, 4, 2, 5, 3 };
            result[14] = new int[] { 1, 4, 3, 2, 5 };
            result[15] = new int[] { 1, 4, 3, 5, 2 };
            result[16] = new int[] { 1, 4, 5, 2, 3 };
            result[17] = new int[] { 1, 4, 5, 3, 2 };
            result[18] = new int[] { 1, 5, 2, 3, 4 };
            result[19] = new int[] { 1, 5, 2, 4, 3 };
            result[20] = new int[] { 1, 5, 3, 2, 4 };
            result[21] = new int[] { 1, 5, 3, 4, 2 };
            result[22] = new int[] { 1, 5, 4, 2, 3 };
            result[23] = new int[] { 1, 5, 4, 3, 2 };
            result[24] = new int[] { 2, 1, 3, 4, 5 };
            result[25] = new int[] { 2, 1, 3, 5, 4 };
            result[26] = new int[] { 2, 1, 4, 3, 5 };
            result[27] = new int[] { 2, 1, 4, 5, 3 };
            result[28] = new int[] { 2, 1, 5, 3, 4 };
            result[29] = new int[] { 2, 1, 5, 4, 3 };
            result[30] = new int[] { 2, 3, 1, 4, 5 };
            result[31] = new int[] { 2, 3, 1, 5, 4 };
            result[32] = new int[] { 2, 3, 4, 1, 5 };
            result[33] = new int[] { 2, 3, 4, 5, 1 };
            result[34] = new int[] { 2, 3, 5, 1, 4 };
            result[35] = new int[] { 2, 3, 5, 4, 1 };
            result[36] = new int[] { 2, 4, 1, 3, 5 };
            result[37] = new int[] { 2, 4, 1, 5, 3 };
            result[38] = new int[] { 2, 4, 3, 1, 5 };
            result[39] = new int[] { 2, 4, 3, 5, 1 };
            result[40] = new int[] { 2, 4, 5, 1, 3 };
            result[41] = new int[] { 2, 4, 5, 3, 1 };
            result[42] = new int[] { 2, 5, 1, 3, 4 };
            result[43] = new int[] { 2, 5, 1, 4, 3 };
            result[44] = new int[] { 2, 5, 3, 1, 4 };
            result[45] = new int[] { 2, 5, 3, 4, 1 };
            result[46] = new int[] { 2, 5, 4, 1, 3 };
            result[47] = new int[] { 2, 5, 4, 3, 1 };
            result[48] = new int[] { 3, 1, 2, 4, 5 };
            result[49] = new int[] { 3, 1, 2, 5, 4 };
            result[50] = new int[] { 3, 1, 4, 2, 5 };
            result[51] = new int[] { 3, 1, 4, 5, 2 };
            result[52] = new int[] { 3, 1, 5, 2, 4 };
            result[53] = new int[] { 3, 1, 5, 4, 2 };
            result[54] = new int[] { 3, 2, 1, 4, 5 };
            result[55] = new int[] { 3, 2, 1, 5, 4 };
            result[56] = new int[] { 3, 2, 4, 1, 5 };
            result[57] = new int[] { 3, 2, 4, 5, 1 };
            result[58] = new int[] { 3, 2, 5, 1, 4 };
            result[59] = new int[] { 3, 2, 5, 4, 1 };
            result[60] = new int[] { 3, 4, 1, 2, 5 };
            result[61] = new int[] { 3, 4, 1, 5, 2 };
            result[62] = new int[] { 3, 4, 2, 1, 5 };
            result[63] = new int[] { 3, 4, 2, 5, 1 };
            result[64] = new int[] { 3, 4, 5, 1, 2 };
            result[65] = new int[] { 3, 4, 5, 2, 1 };
            result[66] = new int[] { 3, 5, 1, 2, 4 };
            result[67] = new int[] { 3, 5, 1, 4, 2 };
            result[68] = new int[] { 3, 5, 2, 1, 4 };
            result[69] = new int[] { 3, 5, 2, 4, 1 };
            result[70] = new int[] { 3, 5, 4, 1, 2 };
            result[71] = new int[] { 3, 5, 4, 2, 1 };
            result[72] = new int[] { 4, 1, 2, 3, 5 };
            result[73] = new int[] { 4, 1, 2, 5, 3 };
            result[74] = new int[] { 4, 1, 3, 2, 5 };
            result[75] = new int[] { 4, 1, 3, 5, 2 };
            result[76] = new int[] { 4, 1, 5, 2, 3 };
            result[77] = new int[] { 4, 1, 5, 3, 2 };
            result[78] = new int[] { 4, 2, 1, 3, 5 };
            result[79] = new int[] { 4, 2, 1, 5, 3 };
            result[80] = new int[] { 4, 2, 3, 1, 5 };
            result[81] = new int[] { 4, 2, 3, 5, 1 };
            result[82] = new int[] { 4, 2, 5, 1, 3 };
            result[83] = new int[] { 4, 2, 5, 3, 1 };
            result[84] = new int[] { 4, 3, 1, 2, 5 };
            result[85] = new int[] { 4, 3, 1, 5, 2 };
            result[86] = new int[] { 4, 3, 2, 1, 5 };
            result[87] = new int[] { 4, 3, 2, 5, 1 };
            result[88] = new int[] { 4, 3, 5, 1, 2 };
            result[89] = new int[] { 4, 3, 5, 2, 1 };
            result[90] = new int[] { 4, 5, 1, 2, 3 };
            result[91] = new int[] { 4, 5, 1, 3, 2 };
            result[92] = new int[] { 4, 5, 2, 1, 3 };
            result[93] = new int[] { 4, 5, 2, 3, 1 };
            result[94] = new int[] { 4, 5, 3, 1, 2 };
            result[95] = new int[] { 4, 5, 3, 2, 1 };
            result[96] = new int[] { 5, 1, 2, 3, 4 };
            result[97] = new int[] { 5, 1, 2, 4, 3 };
            result[98] = new int[] { 5, 1, 3, 2, 4 };
            result[99] = new int[] { 5, 1, 3, 4, 2 };
            result[100] = new int[] { 5, 1, 4, 2, 3 };
            result[101] = new int[] { 5, 1, 4, 3, 2 };
            result[102] = new int[] { 5, 2, 1, 3, 4 };
            result[103] = new int[] { 5, 2, 1, 4, 3 };
            result[104] = new int[] { 5, 2, 3, 1, 4 };
            result[105] = new int[] { 5, 2, 3, 4, 1 };
            result[106] = new int[] { 5, 2, 4, 1, 3 };
            result[107] = new int[] { 5, 2, 4, 3, 1 };
            result[108] = new int[] { 5, 3, 1, 2, 4 };
            result[109] = new int[] { 5, 3, 1, 4, 2 };
            result[110] = new int[] { 5, 3, 2, 1, 4 };
            result[111] = new int[] { 5, 3, 2, 4, 1 };
            result[112] = new int[] { 5, 3, 4, 1, 2 };
            result[113] = new int[] { 5, 3, 4, 2, 1 };
            result[114] = new int[] { 5, 4, 1, 2, 3 };
            result[115] = new int[] { 5, 4, 1, 3, 2 };
            result[116] = new int[] { 5, 4, 2, 1, 3 };
            result[117] = new int[] { 5, 4, 2, 3, 1 };
            result[118] = new int[] { 5, 4, 3, 1, 2 };
            result[119] = new int[] { 5, 4, 3, 2, 1 };
            return result;
        }



    }
}
