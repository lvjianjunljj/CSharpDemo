using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
namespace CSharpDemo
{
    public class Movie
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("Chinese Director")]
        public string Director { get; set; }

        public int ReleaseYear { get; set; }
    }
    class JsonParseDemo
    {
        static void MainMethod()
        {
            Movie m = new Movie
            {
                Name = "FeiChengWuRao",
                Director = "FengXiaoGang",
                ReleaseYear = 2008
            };

            string json = JsonConvert.SerializeObject(m, Formatting.Indented);
            Console.WriteLine(json);
        }
    }
}
