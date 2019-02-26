using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
namespace CSharpDemo.Json
{
    // JSON使用JsonPropertyAttribute重命名属性名 

    // 创建一个Movie对象，然后在其属性上添加JsonProperty，并指定重命名的名称。注意：属性Name和Director已指定。
    class Movie
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("Chinese Director")]
        public string Director { get; set; }
        public int ReleaseYear { get; set; }
    }
    // 创建一个Movie对象，然后指定JsonProperty，并添加Order属性。
    class Movie2
    {
        [JsonProperty(Order = 4)]
        public string Name { get; set; }

        [JsonProperty(Order = 0)]
        public string Director { get; set; }

        public int ReleaseYear { get; set; }

        public string ChiefActor { get; set; }

        [JsonProperty(Order = 2)]
        public string ChiefActress { get; set; }
    }
    class JsonProperty
    {
        public static void MainMethod()
        {
            // 实例化Movie对象，然后序列化。
            Movie m = new Movie
            {
                Name = "FeiChengWuRao",
                Director = "FengXiaoGang",
                ReleaseYear = 2008
            };

            string json = JsonConvert.SerializeObject(m, Formatting.Indented);
            Console.WriteLine(json);

            Movie2 m2 = new Movie2
            {
                Name = "非诚勿扰1",
                Director = "冯小刚",
                ReleaseYear = 2008,
                ChiefActor = "葛优",
                ChiefActress = "舒淇"
            };

            string json2 = JsonConvert.SerializeObject(m2, Formatting.Indented);
            Console.WriteLine(json2);
        }
    }
}
