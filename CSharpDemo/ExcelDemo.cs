namespace CSharpDemo
{
    using CSharpDemo.FileOperation;
    using System.Collections.Generic;

    public class ExcelDemo
    {
        public static void MainMethod()
        {
            ExcelOperation.WriteFile(@"D:\Output.xlsx", new Dictionary<string, IEnumerable<IEnumerable<string>>>
            {
                ["Sheet1"] = new List<List<string>>
                {
                    new List<string>{ "1","2"},
                    new List<string>{ "3","4"},
                },
                ["Sheet2"] = new List<List<string>>
                {
                    new List<string>{ "a","b"},
                    new List<string>{ "c","d"},
                },
            });
        }
    }
}
