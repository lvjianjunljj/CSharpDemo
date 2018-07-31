using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class TaskFriend
    {
        public static void ExcelMethod()
        {
            DateTime beforDT = System.DateTime.Now;
            Console.WriteLine(beforDT.ToString("[HH:mm:ss] "));
            //ExcelOperation e = new ExcelOperation();
            //List<List<string>> data = e.readFile("D:\\personal debris\\yangyu\\固定资产卡片.xlsm", "固定资产卡片", 2, 2445, 1, 24);
            //SaveFile.Save("D:\\test0.txt", data);
            //Console.WriteLine("done 0");

            //List<List<string>> dataSplit1 = e.readFile("D:\\personal debris\\yangyu\\明波评估明细表八点半发2018.06.30.xlsm", "4-6-1房屋建筑物", 4, 117, 2, 17);
            //SaveFile.Save("D:\\test1.txt", dataSplit1);
            //Console.WriteLine("done 1");

            //List<List<string>> dataSplit2 = e.readFile("D:\\personal debris\\yangyu\\明波评估明细表八点半发2018.06.30.xlsm", "4-6-2构筑物", 2, 1, 2, 1);
            //SaveFile.Save("D:\\test2.txt", dataSplit2);
            //Console.WriteLine("done 2");

            //List<List<string>> dataSplit3 = e.readFile("D:\\personal debris\\yangyu\\明波评估明细表八点半发2018.06.30.xlsm", "4-6-3A管道沟槽", 4, 65, 2, 15);
            //SaveFile.Save("D:\\test3.txt", dataSplit3);
            //Console.WriteLine("done 3");

            //List<List<string>> dataSplit4 = e.readFile("D:\\personal debris\\yangyu\\明波评估明细表八点半发2018.06.30.xlsm", "4-6-4机器设备", 4, 1937, 2, 16);
            //SaveFile.Save("D:\\test4.txt", dataSplit4);
            //Console.WriteLine("done 4");

            //List<List<string>> dataSplit5 = e.readFile("D:\\personal debris\\yangyu\\明波评估明细表八点半发2018.06.30.xlsm", "4-6-5车辆", 4, 39, 2, 17);
            //SaveFile.Save("D:\\test5.txt", dataSplit5);
            //Console.WriteLine("done 5");

            //List<List<string>> dataSplit6 = e.readFile("D:\\personal debris\\yangyu\\明波评估明细表八点半发2018.06.30.xlsm", "4-6-6电子设备", 4, 303, 2, 16);
            //SaveFile.Save("D:\\test6.txt", dataSplit6);
            //Console.WriteLine("done 6");

            List<List<string>> data = ReadFile.ForthMethod("D:\\test0.txt");
            List<List<string>> dataSplit1 = ReadFile.ForthMethod("D:\\test1.txt");
            List<List<string>> dataSplit2 = ReadFile.ForthMethod("D:\\test2.txt");
            List<List<string>> dataSplit3 = ReadFile.ForthMethod("D:\\test3.txt");
            List<List<string>> dataSplit4 = ReadFile.ForthMethod("D:\\test4.txt");
            List<List<string>> dataSplit5 = ReadFile.ForthMethod("D:\\test5.txt");
            List<List<string>> dataSplit6 = ReadFile.ForthMethod("D:\\test6.txt");
            List<List<List<string>>> dataSplitList = new List<List<List<string>>>();
            dataSplitList.Add(dataSplit1);
            dataSplitList.Add(dataSplit2);
            dataSplitList.Add(dataSplit3);
            dataSplitList.Add(dataSplit4);
            dataSplitList.Add(dataSplit5);
            dataSplitList.Add(dataSplit6);
            int count = 0;
            for (int x = 0; x < 6; x++)
            {
                if (x == 4)
                {
                    continue;
                }
                foreach (List<string> list in dataSplitList[x])
                {
                    string indexStr = list[0];
                    foreach (List<string> queryList in data)
                    {
                        string queryStr = queryList[2];
                        if (x == 2)
                        {
                            queryStr = queryList[3];
                        }
                        if (indexStr.Equals(queryStr))
                        {
                            StreamWriter writer;
                            string sortResult = queryList[1];
                            switch (sortResult)
                            {
                                case "房屋建筑物": writer = File.AppendText("D:\\res0.txt"); break;
                                case "构筑物": writer = File.AppendText("D:\\res1.txt"); break;
                                case "管道沟槽": writer = File.AppendText("D:\\res2.txt"); break;
                                case "机器设备": writer = File.AppendText("D:\\res3.txt"); break;
                                case "车辆": writer = File.AppendText("D:\\res4.txt"); break;
                                case "电子设备": writer = File.AppendText("D:\\res5.txt"); break;
                                default:
                                    writer = File.AppendText("D:\\res6.txt");
                                    count++;
                                    Console.WriteLine(sortResult);
                                    break;

                            }
                            string writeContent = "";
                            if (x == 2)
                            {
                                writeContent += "\t";
                            }
                            for (int k = 0; k < list.Count; k++)
                            {
                                writeContent += list[k] + "\t";
                            }
                            writer.WriteLine(writeContent);
                            writer.Flush();
                            writer.Close();
                            break;
                        }
                    }
                }
            }
            Console.WriteLine(count);

            DateTime afterDT = System.DateTime.Now;
            Console.WriteLine(afterDT.ToString("[HH:mm:ss] "));
            TimeSpan ts = afterDT.Subtract(beforDT);
            Console.WriteLine(ts.TotalMilliseconds);
            Console.WriteLine("done all");


            //List<List<string>> data = new List<List<string>>();
            //List<string> line = new List<string>();
            //line.Add("1");
            //line.Add("1");
            //line.Add("1");
            //data.Add(line);
            //data.Add(line);
            //data.Add(line);
            //data.Add(line);
            //data.Add(line);
            //SaveFile.Save("D:\\test.txt", data);
        }
    }
}
