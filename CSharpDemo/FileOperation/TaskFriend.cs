﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.FileOperation
{
    class TaskFriend
    {
        public static void MainMethod()
        {
            string filePath = @"D:\data\personal_data\Yang Yu\Intern work\excel分类\7_2\{0}";

            DateTime beforDT = System.DateTime.Now;
            Console.WriteLine(beforDT.ToString("yyyy-MM-dd [HH:mm:ss] "));

            // Step 1: Move all data in Excel to txt file for the next work
            // Read data from txt file is much quicker than from excel file

            List<List<string>> data = ExcelOperation.ReadFile(string.Format(filePath, "固定资产卡片.xlsm"), "固定资产卡片", 2, 2445, 1, 24);
            WriteFile.Save("D:\\test0.txt", data);
            Console.WriteLine("done 0");

            List<List<string>> dataSplit1 = ExcelOperation.ReadFile(string.Format(filePath, "明波评估明细表八点半发2018.06.30.xlsm"), "4-6-1房屋建筑物", 4, 117, 2, 17);
            WriteFile.Save("D:\\test1.txt", dataSplit1);
            Console.WriteLine("done 1");

            List<List<string>> dataSplit2 = ExcelOperation.ReadFile(string.Format(filePath, "明波评估明细表八点半发2018.06.30.xlsm"), "4-6-2构筑物", 2, 1, 2, 1);
            WriteFile.Save("D:\\test2.txt", dataSplit2);
            Console.WriteLine("done 2");

            List<List<string>> dataSplit3 = ExcelOperation.ReadFile(string.Format(filePath, "明波评估明细表八点半发2018.06.30.xlsm"), "4-6-3A管道沟槽", 4, 65, 2, 15);
            WriteFile.Save("D:\\test3.txt", dataSplit3);
            Console.WriteLine("done 3");

            List<List<string>> dataSplit4 = ExcelOperation.ReadFile(string.Format(filePath, "明波评估明细表八点半发2018.06.30.xlsm"), "4-6-4机器设备", 4, 1937, 2, 16);
            WriteFile.Save("D:\\test4.txt", dataSplit4);
            Console.WriteLine("done 4");

            List<List<string>> dataSplit5 = ExcelOperation.ReadFile(string.Format(filePath, "明波评估明细表八点半发2018.06.30.xlsm"), "4-6-5车辆", 4, 39, 2, 17);
            WriteFile.Save("D:\\test5.txt", dataSplit5);
            Console.WriteLine("done 5");

            List<List<string>> dataSplit6 = ExcelOperation.ReadFile(string.Format(filePath, "明波评估明细表八点半发2018.06.30.xlsm"), "4-6-6电子设备", 4, 303, 2, 16);
            WriteFile.Save("D:\\test6.txt", dataSplit6);
            Console.WriteLine("done 6");

            //Step 2: Read the data from txt file and do some operations.

            var newData = ReadFile.ForthMethod("D:\\test0.txt");
            var newDataSplit1 = ReadFile.ForthMethod("D:\\test1.txt");
            var newDataSplit2 = ReadFile.ForthMethod("D:\\test2.txt");
            var newDataSplit3 = ReadFile.ForthMethod("D:\\test3.txt");
            var newDataSplit4 = ReadFile.ForthMethod("D:\\test4.txt");
            var newDataSplit5 = ReadFile.ForthMethod("D:\\test5.txt");
            var newDataSplit6 = ReadFile.ForthMethod("D:\\test6.txt");

            var newDataSplitList = new List<IEnumerable<IEnumerable<string>>>();
            newDataSplitList.Add(newDataSplit1);
            newDataSplitList.Add(newDataSplit2);
            newDataSplitList.Add(newDataSplit3);
            newDataSplitList.Add(newDataSplit4);
            newDataSplitList.Add(newDataSplit5);
            newDataSplitList.Add(newDataSplit6);
            int count = 0;
            for (int x = 0; x < 6; x++)
            {
                if (x == 4)
                {
                    continue;
                }
                foreach (List<string> list in newDataSplitList[x])
                {
                    string indexStr = list[0];
                    foreach (List<string> queryList in newData)
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
        }
    }
}
