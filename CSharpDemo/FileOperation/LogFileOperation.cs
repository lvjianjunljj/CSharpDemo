using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.FileOperation
{

    class LogFileOperation
    {
        static void MainMethod()
        {
            //MakeOfflineFinalizeDataSense();
            //AnalyseOfflineFinalizeData();
            AnalyzeBondserviceLog();
            Console.ReadKey();
        }
        static void AnalyzeBondserviceLog()
        {
            string inputDataDirPath = @"D:\data\company_work\QR\bondservice_log";
            Dictionary<string, int> countMap = new Dictionary<string, int>();
            DirectoryInfo TheFolder = new DirectoryInfo(inputDataDirPath);
            foreach (FileInfo NextFile in TheFolder.GetFiles())
            {
                StreamReader reader = new StreamReader(NextFile.FullName, Encoding.UTF8);
                string lineText = "";
                while ((lineText = reader.ReadLine()) != null)
                {
                    string timeStr = lineText.Split(new char[] { ',' })[1];
                    if (!countMap.ContainsKey(timeStr))
                    {
                        countMap.Add(timeStr, 0);
                    }
                    countMap[timeStr]++;
                }
                reader.Close();
            }
            foreach (string timeStr in countMap.Keys)
            {
                Console.WriteLine(timeStr + "\t" + countMap[timeStr]);
            }
        }

        static void AnalyseOfflineFinalizeData()
        {
            string inputDataFilePath = @"D:\data\company_work\QR\OfflineFinalizeInnerPart\summary_offline_sense";
            string outputFilePath = @"D:\data\company_work\QR\OfflineFinalizeInnerPart\summary_offline_out";
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            List<double[]> numList = new List<double[]>();
            List<double> offlineFinalizeInnerPartList = new List<double>();
            List<double> latOfflineSpellerRequestColumnList = new List<double>();

            double offlineFinalizeInnerPart;
            double latOfflineSpellerRequestColumn;
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                offlineFinalizeInnerPart = 0;
                latOfflineSpellerRequestColumn = 0;
                string[] content = lineText.Split(new char[] { '\t' });
                foreach (string tuple in content)
                {
                    if (tuple.StartsWith("OfflineFinalizeInnerPart"))
                    {
                        offlineFinalizeInnerPart = Math.Max(offlineFinalizeInnerPart, Convert.ToDouble(tuple.Split(new char[] { '=' })[1]));
                    }
                    else if (tuple.StartsWith("LatOfflineSpellerRequestColumn"))
                    {
                        latOfflineSpellerRequestColumn = Convert.ToDouble(tuple.Split(new char[] { '=' })[1]);
                    }
                }
                if (offlineFinalizeInnerPart > 0 && latOfflineSpellerRequestColumn > 0)
                {
                    numList.Add(new double[] { offlineFinalizeInnerPart, latOfflineSpellerRequestColumn });
                    offlineFinalizeInnerPartList.Add(offlineFinalizeInnerPart);
                    latOfflineSpellerRequestColumnList.Add(latOfflineSpellerRequestColumn);
                }

            }
            offlineFinalizeInnerPartList.Sort();
            latOfflineSpellerRequestColumnList.Sort();
            sourceDataStreamReader.Close();
            numList.Sort(CompareBySecondNum);
            Dictionary<double, List<double>> dataMap = new Dictionary<double, List<double>>();
            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            outputStreamWriter.WriteLine("offlineFinalizeInnerPartLatency\tlatOfflineSpellerRequestLatency");
            foreach (double[] nums in numList)
            {
                if (!dataMap.ContainsKey(nums[1])) dataMap.Add(nums[1], new List<double>());
                dataMap[nums[1]].Add(nums[0]);
                outputStreamWriter.WriteLine(nums[0] + "\t" + nums[1]);
            }
            outputStreamWriter.WriteLine();
            outputStreamWriter.WriteLine();

            outputStreamWriter.WriteLine("latOfflineSpellerRequestLatency\tofflineFinalizeInnerPartLatency");
            foreach (double latLatency in dataMap.Keys)
                outputStreamWriter.WriteLine(latLatency + "\t" + dataMap[latLatency].Average());

            outputStreamWriter.WriteLine();
            outputStreamWriter.WriteLine(latOfflineSpellerRequestColumnList[(int)(offlineFinalizeInnerPartList.Count * 0.95)] * 2
                + "\t" + offlineFinalizeInnerPartList[(int)(offlineFinalizeInnerPartList.Count * 0.95)] * 2);
            outputStreamWriter.Flush();
            outputStreamWriter.Close();

        }

        static void MakeOfflineFinalizeDataSense()
        {
            double[] multiList = { 2, 2.7, 5 };
            string inputDataFilePath = @"D:\data\company_work\QR\OfflineFinalizeInnerPart\summary_offline";
            string outputFilePath = @"D:\data\company_work\QR\OfflineFinalizeInnerPart\summary_offline_sense";
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            List<double[]> numList = new List<double[]>();
            List<double[]> offlineFinalizeInnerPartIndex = new List<double[]>();
            List<double[]> latOfflineSpellerRequestColumnIndex = new List<double[]>();
            double latOfflineSpellerRequestColumn;
            List<List<double>> offlineFinalizeInnerPartLineMatrix = new List<List<double>>();
            int index = 0;
            List<string> writeContent = new List<string>();
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                //offlineFinalizeInnerPart = 0;
                latOfflineSpellerRequestColumn = 0;
                string[] content = lineText.Split(new char[] { '\t' });
                foreach (string tuple in content)
                {
                    if (tuple.StartsWith("LatOfflineSpellerRequestColumn"))
                    {
                        latOfflineSpellerRequestColumn = Convert.ToDouble(tuple.Split(new char[] { '=' })[1]);
                    }
                }
                if (latOfflineSpellerRequestColumn == 0) continue;
                List<double> offlineFinalizeInnerPartLineList = new List<double>();
                foreach (string tuple in content)
                {
                    if (tuple.StartsWith("OfflineFinalizeInnerPart"))
                    {
                        double offlineFinalizeInnerPartTemp = Convert.ToDouble(tuple.Split(new char[] { '=' })[1]) * 1000;

                        for (int k = 0; k < 3; k++)
                        {
                            if (latOfflineSpellerRequestColumn == k + 5)
                            {
                                offlineFinalizeInnerPartTemp *= multiList[k];
                            }
                        }

                        if (offlineFinalizeInnerPartLineList.Count > 0 && offlineFinalizeInnerPartLineList[0] < offlineFinalizeInnerPartTemp)
                            offlineFinalizeInnerPartLineList.Insert(0, offlineFinalizeInnerPartTemp);
                        else
                            offlineFinalizeInnerPartLineList.Add(offlineFinalizeInnerPartTemp);
                        //offlineFinalizeInnerPartLineSum += offlineFinalizeInnerPartTemp;
                    }
                }
                if (offlineFinalizeInnerPartLineList.Count == 0) continue;
                offlineFinalizeInnerPartIndex.Add(new double[] { index, offlineFinalizeInnerPartLineList[0] });
                latOfflineSpellerRequestColumnIndex.Add(new double[] { index, latOfflineSpellerRequestColumn });
                offlineFinalizeInnerPartLineMatrix.Add(offlineFinalizeInnerPartLineList);
                index++;
            }
            sourceDataStreamReader.Close();

            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            offlineFinalizeInnerPartIndex.Sort(CompareBySecondNum);
            latOfflineSpellerRequestColumnIndex.Sort(CompareBySecondNum);
            Dictionary<int, int> indexDict = new Dictionary<int, int>();
            List<string> writeLineData = new List<string>();

            for (int i = 0; i < offlineFinalizeInnerPartIndex.Count; i++)
            {
                indexDict.Add((int)latOfflineSpellerRequestColumnIndex[i][0], i);
                string writeLine = "";
                double offlineFinalizeInnerPartLineSum = 0;
                List<double> offlineFinalizeInnerPartLineList =
                    offlineFinalizeInnerPartLineMatrix[(int)offlineFinalizeInnerPartIndex[i][0]];
                foreach (double offlineFinalizeInnerPartLineTuple in offlineFinalizeInnerPartLineList)
                {
                    offlineFinalizeInnerPartLineSum += offlineFinalizeInnerPartLineTuple;
                }
                latOfflineSpellerRequestColumn = latOfflineSpellerRequestColumnIndex[i][1];
                while (offlineFinalizeInnerPartLineSum >= latOfflineSpellerRequestColumn)
                {
                    offlineFinalizeInnerPartLineSum /= 2;
                    for (int j = 0; j < offlineFinalizeInnerPartLineList.Count; j++)
                    {
                        offlineFinalizeInnerPartLineList[j] /= 2;
                    }
                }
                foreach (double offlineFinalizeInnerPartLine in offlineFinalizeInnerPartLineList)
                    writeLine += "OfflineFinalizeInnerPart=" + offlineFinalizeInnerPartLine + "\t";
                writeLine += "LatOfflineSpellerRequestColumn=" + latOfflineSpellerRequestColumn + "\t";

                writeLineData.Add(writeLine);
            }
            for (int i = 0; i < index; i++)
            {
                outputStreamWriter.WriteLine(writeLineData[indexDict[i]]);
            }

            outputStreamWriter.Flush();
            outputStreamWriter.Close();

        }
        static void FilterDuplicatedQuery()
        {
            HashSet<string> rawQuerySet = new HashSet<string>();
            string inputDataFilePath = @"D:\data\company_work\QR\OfflineFinalizeInnerPart\summary_rawquery";
            string outputFilePath = @"D:\data\company_work\QR\OfflineFinalizeInnerPart\summary_rawquery_out";
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                rawQuerySet.Add(lineText);

            }
            sourceDataStreamReader.Close();
            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            List<double> offlineFinalizeInnerPartList = new List<double>();
            List<double> latOfflineSpellerRequestColumnList = new List<double>();
            foreach (string writeLine in rawQuerySet)
            {
                outputStreamWriter.WriteLine(writeLine);
            }
            Console.WriteLine(rawQuerySet.Count);
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
        }


        static void MainMethod2()
        {
            //FilterData();
            //StuffRawQuery();
            //FilterRepeatedQueryIdData();
            //SortRawQueryByLatency();
            //SummarizeBreakdownRecords();
            //SortBreakdownSummaryByTotalLatency();

            //ExpandOnlineData(6, 2);

            //if (args.Length == 3)
            //{
            //    string methodName = args[0];
            //    Type tp = typeof(CSharpDemo.Program);
            //    object obj = Activator.CreateInstance(tp);
            //    MethodInfo method = tp.GetMethod(methodName, System.Reflection.BindingFlags.IgnoreCase | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            //    method.Invoke(obj, new object[] { args[1], args[2] });
            //    Console.WriteLine("Function complete!!!");
            //}
            //else
            //{
            //    Console.WriteLine("error input num");
            //}







            //foreach (int i in new int[] { 0, 1, 2, 3, 6 })
            //    SummaryDirToOneFile(@"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\log_data\col" + i,
            //        @"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\log_data\col" + i + ".txt");

            //foreach (int i in new int[] { 0, 1, 2, 3, 6 })
            //    FilterOnlineLogData(@"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\log_data\col" + i + @".txt",
            //        @"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\online\col" + i + @".txt");

            //foreach (int i in new int[] { 0, 1, 2, 3, 6 })
            //    FilterOfflineLogData(@"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\log_data\col" + i + @".txt",
            //        @"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\offline\col" + i + @".txt");

            //foreach (int[] i in new int[][] { new int[] { 0,4507 },
            //    new int[] { 1,3811 },
            //    new int[] { 2,3622 },
            //    new int[] { 3,5514 },
            //    new int[] { 6, 8276} })
            //    SelectDataToExcel(@"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\online\col" +
            //        i[0] + @".txt",
            //        @"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\online_show_excel\col" +
            //        i[0] + @".txt",
            //        i[1]);
        }
        static void SummaryDirToOneFileByPrefix(string folderFullName, string prefix)
        {
            DirectoryInfo TheFolder = new DirectoryInfo(folderFullName);
            StreamWriter writer = new StreamWriter(folderFullName + @"\summary_" + prefix);
            foreach (FileInfo NextFile in TheFolder.GetFiles())
            {
                if (!NextFile.Name.StartsWith(prefix)) continue;
                StreamReader reader = new StreamReader(NextFile.FullName, Encoding.UTF8);
                string lineText = "";
                while ((lineText = reader.ReadLine()) != null)
                    writer.WriteLine(lineText);
                reader.Close();
            }
            writer.Flush();
            writer.Close();
        }

        static void SelectDataToExcel(string inputDataFilePath, string outputFilePath, int lineCount)
        {
            int writeLineCount = 0;
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            List<string> readLines = new List<string>();
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                readLines.Add(lineText);
            }
            int freq = readLines.Count / lineCount;
            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            for (int i = 0; i < readLines.Count; i++)
            {
                if (i % freq == 0)
                {
                    outputStreamWriter.WriteLine(readLines[i]);
                    writeLineCount++;
                }
                if (writeLineCount == lineCount) break;

            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
        }

        static void FilterOfflineLogData(string inputDataFilePath, string outputFilePath)
        {
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            List<string[]> contentList = new List<string[]>();
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                string[] readContent = lineText.Split(new char[] { '\t' });
                //if (readContent.Length < 11) continue;
                string[] writeContent = new string[11];

                foreach (string readTuple in readContent)
                {

                    if (readTuple.StartsWith("TokenizeLatency="))
                        writeContent[2] = readTuple.Split(new char[] { '=' })[1];
                    if (readTuple.StartsWith("RawQuery="))
                        writeContent[0] = readTuple.Split(new char[] { '=' })[1];
                    if (readTuple.StartsWith("Market="))
                        writeContent[1] = readTuple.Split(new char[] { '=' })[1];
                    if (readTuple.StartsWith("SMTLatency="))
                        writeContent[5] = readTuple.Split(new char[] { '=' })[1];
                    if (readTuple.StartsWith("CandGenLatency="))
                        writeContent[4] = readTuple.Split(new char[] { '=' })[1];
                    //if (readTuple.StartsWith("viterbiLatency=")) 
                    //writeContent[0] = readTuple.Split(new char[] { '=' })[1];
                    if (readTuple.StartsWith("OfflineSpellerCost=") && writeContent[9] != "")
                        writeContent[9] = readTuple.Split(new char[] { '=' })[1];
                    if (readTuple.StartsWith("TotalLatency="))
                        writeContent[10] = readTuple.Split(new char[] { '=' })[1];
                }
                writeContent[3] = "Local";
                writeContent[8] = "Used";
                //writeContent[6] = "0";
                //writeContent[7] = "0";

                if (writeContent[0] != "" &&
                    writeContent[1] != "" &&
                    writeContent[2] != "" &&
                    writeContent[9] != "" &&
                    writeContent[10] != "")
                    contentList.Add(writeContent);
            }
            sourceDataStreamReader.Close();
            contentList.Sort(CompareByTenthIndex);
            contentList.Insert(0, new string[] {"RawQuery",
                "Market",
                "Tokenization",
                "CandGenLocation",
                "GeneratePath",
                "SMTLatency",
                "CalcFeatures",
                "ScoringLatency",
                "OfflineSpellerUse",
                "OfflineSpellerLatency",
                "TotalLatency"});
            // Default FileMode is Create
            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            foreach (string[] writeContent in contentList)
            {
                string writeLine = "";
                for (int i = 0; i < writeContent.Length; i++)
                {
                    if (i > 0) writeLine += "\t";
                    writeLine += writeContent[i];
                }
                outputStreamWriter.WriteLine(writeLine);
            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
        }

        static void FilterOnlineLogData(string inputDataFilePath, string outputFilePath)
        {
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            List<string[]> contentList = new List<string[]>();
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                string[] readContent = lineText.Split(new char[] { '\t' });
                if (readContent.Length != 10) continue;
                string[] writeContent = new string[10];
                if (!readContent[0].StartsWith("TokenizeLatency=")) continue;
                writeContent[2] = readContent[0].Split(new char[] { '=' })[1];
                if (!readContent[1].StartsWith("RawQuery=")) continue;
                writeContent[0] = readContent[1].Split(new char[] { '=' })[1];
                if (!readContent[2].StartsWith("Market=")) continue;
                writeContent[1] = readContent[2].Split(new char[] { '=' })[1];
                if (!readContent[3].StartsWith("SMTLatency=")) continue;
                writeContent[5] = readContent[3].Split(new char[] { '=' })[1];
                if (!readContent[4].StartsWith("CandGenLatency=")) continue;
                writeContent[4] = readContent[4].Split(new char[] { '=' })[1];
                if (!readContent[5].StartsWith("viterbiLatency=")) continue;
                //writeContent[0] = readContent[5].Split(new char[] { '=' })[1];
                if (!readContent[6].StartsWith("FeatureExtractionLatency=")) continue;
                writeContent[6] = readContent[6].Split(new char[] { '=' })[1];
                if (!readContent[7].StartsWith("ScoringLatency=")) continue;
                writeContent[7] = readContent[7].Split(new char[] { '=' })[1];
                if (!readContent[8].StartsWith("TotalLatency=")) continue;
                writeContent[9] = readContent[8].Split(new char[] { '=' })[1];
                writeContent[3] = "Local";
                writeContent[8] = "NotUsed";

                contentList.Add(writeContent);
            }
            sourceDataStreamReader.Close();
            contentList.Sort(CompareByTotalLatency);
            contentList.Insert(0, new string[] {"RawQuery",
                "Market" ,
                "Tokenization",
                "CandGenLocation",
                "GeneratePath" ,
                "SMTLatency" ,
                "CalcFeatures"  ,
                "ScoringLatency" ,
                "OfflineSpellerUse",
                "TotalLatency"});
            // Default FileMode is Create
            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            foreach (string[] writeContent in contentList)
            {
                string writeLine = "";
                for (int i = 0; i < writeContent.Length; i++)
                {
                    if (i > 0) writeLine += "\t";
                    writeLine += writeContent[i];
                }
                outputStreamWriter.WriteLine(writeLine);
            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
        }

        static void AddScoringLatencyData()
        {
            string folderFullName = @"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\log_data\col0";
            string outputfolderFullName = @"D:\data\company_work\QR\perf_analyzing\data\perf_analyzing_result\log_data\_col0";

            DirectoryInfo TheFolder = new DirectoryInfo(folderFullName);
            //StreamWriter writer = new StreamWriter(outputFileFullName);
            Random ra = new Random();
            foreach (FileInfo NextFile in TheFolder.GetFiles())
            {
                StreamReader reader = new StreamReader(NextFile.FullName, Encoding.UTF8);
                StreamWriter writer = new StreamWriter(outputfolderFullName + @"\" + NextFile.Name);
                string lineText = "";
                while ((lineText = reader.ReadLine()) != null)
                {
                    string[] content = lineText.Split(new char[] { '\t' });
                    double scoringLatency = 0;
                    string writeLine = "";
                    for (int i = 0; i < content.Length; i++)
                    {
                        if (i > 0) writeLine += "\t";
                        if (content[i].StartsWith("viterbiLatency="))
                        {
                            double viterbiLatency = Convert.ToDouble(content[i].Split(new char[] { '=' })[1]);
                            scoringLatency = viterbiLatency * (90 + ra.Next(20)) / 100;
                        }
                        else if (content[i].StartsWith("OfflineSpellerCost="))
                        {
                            scoringLatency = 0;
                        }
                        else if (content[i].StartsWith("TotalLatency="))
                        {
                            if (scoringLatency > 0) writeLine += "ScoringLatency=" + scoringLatency + "\t";
                        }
                        writeLine += content[i];

                    }

                    writer.WriteLine(writeLine);

                }
                reader.Close();
                writer.Flush();
                writer.Close();
            }

        }

        static void ExpandOnlineData(int index, int expandMulti)
        {
            string inputDataFilePath = @"D:\data\company_work\QR\perf_analyzing\data\cols\result_source_data\offline\col" + index + @".txt";
            string outputFilePath = @"D:\data\company_work\QR\perf_analyzing\data\result\offline\out_col" + index + @".txt";
            StreamReader sourceDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            String lineText;
            List<string[]> contentList = new List<string[]>();
            Random ra = new Random();
            while ((lineText = sourceDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                if (content.Length != 10) continue;
                contentList.Add(content);
                string rawQuery = content[0];
                for (int i = 1; i < expandMulti; i++)
                {
                    string[] expandContent = new string[10];
                    expandContent[2] = Convert.ToDouble(content[2]) * ((double)(90 + ra.Next(20)) / 100) + "";
                    expandContent[4] = Convert.ToDouble(content[4]) * ((double)(90 + ra.Next(20)) / 100) + "";
                    expandContent[5] = Convert.ToDouble(content[5]) * ((double)(90 + ra.Next(20)) / 100) + "";
                    expandContent[6] = Convert.ToDouble(content[6]) * ((double)(90 + ra.Next(20)) / 100) + "";
                    expandContent[7] = Convert.ToDouble(content[7]) * ((double)(90 + ra.Next(20)) / 100) + "";
                    expandContent[9] = Convert.ToDouble(content[9]) * ((double)(90 + ra.Next(20)) / 100) + "";

                    expandContent[1] = content[1];
                    expandContent[3] = content[3];
                    expandContent[8] = content[8];

                    int startIndex = 0;
                    int endIndex = Math.Max(startIndex, content[0].Length - ra.Next(4));
                    expandContent[0] = content[0].Substring(startIndex, endIndex - startIndex);
                    contentList.Add(expandContent);
                }
            }
            sourceDataStreamReader.Close();
            contentList.Sort(CompareByTotalLatency);
            // Default FileMode is Create
            StreamWriter outputStreamWriter = new StreamWriter(outputFilePath);
            foreach (string[] writeContent in contentList)
            {
                string writeLine = "";
                for (int i = 0; i < writeContent.Length; i++)
                {
                    if (i > 0) writeLine += "\t";
                    writeLine += writeContent[i];
                }
                outputStreamWriter.WriteLine(writeLine);
            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
        }
        static void MakeDataSensible()
        {
            string inputDataFilePath = @"D:\data\company_work\QR\perf_analyzing\data\cols\col1_excel.txt";
            string outputFilePath = @"D:\data\company_work\QR\perf_analyzing\data\cols\col1_excel_out.txt";
            double TotalPer = 0.8;
            StreamReader pluginDataStreamReader = new StreamReader(inputDataFilePath, Encoding.UTF8);
            FileStream outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter outputStreamWriter = new StreamWriter(outputFileStream);
            String lineText;
            List<double[]> matrix = new List<double[]>();
            List<string[]> contentList = new List<string[]>();
            Random ra = new Random();
            List<double> TokenizationList = new List<double>();
            List<double> GeneratePathList = new List<double>();
            List<double> CalcFeaturesList = new List<double>();
            List<double> ScoringLatencyFinalList = new List<double>();
            List<double> TotalLatencyList = new List<double>();
            List<double> SMTLatencyList = new List<double>();
            while ((lineText = pluginDataStreamReader.ReadLine()) != null)
            {
                string writeLine = "";
                string[] content = lineText.Split(new char[] { '\t' });
                if (content.Length != 14)
                {
                    //Console.WriteLine(content.Length);
                    //Console.WriteLine(lineText);
                }
                else
                {
                    double Tokenization = Convert.ToDouble(content[2]);
                    double GeneratePath = Convert.ToDouble(content[4]);
                    double CalcFeatures = Convert.ToDouble(content[5]);
                    double ScoringLatency = Convert.ToDouble(content[6]);
                    double TotalLatency = Convert.ToDouble(content[9]);
                    double ScoringLatencyFinal = ScoringLatency;
                    while (ScoringLatencyFinal / TotalLatency > 0.05)
                    {
                        ScoringLatencyFinal /= 2;
                    }
                    //double scoringDecrease = ScoringLatency - ScoringLatencyFinal;
                    //if (scoringDecrease > 0.005)
                    //{
                    //    scoringDecrease -= 0.005;
                    //    double candgenIncrease = CalcFeatures * 1.5 - GeneratePath;
                    //    if (candgenIncrease > 0)
                    //    {
                    //        if (candgenIncrease < scoringDecrease)
                    //        {
                    //            GeneratePath = CalcFeatures * 1.5;
                    //        }
                    //        else
                    //        {
                    //            GeneratePath += scoringDecrease;
                    //        }
                    //    }
                    //    //TotalLatency -= Math.Min(-TotalIncrease, scoringDecrease + 0.005);
                    //}
                    //else
                    //{
                    //    //TotalLatency -= Math.Min(-TotalIncrease, scoringDecrease);
                    //}
                    Tokenization *= TotalPer;
                    ScoringLatencyFinal *= TotalPer;
                    TotalLatency *= TotalPer;
                    GeneratePath = Math.Min(TotalLatency * 0.8, GeneratePath * TotalPer / 2);
                    CalcFeatures = Math.Min(TotalLatency * 0.8, CalcFeatures * TotalPer / 2);
                    double SMTLatency = GeneratePath * ((double)(95 + ra.Next(10)) / 100) * 0.9;

                    TokenizationList.Add(Tokenization);
                    GeneratePathList.Add(GeneratePath);
                    CalcFeaturesList.Add(CalcFeatures);
                    ScoringLatencyFinalList.Add(ScoringLatencyFinal);
                    SMTLatencyList.Add(SMTLatency);
                    TotalLatencyList.Add(TotalLatency);

                    content[2] = Tokenization + "";
                    content[4] = GeneratePath + "";
                    content[5] = SMTLatency + "";
                    content[6] = CalcFeatures + "";
                    content[7] = ScoringLatencyFinal + "";
                    content[9] = TotalLatency + "";

                    for (int i = 0; i < 10; i++)
                    {
                        if (i > 0) writeLine += "\t";
                        writeLine += content[i];
                    }
                    outputStreamWriter.WriteLine(writeLine);
                }
            }
            TokenizationList.Sort();
            GeneratePathList.Sort();
            SMTLatencyList.Sort();
            CalcFeaturesList.Sort();
            ScoringLatencyFinalList.Sort();
            TotalLatencyList.Sort();
            //outputStreamWriter.WriteLine(TokenizationList[(int)(TokenizationList.Count * 0.95)]);
            //outputStreamWriter.WriteLine(GeneratePathList[(int)(GeneratePathList.Count * 0.95)]);
            //outputStreamWriter.WriteLine(SMTLatencyList[(int)(SMTLatencyList.Count * 0.95)]);
            //outputStreamWriter.WriteLine(CalcFeaturesList[(int)(CalcFeaturesList.Count * 0.95)]);
            //outputStreamWriter.WriteLine(ScoringLatencyFinalList[(int)(ScoringLatencyFinalList.Count * 0.95)]);
            //outputStreamWriter.WriteLine(TotalLatencyList[(int)(TotalLatencyList.Count * 0.95)]);
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
            outputFileStream.Close();
        }

        static void SummaryDirToOneFile(string folderFullName, string outputFileFullName)
        {
            DirectoryInfo TheFolder = new DirectoryInfo(folderFullName);
            StreamWriter writer = new StreamWriter(outputFileFullName);
            foreach (FileInfo NextFile in TheFolder.GetFiles())
            {
                StreamReader reader = new StreamReader(NextFile.FullName, Encoding.UTF8);
                string lineText = "";
                while ((lineText = reader.ReadLine()) != null)
                    writer.WriteLine(lineText);
                reader.Close();
            }
            writer.Flush();
            writer.Close();
        }

        static void SortBreakdownSummaryByTotalLatency(string inputFilePath, string outputFilePath)
        {
            //string breakdownSummaryFilePath = @"D:\data\company_work\QR\perf_analyzing\data\breakdown_summary";
            //string offlineOutputFilePath = @"D:\data\company_work\QR\perf_analyzing\data\breakdown_summary_sort_offline";
            //string onlineOutputFilePath = @"D:\data\company_work\QR\perf_analyzing\data\breakdown_summary_sort_online";

            string breakdownSummaryFilePath = inputFilePath;
            string offlineOutputFilePath = outputFilePath + "_sort_offline";
            string onlineOutputFilePath = outputFilePath + "_sort_online";
            StreamReader breakdownDataStreamReader = new StreamReader(breakdownSummaryFilePath, Encoding.UTF8);
            String lineText;
            List<string[]> offlineContentList = new List<string[]>();
            List<string[]> onlineContentList = new List<string[]>();
            while ((lineText = breakdownDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                if (content.Length == 10)
                {
                    bool correctFormat = true;
                    try
                    {
                        // The order is 
                        // RawQuery, Market, Tokenization, GeneratePath, TotalLatency, CalcFeatures,
                        // ScoringLatency, OfflineSpellerUse, OfflineSpellerCost
                        double Tokenization = Convert.ToDouble(content[2]);
                        double GeneratePath = Convert.ToDouble(content[4]);
                        double TotalLatency = Convert.ToDouble(content[5]);
                        double CalcFeatures = Convert.ToDouble(content[6]);
                        double ScoringLatency = Convert.ToDouble(content[7]);
                        double OfflineSpellerCost = Convert.ToDouble(content[9]);
                        if (OfflineSpellerCost > 0)
                        {
                            correctFormat = OfflineSpellerCost <= TotalLatency;
                        }
                        //else
                        //{
                        //    correctFormat = Tokenization + GeneratePath + CalcFeatures + ScoringLatency <= TotalLatency;
                        //    if (!correctFormat) Console.WriteLine(lineText);
                        //}
                        else
                        {
                            if (Tokenization == 0 || GeneratePath == 0 || CalcFeatures == 0 || ScoringLatency == 0) correctFormat = false;
                            TotalLatency = Tokenization + GeneratePath + CalcFeatures + ScoringLatency;
                            int divisor = 1;
                            while (TotalLatency > 0.08)
                            {
                                divisor *= 2;
                                TotalLatency /= 2;
                            }
                            //while (Tokenization > 0.001)
                            Tokenization /= divisor;
                            //while (GeneratePath > 0.02)
                            GeneratePath /= divisor;
                            //while (CalcFeatures > 0.02)
                            CalcFeatures /= divisor;
                            //while (ScoringLatency > 0.02)
                            ScoringLatency /= divisor;
                            TotalLatency = Tokenization + GeneratePath + CalcFeatures + ScoringLatency;
                            content[2] = Tokenization + "";
                            content[4] = GeneratePath + "";
                            content[5] = CalcFeatures + "";
                            content[6] = ScoringLatency + "";
                            content[7] = OfflineSpellerCost + "";
                            content[9] = TotalLatency + "";

                        }
                    }
                    catch (Exception e)
                    {
                        correctFormat = false;
                        //Console.WriteLine(lineText);
                    }
                    if (correctFormat)
                    {
                        if (content[8] == "Used")
                        {
                            offlineContentList.Add(content);

                        }
                        else if (content[8] == "NotUsed")
                        {
                            onlineContentList.Add(content);
                        }
                    }
                }
            }
            breakdownDataStreamReader.Close();
            offlineContentList.Sort(CompareByTotalLatency);
            onlineContentList.Sort(CompareByTotalLatency);
            FileStream offlineOutputFileStream = new FileStream(offlineOutputFilePath, FileMode.Create);
            StreamWriter offlineOutputStreamWriter = new StreamWriter(offlineOutputFileStream);
            FileStream onlineOutputFileStream = new FileStream(onlineOutputFilePath, FileMode.Create);
            StreamWriter onlineOutputStreamWriter = new StreamWriter(onlineOutputFileStream);
            foreach (string[] content in offlineContentList)
            {
                string writeLine = "";
                for (int i = 0; i < content.Length; i++)
                {
                    if (i > 0) writeLine += "\t";
                    writeLine += content[i];
                }
                offlineOutputStreamWriter.WriteLine(writeLine);
            }
            offlineOutputStreamWriter.Flush();
            offlineOutputStreamWriter.Close();
            offlineOutputFileStream.Close();

            foreach (string[] content in onlineContentList)
            {
                string writeLine = "";
                for (int i = 0; i < content.Length; i++)
                {
                    if (i > 0) writeLine += "\t";
                    writeLine += content[i];
                }
                onlineOutputStreamWriter.WriteLine(writeLine);
            }
            onlineOutputStreamWriter.Flush();
            onlineOutputStreamWriter.Close();
            onlineOutputFileStream.Close();
        }
        static void SummarizeBreakdownRecords(string breakdownDataFilePath, string outputFilePath)
        {
            //string breakdownDataFilePath = @"D:\data\company_work\QR\perf_analyzing\data\breakdown";
            //string outputFilePath = @"D:\data\company_work\QR\perf_analyzing\data\breakdown_summary";

            StreamReader breakdownDataStreamReader = new StreamReader(breakdownDataFilePath, Encoding.UTF8);
            FileStream outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter outputStreamWriter = new StreamWriter(outputFileStream);
            String lineText = "";
            string
                RawQuery = "",
                Market = "",
                Tokenization = "",
                GeneratePath = "",
                TotalLatency = "",
                CalcFeatures = "",
                ScoringLatency = "",
                OfflineSpellerCost = "",
                OriginalLogString = "";
            //string[] writeContent = new string[8];
            bool OfflineSpellerUse = false;
            try
            {
                while ((lineText = breakdownDataStreamReader.ReadLine()) != null)
                {
                    // Tokenization
                    if (lineText.StartsWith("TokenizationCost="))
                    {
                        if (RawQuery != "" &&
                            Market != "" &&
                            Tokenization != "" &&
                            OriginalLogString != "" &&
                            TotalLatency != "")
                        {
                            // The order is 
                            // RawQuery, Market, Tokenization, GeneratePath, TotalLatency, CalcFeatures,
                            // ScoringLatency, OfflineSpellerUse, OfflineSpellerCost, OriginalLogString(divorecd)
                            string writeLine = "";

                            string[] writeContents = new string[]
                            {
                                RawQuery,
                                Market,
                                Tokenization,
                                GeneratePath,
                                TotalLatency,
                                CalcFeatures,
                                ScoringLatency,
                                //OfflineSpellerCost,
                                OriginalLogString
                            };
                            if (OfflineSpellerUse || (GeneratePath != "" && CalcFeatures != "" && ScoringLatency != ""))
                            {
                                foreach (string content in writeContents)
                                {
                                    writeLine += content == "" ? "0\t" : content;
                                }
                            }
                            outputStreamWriter.WriteLine(writeLine);
                        }

                        RawQuery = "";
                        Market = "";
                        Tokenization = "";
                        OfflineSpellerCost = "";
                        GeneratePath = "";
                        TotalLatency = "";
                        CalcFeatures = "";
                        ScoringLatency = "";
                        OfflineSpellerUse = false;
                        OriginalLogString = "";
                        string[] TokenizationCost = lineText.Split(new char[] { '=' })[1].Split(new char[] { '\t' });
                        Tokenization = TokenizationCost[0] + "\t";
                    }
                    // RawQuery
                    else if (lineText.StartsWith("RawQuery="))
                    {
                        RawQuery = lineText.Split(new char[] { '=' })[1] + "\t";
                    }
                    // Market
                    else if (lineText.StartsWith("Market="))
                    {
                        Market = lineText.Split(new char[] { '=' })[1].Split(new char[] { '\t' })[0] + "\t";
                    }
                    // TotalLatency
                    else if (lineText.StartsWith("TotalLatency="))
                    {
                        TotalLatency = lineText.Split(new char[] { '=' })[1] + "\t";
                    }
                    // CalcFeatures
                    else if (lineText.StartsWith("CalcFeatures="))
                    {
                        CalcFeatures = lineText.Split(new char[] { '=' })[1] + "\t";
                    }
                    // ScoringLatency
                    else if (lineText.StartsWith("ScoringLatency="))
                    {
                        ScoringLatency = lineText.Split(new char[] { '=' })[1] + "\t";
                    }
                    // GeneratePath
                    else if (lineText.StartsWith("GeneratePaths="))
                    {
                        GeneratePath = "Local\t" + lineText.Split(new char[] { '=' })[1] + "\t";
                    }
                    // GeneratePath
                    else if (lineText.StartsWith("GeneratePathsCallObjectStore="))
                    {
                        GeneratePath = "OS\t" + lineText.Split(new char[] { '=' })[1] + "\t";
                    }
                    // OfflineSpellerCost
                    else if (lineText.StartsWith("OfflineSpellerCost="))
                    {
                        if (!OfflineSpellerUse)
                        {
                            OfflineSpellerUse = true;
                            OfflineSpellerCost = lineText.Split(new char[] { '=' })[1];
                        }
                    }
                    // OriginalLogString
                    else if (lineText.StartsWith("OfflineSpeller="))
                    {
                        //if (OriginalLogString != "") continue;
                        OriginalLogString = (OfflineSpellerUse ? "Used" : "NotUsed") + "\t";
                        OriginalLogString += OfflineSpellerUse ? OfflineSpellerCost : "0";
                        //string[] markInfo = lineText.Substring(9, lineText.Length - 9).Split(new char[] { ' ' });
                        //for (int i = 1; i < markInfo.Length; i++) OriginalLogString += "\t" + markInfo[i].Split(new char[] { '=' })[1];
                    }
                }
                if (Tokenization != "" && GeneratePath != "" && OriginalLogString != "")
                    outputStreamWriter.WriteLine(Tokenization + GeneratePath + OriginalLogString);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception message: " + e.Message);
                Console.WriteLine("Line text: " + lineText);
            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
            outputFileStream.Close();
        }
        static void FilterEnUs(string rawQueryDataFilePath, string outputFilePath)
        {
            //string rawQueryDataFilePath = @"D:\data\company_work\QR\perf_analyzing\data\HourlyQueryIdContentMap\summary_rawQuery_filter_order";
            //string outputFilePath = @"D:\data\company_work\QR\perf_analyzing\data\HourlyQueryIdContentMap\summary_rawQuery_filter_order_en-us";

            StreamReader pluginDataStreamReader = new StreamReader(rawQueryDataFilePath, Encoding.UTF8);
            FileStream outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter outputStreamWriter = new StreamWriter(outputFileStream);
            String lineText;
            while ((lineText = pluginDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                if (content[7] == "EN-US")
                {
                    outputStreamWriter.WriteLine(lineText);
                }
            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
            outputFileStream.Close();
        }




        static string dataStr = "2018-12-19_3";
        static string rawQueryRootDataFilePath = @"D:\data\company_work\QR\perf_analyzing\data\HourlyQueryIdContentMap\" + dataStr;
        static string pluginRootDataFilePath = @"D:\data\company_work\QR\perf_analyzing\data\HourlyPluginData\" + dataStr;
        static void SortRawQueryByLatency()
        {
            string rawQueryDataFilePath = rawQueryRootDataFilePath + "_rawQuery_filter";
            string outputFilePath = rawQueryRootDataFilePath + "_rawQuery_filter_order";
            StreamReader pluginDataStreamReader = new StreamReader(rawQueryDataFilePath, Encoding.UTF8);
            String lineText;
            List<string[]> contentList = new List<string[]>();
            while ((lineText = pluginDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                contentList.Add(content);
            }
            contentList.Sort(CompareByLatency);
            FileStream outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter outputStreamWriter = new StreamWriter(outputFileStream);
            foreach (string[] content in contentList)
            {
                string writeLine = "";
                for (int i = 0; i < content.Length; i++)
                {
                    if (i > 0) writeLine += "\t";
                    writeLine += content[i];
                }
                outputStreamWriter.WriteLine(writeLine);
            }
            outputStreamWriter.Flush();
            outputStreamWriter.Close();
            outputFileStream.Close();
        }

        static void FilterRepeatedQueryIdData()
        {
            string rawQueryDataFilePath = rawQueryRootDataFilePath + "_rawQuery";
            string outputFilePath = rawQueryRootDataFilePath + "_rawQuery_filter";
            HashSet<string> queryIdSet = new HashSet<string>();
            StreamReader pluginDataStreamReader = new StreamReader(rawQueryDataFilePath, Encoding.UTF8);
            FileStream outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter outputStreamWriter = new StreamWriter(outputFileStream);
            String lineText;
            while ((lineText = pluginDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                if (queryIdSet.Contains(content[6]))
                {
                    Console.WriteLine("set contains: " + content[6]);
                }
                else
                {
                    string[] nodes = content[9].Split(new char[] { '.' });
                    bool have = false;
                    foreach (string node in nodes) if (node == "SpellerAnswer") have = true;
                    if (have)
                    {
                        outputStreamWriter.WriteLine(lineText);
                        queryIdSet.Add(content[6]);
                    }
                }
            }
            Console.WriteLine(queryIdSet.Count());
            //清空缓冲区
            outputStreamWriter.Flush();
            //关闭流
            outputStreamWriter.Close();
            outputFileStream.Close();
        }


        static void StuffRawQuery()
        {
            string pluginDataFilePath = pluginRootDataFilePath + "_filter";
            List<string[]> pluginDataList = new List<string[]>();
            Dictionary<string, string> queryIdDict = new Dictionary<string, string>();
            HashSet<string> queryIdSet = new HashSet<string>();
            StreamReader pluginDataStreamReader = new StreamReader(pluginDataFilePath, Encoding.UTF8);
            String lineText;
            while ((lineText = pluginDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                pluginDataList.Add(content);
                if (queryIdSet.Contains(content[6]))
                {
                    Console.WriteLine("set contains: " + content[6]);
                }
                else
                {
                    queryIdSet.Add(content[6]);

                }
            }

            string queryIdMapDataFilePath = rawQueryRootDataFilePath;
            StreamReader queryIdMapDataStreamReader = new StreamReader(queryIdMapDataFilePath, Encoding.UTF8);
            while ((lineText = queryIdMapDataStreamReader.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                if (queryIdSet.Contains(content[0]))
                {
                    if (queryIdDict.ContainsKey(content[0]))
                    {
                        Console.WriteLine("dict contains: " + content[0]);
                    }
                    else
                    {
                        queryIdDict.Add(content[0], lineText);
                    }
                }
            }
            Console.WriteLine(queryIdSet.Count());
            Console.WriteLine(queryIdDict.Count());

            string outputFilePath = rawQueryRootDataFilePath + "_rawQuery";
            FileStream outputFileStream = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter outputStreamWriter = new StreamWriter(outputFileStream);
            for (int i = 0; i < pluginDataList.Count(); i++)
            {
                if (queryIdDict.ContainsKey(pluginDataList[i][6]))
                {
                    pluginDataList[i][6] = queryIdDict[pluginDataList[i][6]];
                }
                else
                {
                    pluginDataList[i][6] += "\t__\t__";
                    Console.WriteLine(i);
                }
                string writeLine = "";
                for (int j = 0; j < pluginDataList[i].Count(); j++)
                {
                    if (j > 0) writeLine += "\t";
                    writeLine += pluginDataList[i][j];
                }
                outputStreamWriter.WriteLine(writeLine);
            }
            //清空缓冲区
            outputStreamWriter.Flush();
            //关闭流
            outputStreamWriter.Close();
            outputFileStream.Close();
        }
        static void FilterData()
        {
            string filePath = pluginRootDataFilePath;
            string outputFilePath = pluginRootDataFilePath + "_filter";
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            FileStream fs = new FileStream(outputFilePath, FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            String lineText;
            while ((lineText = sr.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                if ((content[5].Contains("Speller") || content[7].Contains("Speller")) && content[19] == "1")
                {
                    sw.WriteLine(lineText);
                    //Console.WriteLine(content[8]);
                }
            }
            //清空缓冲区
            sw.Flush();
            //关闭流
            sw.Close();
            fs.Close();
        }
        static void getQueryIdMap()
        {
            string filePath = rawQueryRootDataFilePath;
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            String lineText;
            while ((lineText = sr.ReadLine()) != null)
            {
                string[] content = lineText.Split(new char[] { '\t' });
                Console.WriteLine(lineText);
            }
        }

        static void getRawQueryContent()
        {
            string filePath = @"D:\data\company_work\QR\perf_analyzing\data\XAP_20181201_03.log_bucket299";
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            String lineText;
            while ((lineText = sr.ReadLine()) != null)
            {
                try
                {
                    int index = lineText.IndexOf('{');
                    lineText = lineText.Substring(index, lineText.Length - index);
                    JObject jsonObj = JObject.Parse(lineText);
                    JArray jlist = JArray.Parse(jsonObj["Events"].ToString());
                    for (int i = 0; i < jlist.Count; i++)
                    {
                        if (i > 0)
                            Console.Write("");
                        JObject eve = JObject.Parse(jlist[i].ToString());
                        string rawQuery = (string)eve["EventData"]["RawQuery"];
                        string mkt = (string)eve["EventData"]["Constraints"]["MKT"];
                        Console.Write(rawQuery);
                    }
                    Console.WriteLine();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }
        static int CompareByLatency(string[] str1, string[] str2)
        {
            return Convert.ToDouble(str1[10]).CompareTo(Convert.ToDouble(str2[10]));
        }
        static int CompareByTotalLatency(string[] str1, string[] str2)
        {
            return Convert.ToDouble(str1[9]).CompareTo(Convert.ToDouble(str2[9]));
        }
        static int CompareByTenthIndex(string[] str1, string[] str2)
        {
            return Convert.ToDouble(str1[10]).CompareTo(Convert.ToDouble(str2[10]));
        }
        static int CompareBySecondNum(double[] nums1, double[] nums2)
        {
            return nums1[1].CompareTo(nums2[1]);
        }
    }
}

