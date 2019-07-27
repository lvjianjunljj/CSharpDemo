﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.FileOperation
{
    class ReadFile
    {
        static byte[] byData = new byte[100];
        static char[] charData = new char[1000];
        public static void FirstMethod(string filePath)
        {
            try
            {
                FileStream file = new FileStream(filePath, FileMode.Open);
                file.Seek(0, SeekOrigin.Begin);
                file.Read(byData, 0, 100); //byData传进来的字节数组,用以接受FileStream对象中的数据,第2个参数是字节数组中开始写入数据的位置,它通常是0,表示从数组的开端文件中向数组写数据,最后一个参数规定从文件读多少字符.
                Decoder d = Encoding.Default.GetDecoder();
                d.GetChars(byData, 0, byData.Length, charData, 0);
                Console.WriteLine(charData);
                file.Close();
            }
            catch (IOException e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        public static void SecondMethod(string filePath)
        {
            List<string> l = new List<string>();
            //StreamReader sr = new StreamReader(filePath, Encoding.Default);
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            String line;
            while ((line = sr.ReadLine()) != null)
            {

                Console.WriteLine(line);
                l.Add(line);
            }
            sr.Close();
        }
        public static string ThirdMethod(string filePath)
        {
            //return File.ReadAllText(filePath, Encoding.ASCII);
            return File.ReadAllText(filePath, Encoding.UTF8);
        }

        public static List<List<string>> ForthMethod(string filePath)
        {
            List<List<string>> data = new List<List<string>>();
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            String line;
            while ((line = sr.ReadLine()) != null)
            {
                string[] sps = line.Split(new char[] { '\t' });
                List<string> list = new List<string>();
                foreach (string str in sps)
                {
                    list.Add(str);
                }
                data.Add(list);
            }
            sr.Close();
            return data;
        }

        public static void SummaryDirToOneFile(string folderFullName, string outputFileFullName)
        {
            DirectoryInfo TheFolder = new DirectoryInfo(folderFullName);
            // Traversing folders
            //foreach (DirectoryInfo NextFolder in TheFolder.GetDirectories())
            //    Console.WriteLine(NextFolder.Name);
            StreamWriter writer = new StreamWriter(outputFileFullName);
            // Traversing files
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

        public static bool CheckFileExist(string filePath)
        {
            return File.Exists(filePath);
        }
    }
}
