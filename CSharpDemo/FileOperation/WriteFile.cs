using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.FileOperation
{
    public class WriteFile
    {
        public static void FirstMethod(string filePath, string content)
        {
            FileStream fs = new FileStream(filePath, FileMode.Create);
            //获得字节数组
            byte[] data = System.Text.Encoding.Default.GetBytes(content);
            //开始写入
            fs.Write(data, 0, data.Length);
            //清空缓冲区、关闭流
            fs.Flush();
            fs.Close();
        }

        public static void SecondMethod(string filePath, string content)
        {
            //FileStream fs = new FileStream(filePath, FileMode.Create);
            //StreamWriter sw = new StreamWriter(fs);
            // Maybe just function using StreamWriter(string path) is better.
            StreamWriter sw = new StreamWriter(filePath);
            //开始写入
            sw.Write(content);
            //清空缓冲区
            sw.Flush();
            //关闭流
            sw.Close();
            //fs.Close();
        }

        public static void Save(string filePath, List<string> content)
        {
            //FileStream fs = new FileStream(filePath, FileMode.Create);
            //StreamWriter sw = new StreamWriter(fs);
            // Maybe just function using StreamWriter(string path) is better.
            StreamWriter sw = new StreamWriter(filePath);
            // Start to write
            foreach (string line in content)
            {
                sw.WriteLine(line);
            }
            // 清空缓冲区
            sw.Flush();
            //关闭流
            sw.Close();
        }

        public static void Save(string filePath, List<List<string>> content)
        {
            FileStream fs = new FileStream(filePath, FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            //开始写入
            foreach (List<string> list in content)
            {
                string line = "";
                for (int i = 0; i < list.Count; i++)
                {
                    if (i > 0)
                    {
                        line += "\t";
                    }
                    line += list[i];
                }
                sw.WriteLine(line);
            }
            //清空缓冲区
            sw.Flush();
            //关闭流
            sw.Close();
            fs.Close();
        }
    }
}
