namespace AdlsDemo.FileOperation
{
    using System.Collections.Generic;
    using System.IO;

    public class WriteFile
    {
        public static void FirstMethod(string filePath, string content)
        {
            FileStream fs = new FileStream(filePath, FileMode.Create);
            // Get byte array
            byte[] data = System.Text.Encoding.Default.GetBytes(content);
            // Start to write
            fs.Write(data, 0, data.Length);
            // Clear buffer
            fs.Flush();
            // Close stream
            fs.Close();
        }

        public static void SecondMethod(string filePath, string content)
        {
            //FileStream fs = new FileStream(filePath, FileMode.Create);
            //StreamWriter sw = new StreamWriter(fs);
            // Maybe just function using StreamWriter(string path) is better.
            StreamWriter sw = new StreamWriter(filePath);
            // Start writing
            sw.Write(content);
            // Clear buffer
            sw.Flush();
            // Close stream
            sw.Close();
            //fs.Close();
        }

        public static void Save(string filePath, IEnumerable<string> content)
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
            // Clear buffer
            sw.Flush();
            // Close stream
            sw.Close();
        }

        public static void Save(string filePath, IEnumerable<IEnumerable<string>> content)
        {
            FileStream fs = new FileStream(filePath, FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            // Start writing
            foreach (IEnumerable<string> list in content)
            {
                sw.WriteLine(string.Join("\t", list));
            }
            // Clear buffer
            sw.Flush();
            // Close stream
            sw.Close();
            fs.Close();
        }
    }
}
