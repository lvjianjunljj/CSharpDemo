﻿namespace AdlsDemo.FileOperation
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

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
                // The byte array passed in byData is used to accept the data in the FileStream object. 
                // The second parameter is the position in the byte array to start writing data. 
                // It is usually 0, which means that data is written to the array from the file at the beginning of the array. 
                // The last parameter specifies how many characters to read from the file.
                file.Read(byData, 0, 100);
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

        public static IEnumerable<string> FifthMethod(string filePath)
        {
            List<List<string>> data = new List<List<string>>();
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            String line;
            while ((line = sr.ReadLine()) != null)
            {
                yield return line;
            }
            sr.Close();
        }

        public static void SummaryDirToOneFile(string folderAbsolutePath, string outputFileFullName)
        {
            DirectoryInfo TheFolder = new DirectoryInfo(folderAbsolutePath);
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

        public static bool CheckDirectoryExist(string directoryPath)
        {
            return Directory.Exists(directoryPath);

        }

        public static List<string> GetFolderSubPaths(string folderAbsolutePath, ReadType readType, PathType pathType)
        {
            List<string> fileAbsolutePaths = new List<string>();
            DirectoryInfo folder = new DirectoryInfo(folderAbsolutePath);
            FileSystemInfo[] nextFileSystemInfos;
            switch (readType)
            {
                case ReadType.File:
                    // Traversing files
                    nextFileSystemInfos = folder.GetFiles();
                    break;
                case ReadType.Directory:
                    // Traversing folders
                    nextFileSystemInfos = folder.GetDirectories();
                    break;
                default:
                    throw new Exception("Not support folder read type!");
            }
            foreach (FileSystemInfo nextFileSystemInfo in nextFileSystemInfos)
            {
                switch (pathType)
                {
                    case PathType.Absolute:
                        fileAbsolutePaths.Add(nextFileSystemInfo.FullName);
                        break;
                    case PathType.Relative:
                        fileAbsolutePaths.Add(nextFileSystemInfo.Name);
                        break;
                    default:
                        throw new Exception("Not support folder path type!");
                }
            }
            return fileAbsolutePaths;
        }

        public static List<string> GetAllFilePath(string folderAbsolutePath)
        {
            return GetAllFilePathBFS(new List<string>() { folderAbsolutePath });
        }


        private static List<string> GetAllFilePathBFS(List<string> folderAbsolutePaths)
        {
            var folderList = new List<string>();
            var fileList = new List<string>();
            if (folderAbsolutePaths.Count == 0)
            {
                return fileList;
            }
            foreach (var folderAbsolutePath in folderAbsolutePaths)
            {
                folderList.AddRange(GetFolderSubPaths(folderAbsolutePath, ReadType.Directory, PathType.Absolute));
                var subFileList = GetFolderSubPaths(folderAbsolutePath, ReadType.File, PathType.Absolute);
                foreach (var subFile in subFileList)
                {
                    fileList.Add(subFile);
                }
            }
            fileList.AddRange(GetAllFilePathBFS(folderList));
            return fileList;
        }

        public static IEnumerable<string> GetAllFilePaths(string folderAbsolutePath)
        {
            DirectoryInfo folder = new DirectoryInfo(folderAbsolutePath);
            foreach (var fileInfo in folder.GetFiles())
            {
                yield return fileInfo.FullName;
            }

            foreach (var directoryInfo in folder.GetDirectories())
            {
                foreach (var filePath in GetAllFilePaths(directoryInfo.FullName))
                {
                    yield return filePath;
                }
            }

            // We can use this to replace these lines.
            //Directory.EnumerateFiles(folderAbsolutePath, "*", SearchOption.AllDirectories);
        }
    }
    public enum ReadType
    {
        File = 1,
        Directory = 2
    }

    public enum PathType
    {
        Absolute = 1,
        Relative = 2
    }
}
