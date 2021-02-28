namespace CSharpDemo
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text.RegularExpressions;

    class CommandRunDemo
    {
        public static void MainMethod()
        {
            //RunGitCommandWithOutput();
            //RunGitCommandWithoutOutput();
            OutputGitCommitAuthorsDemo();
        }

        private static void RunGitCommandWithoutOutput()
        {
            var proc1 = new ProcessStartInfo();
            string anyCommand = @"add .";
            proc1.UseShellExecute = true;

            proc1.WorkingDirectory = @"D:\IDEAs\repos\Ibiza";

            //proc1.FileName = @"C:\Windows\System32\cmd.exe";
            proc1.FileName = @"git";
            //proc1.Verb = "runas";
            //proc1.Arguments = "/c " + anyCommand;
            proc1.Arguments = anyCommand;
            proc1.WindowStyle = ProcessWindowStyle.Hidden;
            Process.Start(proc1);
        }

        private static void RunGitCommandWithOutput()
        {
            Process process = new Process();
            process.StartInfo.FileName = "git";
            process.StartInfo.Arguments = "status"; // Note the /c command (*)
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.WorkingDirectory = @"D:\IDEAs\repos\Ibiza";
            process.Start();
            //* Read the output (or the error)
            string output = process.StandardOutput.ReadToEnd();
            Console.WriteLine("Output: ");
            Console.WriteLine(output);
            string err = process.StandardError.ReadToEnd();
            Console.WriteLine("Error: ");
            Console.WriteLine(err);
            process.WaitForExit();
        }

        private static void TestMenthod()
        {
            Process process = new Process();
            process.StartInfo.FileName = "cmd.exe";
            process.StartInfo.Arguments = "/c DIR"; // Note the /c command (*)
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.Start();
            //* Read the output (or the error)
            string output = process.StandardOutput.ReadToEnd();
            Console.WriteLine(output);
            string err = process.StandardError.ReadToEnd();
            Console.WriteLine(err);
            process.WaitForExit();
        }

        private static void OutputGitCommitAuthorsDemo()
        {
            Console.WriteLine("Run OutputCommitAuthorsDemo: ");
            string gitRepoFolderPath = @"D:/IDEAs/repos/Ibiza";
            string gitFileRelativePath = @"Source/Services/DataCop/DataCop.sln";
            foreach (var commitAuthorPair in GetGitCommitAuthors(gitRepoFolderPath, gitFileRelativePath))
            {
                Console.WriteLine($"{commitAuthorPair.Key}\t{commitAuthorPair.Value}");
            }
        }

        private static Regex GitAuthorRegex = new Regex(@"Author: (?<name>.*) <(?<email>.*)>");


        /// <summary>
        /// Get the git commit auth list for a file 
        /// </summary>
        /// <param name="gitRepoFolderPath"></param>
        /// <param name="gitFileRelativePath">The split needs to be '/'</param>
        /// <returns>The list of pair commit auth name and email</returns>
        public static IEnumerable<KeyValuePair<string, string>> GetGitCommitAuthors(string gitRepoFolderPath, string gitFileRelativePath)
        {
            Process process = new Process();
            process.StartInfo.FileName = "git";
            process.StartInfo.Arguments = $"log --all  {gitFileRelativePath}";
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.WorkingDirectory = gitRepoFolderPath;
            process.Start();
            //* Read the output (or the error)
            string outputString = process.StandardOutput.ReadToEnd();
            foreach (var line in outputString.Split('\n'))
            {
                var match = GitAuthorRegex.Match(line.Trim());
                if (match.Success)
                {
                    var name = match.Result("${name}").ToString();
                    var email = match.Result("${email}").ToString();
                    yield return new KeyValuePair<string, string>(name, email);
                }
            }
            process.WaitForExit();
        }
    }
}
