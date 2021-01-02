// I think We should get the lib on Nuget, it is better than lib of office is based on the Office app in windows
namespace CSharpDemo.FileOperation
{
    using Microsoft.Office.Interop.Excel;
    using System;
    using System.Collections.Generic;
    using System.IO;

    class ExcelOperation
    {
        public static List<List<string>> ReadFile(string filePath, int itemIndex, int rowStart, int rowEnd, int lineStart, int lineEnd)
        {
            List<List<string>> data = new List<List<string>>();
            _Application app = new Microsoft.Office.Interop.Excel.Application();
            Workbooks wbks = app.Workbooks;
            _Workbook _wbk = wbks.Add(filePath);
            Sheets shs = _wbk.Sheets;
            //i是要取得的sheet的index
            _Worksheet _wsh = (_Worksheet)shs.get_Item(itemIndex);
            //Console.WriteLine(_wsh.Name);
            _wsh.Name = "123";
            for (int i = rowStart; i <= rowEnd; i++)
            {
                List<string> row = new List<string>();
                for (int j = lineStart; j <= lineEnd; j++)
                {
                    //Console.WriteLine(_wsh.Cells[i + 1, j + 1].value);
                    row.Add(_wsh.Cells[i + 1, j + 1].value);
                }
                data.Add(row);
            }
            return data;
        }

        public static List<List<string>> ReadFile(string filePath, string itemIndex, int rowStart, int rowEnd, int lineStart, int lineEnd)
        {
            List<List<string>> data = new List<List<string>>();
            _Application app = new Microsoft.Office.Interop.Excel.Application();
            Workbooks wbks = app.Workbooks;
            _Workbook _wbk = wbks.Add(filePath);
            Sheets shs = _wbk.Sheets;
            //i是要取得的sheet的index
            _Worksheet _wsh = (_Worksheet)shs.get_Item(itemIndex);
            Console.WriteLine(_wsh.Name);
            //_wsh.Name = "123";
            for (int i = rowStart; i <= rowEnd; i++)
            {
                List<string> row = new List<string>();
                for (int j = lineStart; j <= lineEnd; j++)
                {
                    row.Add(_wsh.Cells[i, j].value + "");
                }
                data.Add(row);
            }
            return data;
        }

        public static void WriteFile(string filePath, Dictionary<string, IEnumerable<IEnumerable<string>>> data)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            var xlApplication = new Microsoft.Office.Interop.Excel.Application();
            var workbooks = xlApplication.Workbooks;
            Workbook workbook = workbooks.Add();
            bool addNewSheet = false;
            foreach (var sheetContentPair in data)
            {
                if (addNewSheet)
                {
                    workbook.Sheets.Add();
                }

                // The function "workbook.Sheets.Add()" is to insert a new sheet into the start of a excel work book, 
                // So the index of work book sheets is always "1" so that the worksheet we defined is always the new one we created.
                Worksheet worksheet = workbook.Sheets[1];
                worksheet.Name = sheetContentPair.Key;
                var sheetContent = sheetContentPair.Value;
                int lineIndex = 1;
                foreach (var line in sheetContent)
                {
                    int cellIndex = 1;
                    foreach (var cell in line)
                    {
                        worksheet.Cells[lineIndex, cellIndex] = cell;
                        // Relative doc link: https://www.cnblogs.com/kongbailingluangan/p/5428909.html
                        //worksheet.Cells[lineIndex, cellIndex].Font.Bold = true;
                        cellIndex++;
                        // This is another function to write value into a cell
                        //Range cell = worksheet.get_Range("A1");
                        //cell.Value = "New Value";
                    }
                    lineIndex++;
                }
                addNewSheet = true;
            }
            workbook.SaveAs(filePath);
            workbook.Close();
            xlApplication.Quit();
        }
    }
}
