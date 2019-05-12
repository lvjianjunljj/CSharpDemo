using Microsoft.Office.Interop.Excel;
using System;
using System.Collections.Generic;

// The lib of office is based on the Office app in windows.Most of my VM do not setup the Office. 
// But I think I can get the lib on Nuget, it is great
namespace CSharpDemo.FileOperation
{
    class ExcelOperation
    {
        public List<List<string>> readFile(string filePath, int itemIndex, int rowStart, int rowEnd, int lineStart, int lineEnd)
        {
            List<List<string>> data = new List<List<string>>();
            _Application app = new Microsoft.Office.Interop.Excel.Application();
            Workbooks wbks = app.Workbooks;
            _Workbook _wbk = wbks.Add(filePath);
            Sheets shs = _wbk.Sheets;
            //i是要取得的sheet的index
            _Worksheet _wsh = (_Worksheet)shs.get_Item("UFPrn20180626141305");
            Console.WriteLine(_wsh.Name);
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

        public List<List<string>> readFile(string filePath, string itemIndex, int rowStart, int rowEnd, int lineStart, int lineEnd)
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
    }
}
