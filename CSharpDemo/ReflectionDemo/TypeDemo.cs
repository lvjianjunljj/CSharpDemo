namespace CSharpDemo.ReflectionDemo
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.ComponentModel;

    class TypeDemo
    {
        public static void MainMethod()
        {
            TypeCaseDemo();
        }

        private static void TypeCaseDemo()
        {
            Console.WriteLine(ParametersCheck("{\"parameters\":[{\"name\":\"hjgjh\",\"defaultType\":\"System.Int32\",\"value\":3}]}"));
        }
        private static bool ParametersCheck(string requestBodyContent)
        {
            JObject requestBodyJObject = JObject.Parse(requestBodyContent);
            JToken parametersValue = requestBodyJObject["parameters"];

            if (parametersValue == null || string.IsNullOrEmpty(parametersValue.ToString()))
            {
                return true;
            }
            try
            {
                JArray parameters = JArray.Parse(parametersValue.ToString());

                foreach (var parameter in parameters)
                {
                    string name = parameter["name"].ToString();
                    StringConverter sc = new StringConverter();
                    Type defaultType = Type.GetType(parameter["defaultType"].ToString());
                    JTokenType type = parameter["value"].Type;
                    bool isSnapshotDate = parameter["isSnapshotDate"] != null && bool.Parse(parameter["isSnapshotDate"].ToString());
                    Console.WriteLine(defaultType);
                    switch (defaultType)
                    {
                        case Type guidType when guidType == typeof(Guid):
                            if (type != JTokenType.Guid) return false;
                            break;
                        case Type intType when intType == typeof(int):
                            Console.WriteLine("int");
                            if (type != JTokenType.Integer) return false;
                            break;
                        case Type floatType when floatType == typeof(float):
                            if (type != JTokenType.Float) return false;
                            break;
                        case Type doubleType when doubleType == typeof(double):
                            if (type != JTokenType.Float) return false;
                            break;
                        case Type dateTimeType when dateTimeType == typeof(DateTime):
                            if (isSnapshotDate && parameter["value"].ToString() != "@@TestDate@@") return false;
                            if (type != JTokenType.Date) return false;
                            break;
                        case Type stringType when stringType == typeof(string):
                            if (type != JTokenType.String) return false;
                            break;
                        case Type boolType when boolType == typeof(bool):
                            if (type != JTokenType.Boolean) return false;
                            break;
                        default:
                            return false;
                    }
                }
            }
            catch (JsonReaderException jre)
            {
                Console.WriteLine(jre.Message);
                return false;
            }
            return true;
        }
    }
}
