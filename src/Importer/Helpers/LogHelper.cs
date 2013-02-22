using System;
using System.IO;

namespace Importer.Helpers
{
    public class LogHelper
    {
        public static void Log()
        {
            Log(string.Empty);
        }

        public static void Log(string text, params object[] args)
        {
            try
            {
                Console.WriteLine(text, args);
                File.AppendAllText("log.txt",
                                   string.Format("{0}: {1}{2}",
                                                 DateTime.UtcNow.ToString("G"),
                                                 string.Format(text, args),
                                                 Environment.NewLine));
            }
            catch (Exception)
            {
                Console.WriteLine("couldnt write logs to file log.txt ...");
            }
        }
    }
}
