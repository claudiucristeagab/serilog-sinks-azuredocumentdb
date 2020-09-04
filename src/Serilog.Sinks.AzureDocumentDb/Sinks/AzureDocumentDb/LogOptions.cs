using System.Collections.Generic;

namespace Serilog.Sinks.AzureDocumentDB.Sinks.AzureDocumentDb
{
    public class LogOptions
    {
        public List<StandardLogs> ExcludedStandardLogElements = new List<StandardLogs>();
        public List<string> IncludedProperties = new List<string>();

        internal static string MapStandardLogToKey(StandardLogs standardLog)
        {
            switch (standardLog)
            {
                case StandardLogs.Id:
                    return StandardLogKeys.Id;
                case StandardLogs.Message:
                    return StandardLogKeys.Message;
                case StandardLogs.MessageTemplate:
                    return StandardLogKeys.MessageTemplate;
                case StandardLogs.Level:
                    return StandardLogKeys.Level;
                case StandardLogs.TimeStamp:
                    return StandardLogKeys.TimeStamp;
                case StandardLogs.Exception:
                    return StandardLogKeys.Exception;
                case StandardLogs.Properties:
                    return StandardLogKeys.Properties;
                case StandardLogs.LogEvent:
                    return StandardLogKeys.LogEvent;
                default:
                    return null;
            }
        }
    }

    internal class StandardLogKeys
    {
        public const string Id = "Id";
        public const string Message = "Message";
        public const string MessageTemplate = "MessageTemplate";
        public const string Level = "Level";
        public const string TimeStamp = "TimeStamp";
        public const string Exception = "Exception";
        public const string Properties = "Properties";
        public const string LogEvent = "LogEvent";
    }

    public enum StandardLogs
    {
        Id,
        Message,
        MessageTemplate,
        Level,
        TimeStamp,
        Exception,
        Properties,
        LogEvent
    }

    
}
