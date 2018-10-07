//  Copyright (C) 2015, The Duplicati Team
//  http://www.duplicati.com, info@duplicati.com
//
//  This library is free software; you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as
//  published by the Free Software Foundation; either version 2.1 of the
//  License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful, but
//  WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//  Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this library; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Duplicati.Server.WebServer.RESTMethods
{
    public class LogData : IRESTMethodGET, IRESTMethodDocumented
    {
        public void GET(string key, RequestInfo info)
        {
            if ("poll".Equals(key, StringComparison.OrdinalIgnoreCase))
            {
                var input = info.Request.QueryString;
                var level_str = input["level"].Value ?? "";
                var id_str = input["id"].Value ?? "";

                int pagesize;
                if (!int.TryParse(info.Request.QueryString["pagesize"].Value, out pagesize))
                    pagesize = 100;

                pagesize = Math.Max(1, Math.Min(500, pagesize));

                Library.Logging.LogMessageType level;
                long id;

                long.TryParse(id_str, out id);
                Enum.TryParse(level_str, true, out level);

                info.OutputOK(Program.LogHandler.AfterID(id, level, pagesize));
            }
            else
            {

                List<Dictionary<string, object>> res = null;
                Program.DataConnection.ExecuteWithCommand(x =>
                {
                    res = DumpTable(x, "ErrorLog", "Timestamp", info.Request.QueryString["offset"].Value, info.Request.QueryString["pagesize"].Value);
                });

                info.OutputOK(res);
            }
        }


        public static List<Dictionary<string, object>> DumpTable(System.Data.IDbCommand cmd, string tablename, string pagingfield, string offset_str, string pagesize_str)
        {
            var result = new List<Dictionary<string, object>>();

            long pagesize;
            if (!long.TryParse(pagesize_str, out pagesize))
                pagesize = 100;

            pagesize = Math.Max(10, Math.Min(500, pagesize));

            cmd.CommandText = "SELECT * FROM \"" + tablename + "\"";
            long offset = 0;
            if (!string.IsNullOrWhiteSpace(offset_str) && long.TryParse(offset_str, out offset) && !string.IsNullOrEmpty(pagingfield))
            {
                var p = cmd.CreateParameter();
                p.Value = offset;
                cmd.Parameters.Add(p);

                cmd.CommandText += " WHERE \"" + pagingfield + "\" < ?";
            }

            if (!string.IsNullOrEmpty(pagingfield))
                cmd.CommandText += " ORDER BY \"" + pagingfield + "\" DESC";
            cmd.CommandText += " LIMIT " + pagesize.ToString();

            using(var rd = cmd.ExecuteReader())
            {
                var names = new List<string>();
                for(var i = 0; i < rd.FieldCount; i++)
                    names.Add(rd.GetName(i));

                while (rd.Read())
                {
                    var dict = new Dictionary<string, object>();
                    for(int i = 0; i < names.Count; i++)
                        dict[names[i]] = rd.GetValue(i);

                    result.Add(dict);                                    
                }
            }

            return result;
        }

        /// <summary>
        /// More pretty version of DumpTable. It parses the "Message" string extracting
        /// the most useful variables so they can be fetched from the dictionary by name
        /// </summary>
        public static List<Dictionary<String, Object>> GetTable(System.Data.IDbCommand cmd, string tablename, string pagingfield, string offset_str, string pagesize_str)
        {
            var messages = LogData.DumpTable(cmd, tablename, pagingfield, offset_str, pagesize_str);
            foreach (var message in messages)
            {
                // Variable names to extract
                message.TryGetValue("Message", out object msg);

                // Split up the object
                var allResults = msg.ToString().Split('\n');
                // Get the usable "First level" attributes beloning to the backup
                var BackupResults = allResults
                    .Where(x => x.Substring(0, 1) != " "
                           && x.Substring(0, 1) != "."
                           && x.Substring(0, 1) != "]"
                           && x.Substring(x.Length - 1, 1) != "["
                           && x.Substring(x.Length - 1, 1) != "]"
                           && x.Substring(x.Length - 4, 4) != "null"
                           && x.Substring(x.Length - 1, 1) != ":"
                           && x.Substring(0, 4) != "Read"
                          ).ToDictionary(
                              x => x.Split(new string[] { ": " }, StringSplitOptions.None).First().Trim(),
                              y => y.Split(new string[] { ": " }, StringSplitOptions.None).Last().Trim()
                             );

                // Get the index of each "sub element"
                var CompactResultsIndex = Array.FindIndex(allResults, x => x.Contains("CompactResults"));
                var RepairResultsIndex = Array.FindIndex(allResults, x => x.Contains("RepairResults"));
                var DeleteResultsIndex = Array.FindIndex(allResults, x => x.Contains("DeleteResults"));
                var TestResultsIndex = Array.FindIndex(allResults, x => x.Contains("TestResults"));
                var BackendStatisticsIndex = Array.FindIndex(allResults, x => x.Contains("BackendStatistics"));

                string MainOperation = "Unknown";
                if (BackupResults.TryGetValue("MainOperation", out MainOperation)) {
                    message.Add("MainOperation", MainOperation);
                }
                string ParsedResult = "Unknown";
                if (BackupResults.TryGetValue("ParsedResult", out ParsedResult))
                {
                    message.Add("ParsedResult", ParsedResult);
                }
                message.Add("BackupResults", BackupResults);

                // Get the sub element attributes
                var CompactResults = allResults
                    .Skip(CompactResultsIndex)
                    .Take(DeleteResultsIndex - CompactResultsIndex)
                    .Where(x => x.Length > 4
                           && x.Substring(4, 1) != " "
                           && x.Substring(0, 1) != "."
                           && x.Substring(0, 1) != "]"
                           && x.Substring(x.Length - 1, 1) != "["
                           && x.Substring(x.Length - 1, 1) != "]"
                           && x.Substring(x.Length - 4, 4) != "null"
                           && x.Substring(x.Length - 1, 1) != ":"
                          ).ToDictionary(
                              x => x.Split(new string[] { ": " }, StringSplitOptions.None).First().Trim(),
                              y => y.Split(new string[] { ": " }, StringSplitOptions.None).Last().Trim()
                         );
                
                var DeleteResults = allResults
                    .Skip(DeleteResultsIndex)
                    .Take(RepairResultsIndex - DeleteResultsIndex)
                    .Where(x => x.Length > 4
                           && x.Substring(4, 1) != " "
                           && x.Substring(0, 1) != "]"
                           && x.Substring(x.Length - 1, 1) != "["
                           && x.Substring(x.Length - 1, 1) != "]"
                           && x.Substring(x.Length - 4, 4) != "null"
                           && x.Substring(x.Length - 1, 1) != ":"
                           && x.Substring(4, 3) != "Key"
                           && x.Substring(4, 5) != "Value"
                          ).ToDictionary(
                              x => x.Split(new string[] { ": " }, StringSplitOptions.None).First().Trim(),
                              y => y.Split(new string[] { ": " }, StringSplitOptions.None).Last().Trim()
                             );
                
                var TestResults = allResults
                    .Skip(TestResultsIndex)
                    .Take(BackendStatisticsIndex - TestResultsIndex)
                    .Where(x => x.Length > 4
                           && x.Substring(4, 1) != " "
                           && x.Substring(0, 1) != "]"
                           && x.Substring(x.Length - 1, 1) != "["
                           && x.Substring(x.Length - 1, 1) != "]"
                           && x.Substring(x.Length - 4, 4) != "null"
                           && x.Substring(x.Length - 1, 1) != ":"
                           && x.Substring(4, 3) != "Key"
                           && x.Substring(4, 5) != "Value"
                          ).ToDictionary(
                              x => x.Split(new string[] { ": " }, StringSplitOptions.None).First().Trim(),
                              y => y.Split(new string[] { ": " }, StringSplitOptions.None).Last().Trim()
                             );

                message.Add("CompactResults", CompactResults);
                message.Add("DeleteResults", DeleteResults);
                message.Add("TestResults", TestResults);
            }
            return messages;
        }

        public string Description { get { return "Retrieves system log data"; } }

        public IEnumerable<KeyValuePair<string, Type>> Types
        {
            get
            {
                return new KeyValuePair<string, Type>[] {
                    new KeyValuePair<string, Type>(HttpServer.Method.Get, typeof(Dictionary<string, string>[])),
                };
            }
        }
    }
}

