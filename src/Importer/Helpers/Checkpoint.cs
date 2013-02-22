using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Importer.Helpers
{
    interface ICheckpoint
    {
       void Save(DateTime commitStamp, Queue<Guid> lastProcessedCommits, long totalCommitsRead);

       void Load(out DateTime lastProcessedCommit,
                 out Queue<Guid> commitsQ,
                 out HashSet<Guid> commitsS,
                 out long totalCommitsRead);

       void CleanUp();
    }

    public class NullCheckpoint :ICheckpoint
    {
        public void Save(DateTime commitStamp, Queue<Guid> lastProcessedCommits, long totalCommitsRead)
        {
        }

        public void Load(out DateTime lastProcessedCommit, out Queue<Guid> commitsQ, out HashSet<Guid> commitsS, out long totalCommitsRead)
        {
            lastProcessedCommit = DateTime.MinValue;
            commitsQ = new Queue<Guid>();
            commitsS = new HashSet<Guid>();
            totalCommitsRead = 0;
        }

        public void CleanUp()
        {
        }
    }

    public class Checkpoint : ICheckpoint
    {
        private const string DateTimeFormat = "O";
        private const string CheckpointFileName = @"_failover.chk";

        private readonly string _esServer;
        private readonly string _joServer;

        public Checkpoint(string esServer, string joServer)
        {
            _esServer = esServer;
            _joServer = joServer;
        }

        public  void Save(DateTime commitStamp, Queue<Guid> lastProcessedCommits, long totalCommitsRead)
        {

            var sb = new StringBuilder();

            sb.AppendLine(_esServer);
            sb.AppendLine(_joServer);

            sb.AppendLine(commitStamp.ToString(DateTimeFormat, CultureInfo.InvariantCulture));
            sb.AppendLine(totalCommitsRead.ToString());
            foreach (var commit in lastProcessedCommits)
                sb.AppendLine(commit.ToString());

            File.WriteAllText(CheckpointFileName, sb.ToString());
        }


        public  void Load(out DateTime lastProcessedCommit,
                                out Queue<Guid> commitsQ,
                                out HashSet<Guid> commitsS,
                                out long totalCommitsRead)
        {
            lastProcessedCommit = DateTime.MinValue;
            commitsQ = new Queue<Guid>();
            commitsS = new HashSet<Guid>();
            totalCommitsRead = 0;


            if (!File.Exists(CheckpointFileName))
                return;

            try
            {
                var lines = File.ReadAllLines(CheckpointFileName);

                var esServer = lines[0];
                var joServer = lines[1];

                if (esServer != _esServer)
                    throw new InvalidOperationException(
                        string.Format("Attempt to load checkpoint for wrong EsEvent Store server. " +
                                      "Current server: {0}, in checkpoint: {1}", _esServer, esServer));

                if (joServer != _joServer)
                    throw new InvalidOperationException(
                        string.Format("Attempt to load checkpoint for wrong JOliver server. " +
                                      "Current server: {0}, in checkpoint: {1}", _joServer, joServer));

                lastProcessedCommit = DateTime.ParseExact(lines[2], DateTimeFormat, CultureInfo.InvariantCulture);
                totalCommitsRead = long.Parse(lines[3]);

                Guid commitId;
                foreach (var line in lines.Skip(4))
                {
                    if (string.IsNullOrEmpty(line))
                        continue;

                    commitId = Guid.Parse(line);
                    commitsQ.Enqueue(commitId);
                    commitsS.Add(commitId);
                }

            }
            catch (Exception ex)
            {
                throw new Exception("Error while loading checkpoint", ex);
            }
        }

        public void CleanUp()
        {
            try
            {
                File.Delete(CheckpointFileName);
            }
            catch (Exception)
            {
                
            }
        }
    }
}
