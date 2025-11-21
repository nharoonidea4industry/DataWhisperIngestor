using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWhisperIngest.Options
{
    public sealed class IngestOptions
    {
        public string IncomingPath { get; set; } = "";
        public string ArchiveFolder { get; set; } = "";
        public string ErrorFolder { get; set; } = "";
        public string SqlConnectionString { get; set; } = "";
        public int PollSeconds { get; set; } = 10;
        public string Model { get; set; } = "gpt-4o-mini";
        public double MinConfidence { get; set; } = 0.55;
    }
}
