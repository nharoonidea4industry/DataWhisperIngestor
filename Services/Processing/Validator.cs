using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWhisperIngest.Services.Processing
{
    public static class Validator
    {
        public static (bool ok, string? why) Row(Dictionary<string, object?> row)
        {
            bool Has(string k) => row.TryGetValue(k, out var v) && v is not null && $"{v}".Trim().Length > 0;

            if (!Has("cust_id")) return (false, "Missing cust_id");
            if (!Has("cust_nm")) return (false, "Missing cust_nm");
            if (!Has("product_id")) return (false, "Missing product_id");
            if (!Has("invoice_nbr")) return (false, "Missing invoice_nbr");
            if (!Has("invoice_dt")) return (false, "Missing invoice_dt");
            if (!DateTime.TryParse($"{row["invoice_dt"]}", out _)) return (false, "Bad invoice_dt");
            if (!Has("sales_amt") || decimal.TryParse($"{row["sales_amt"]}", out var s) == false || s <= 0) return (false, "Sales <= 0");
            if (row.TryGetValue("cost_amt", out var cObj) &&
                decimal.TryParse($"{cObj}", out var cVal) && cVal < 0) return (false, "Cost < 0");

            // Filtering rules (freight/adjustments or $0 lines – if present):
            if (row.TryGetValue("line_type", out var lt) &&
                $"{lt}".Contains("freight", StringComparison.OrdinalIgnoreCase)) return (false, "Freight-only line");
            if (row.TryGetValue("adj_type", out var at) &&
                $"{at}".Contains("rebate", StringComparison.OrdinalIgnoreCase)) return (false, "Rebate/adjustment");

            return (true, null);
        }
    }
}
