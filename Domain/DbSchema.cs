namespace DataWhisperIngest.Domain;

public static class DbSchema
{
    public static readonly List<TableSchema> Tables = new()
    {
        new TableSchema("stgMain", new()
        {
            new ColumnSchema("stgMainId","int IDENTITY", Array.Empty<string>(), Insertable:false),
            new ColumnSchema("IngestFileId","int", new[] { "file id", "ingest file id" }),

            // Customer-ish
            new ColumnSchema("entity_cust_id","nvarchar(255)", new[] { "external customer id","entity customer id","customer no","customer id","cust id","customer number","client id","client number","account id","account number","party id","party number","subscriber id","membership id","customer code","cust ref","customer reference","cust_no","cust_num","acct_id","acct_no"}),
            new ColumnSchema("cust_nm","nvarchar(255)", new[] { "customer name","cust name","name","client name","account name","party name","subscriber name","member name","acct_nm","cust_full_name","customer fullname" }),
            new ColumnSchema("cust_grp_1","nvarchar(255)", new[] { "customer group 1","cust group 1","group 1","segment 1","tier 1","class 1","cluster 1","category 1" }),
            new ColumnSchema("cust_grp_2","nvarchar(255)", new[] { "customer group 2","cust group 2","group 2","segment 2","tier 2","class 2","cluster 2","category 2" }),

            // Location-ish
            new ColumnSchema("entity_loctn_id","nvarchar(255)", new[] { "external location id","entity location id","location id","loctn id","site id","branch id","store id","warehouse id","facility id" }),
            new ColumnSchema("loctn_nm","nvarchar(255)", new[] { "location name","loctn name","name","site name","branch name","store name","warehouse name","facility name" }),
            new ColumnSchema("loctn_grp_1","nvarchar(255)", new[] { "location group 1","loctn group 1","group 1","region 1","area 1","zone 1","cluster 1","territory 1" }),
            new ColumnSchema("loctn_grp_2","nvarchar(255)", new[] { "location group 2","loctn group 2","group 2","region 2","area 2","zone 2","cluster 2","territory 2" }),

            // Product-ish
            new ColumnSchema("entity_product_id","nvarchar(255)", new[] { "external product id","entity product id","product id","item id","sku","stock keeping unit","material id","article id","part number","pn","ERP_ITEM_ID" }),
            new ColumnSchema("product_nm","nvarchar(255)", new[] { "product name","item name","name","item","sku name","material name","article name","part name","description","ITEM_DESC_SHORT" }),
            new ColumnSchema("product_grp_1","nvarchar(255)", new[] { "product group 1","group 1","product group","category 1","class 1","segment 1","line 1","division 1","NAP_CATEGORY_DESC" }),
            new ColumnSchema("product_grp_2","nvarchar(255)", new[] { "product group 2","group 2","category 2","class 2","segment 2","line 2","division 2","NAP_SECTION_DESC" }),

            // Sales-ish
            new ColumnSchema("invoice_dt","datetime2(0)", new[] { "invoice date","inv date","billing date","bill date","document date","doc date" }),
            new ColumnSchema("invoice_nbr","nvarchar(255)", new[] { "invoice number","invoice no","inv no","bill no","billing number","document number","doc no" }),
            new ColumnSchema("bill_qty","int", new[] { "billed quantity","quantity","qty ord","qty","order qty","ordered quantity","shipped qty","delivered qty" }),
            new ColumnSchema("unit_cost","float", new[] { "unit cost","cost per unit","price cost","per item cost","item cost" }),
            new ColumnSchema("unit_sls_prc","float", new[] { "unit sales price","unit price","price","selling price","list price","retail price","per item price" }),
            new ColumnSchema("cost_amt","float", new[] { "cost amount","cost","total cost","extended cost","amt cost" }),
            new ColumnSchema("sales_amt","float", new[] { "sales amount","revenue","sales","turnover","gross sales","net sales","amt sales" }),
            new ColumnSchema("Delivery","nvarchar(255)", new[] { "delivery","shipment","dispatch","fulfillment","ship info" }),
            new ColumnSchema("Prc_Md","nvarchar(255)", new[] { "price mode","pricing mode","prc md","pricing method","price type" }),

            // Audit/Helpers
            new ColumnSchema("crt_dt","datetime2(0)", new[] { "created date","create date" }),
            new ColumnSchema("upd_dt","datetime2(0)", new[] { "updated date","update date" }),
            new ColumnSchema("ExcelRowNumber","int", Array.Empty<string>())
        })
    };

    public static ColumnSchema? GetColumn(string table, string column) =>
        Tables.FirstOrDefault(t => t.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
              ?.Columns.FirstOrDefault(c => c.Name.Equals(column, StringComparison.OrdinalIgnoreCase));
}
