library(data.table)
library(rdleaflogix)
library(DBI)

cn <- dbc("prod2", "integrated")

print("starting job...")


t <- Sys.time()
qry <- "
SELECT
org_uuid,
store_id,
order_id,
pos_order_line_id,
order_time_utc,
customer_id,
phone,
category3,
brand_name,
product_name,
product_sku,
product_qty,
item_subtotal,
item_discount,
item_list_price,
order_subtot,
order_disc
FROM population2
WHERE org_uuid = 'a6cefdc6-0561-48ee-88cf-7e1e47420e41'
AND order_time_utc > (CURRENT_DATE - '91 days'::interval DAY);
"

DT <- setDT(dbGetQuery(cn, qry))
print(nrow(DT))
print(Sys.time() - t)



dbd(cn)

