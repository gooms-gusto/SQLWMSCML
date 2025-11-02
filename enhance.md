your task is only generate the store procedure with name z_Record_preAsnStock to file z_Record_preAsnStock.sql
with detail below
0. read base real table from server db1 to read information tables on below
1. table soure for record is Z_SORTIR_INBOUND_DETAILS
2. table for record or snapshot stock in the table  Z_InventoryBalance_preASN
3. when insert into Z_InventoryBalance_preASN need select data from Z_SORTIR_INBOUND_DETAILS and join into BAS_SKU (ONLY SKU ACTIVE FLAG = 'Y'),BAS_SKU_MULTIWAREHOUSE (to join warehouseID and packId) BAS_PACKAGE_DETAILS ( get for packUom 'PL' to get qty Pallet for insert)
4. stockdate default is now - 1 day
5. SP will execute early morning tommorow, so if today morning is 25 Nov 2025 then stockDate insert is 24 Nov 2025
6. include error handling in store procedure mysql;
7. your table above you can read use db1
8. mapping insert
id = increment table
organizationId = get from Z_SORTIR_INBOUND_DETAILS
warehouseId= get from Z_SORTIR_INBOUND_DETAILS
sortirId= get from Z_SORTIR_INBOUND_DETAILS
sortirLineId= get from Z_SORTIR_INBOUND_DETAILS
customerId= get from Z_SORTIR_INBOUND_DETAILS
palletId= get from Z_SORTIR_INBOUND_DETAILS
ean= get from Z_SORTIR_INBOUND_DETAILS
qtyPallet= get from Z_SORTIR_INBOUND_DETAILS (column qty)
qtyEach = get qtyPallet * qty from BAS_PACKAGE_DETAILS with packUom = 'PL'
packey = get from select result packId from BAS_SKU_MULTIWAREHOUSE
sku= get from Z_SORTIR_INBOUND_DETAILS
stockDate= NOW - 1 DAY
status = when insert set '00'
stopCalculateDate = default null
noteText = default null
udf01 = default null
udf02 = default null
udf03 = default null
udf04 = default null
udf05 = default null
currentVersion=100
oprSeqFlag=2016
addWho = UDFTIMER
addTime =NOW()
editWho = NULL
editTime= NULL
