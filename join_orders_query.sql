--
-- Database connection details from .env file
-- Database Type: mysql
-- Host: omahkudewe.asia
-- Port: 63306
-- Username: middleware
-- Database: wms_cml
--
-- Execute with: mysql -h omahkudewe.asia -P 63306 -u middleware -p wms_cml < join_orders_query.sql
--

SET NAMES 'utf8mb4';
USE wms_cml;

-- ========================================
-- TABLE JOIN QUERY: T_DOC_ORDER_HEADER + DOC_ORDER_DETAILS
-- ========================================

-- 1. BASIC JOIN - Order Header with Details
SELECT
    -- Header fields
    h.organizationId,
    h.orderNo,
    h.wmsOrderNo,
    h.omsOrderNo,
    h.customerId,
    h.customerDescr1,
    h.orderStatus,
    h.orderType,
    h.priority,
    h.warehouseId,

    -- Detail fields
    d.warehouseId AS detail_warehouse_id,
    d.orderLineNo,
    d.sku,
    d.lineStatus,
    d.lotNum,
    d.location,
    d.pickZone,
    d.traceId,

    -- Quantity fields
    d.qtyOrdered,
    d.qtyAllocated,
    d.qtyPicked,
    d.qtyShipped,

    -- Timestamps
    h.addTime AS order_created_time,
    d.addTime AS detail_created_time,
    h.expectedShipmentTime1,
    h.expectedShipmentTime2

FROM T_DOC_ORDER_HEADER h
INNER JOIN DOC_ORDER_DETAILS d
    ON h.organizationId = d.organizationId
    AND h.orderNo = d.orderNo
ORDER BY h.orderNo, d.orderLineNo;

-- ========================================
-- 2. COMPLETE ORDER INFORMATION
-- ========================================

SELECT
    -- Order Header Information
    h.organizationId,
    h.orderNo,
    h.wmsOrderNo,
    h.omsOrderNo,
    h.reference1,
    h.reference2,
    h.reference3,
    h.reference4,
    h.reference5,
    h.parentOrderNo,
    h.splitFlag,
    h.expressNo,
    h.waybillNo,

    -- Customer Information
    h.customerId,
    h.customerDescr1,
    h.customerDescr2,
    h.customerContact,
    h.customerTel1,
    h.customerTel2,

    -- Order Status and Type
    h.orderStatus,
    h.orderType,
    h.priority,
    h.warehouseId,
    h.createSource,

    -- Shipping Information
    h.incoterm,
    h.shipmentType,
    h.carrierId,
    h.carrierName,
    h.carrierContact,
    h.carrierTel1,

    -- Address Information
    h.receiverName,
    h.receiverAddress1,
    h.receiverAddress2,
    h.receiverCity,
    h.receiverProvince,
    h.receiverCountry,
    h.receiverZip,
    h.receiverContact,
    h.receiverTel1,
    h.receiverTel2,

    -- Order Line Details
    d.orderLineNo,
    d.sku,
    d.skuDescr1,
    d.skuDescr2,
    d.lineStatus,
    d.lotNum,
    d.lotAtt01,
    d.lotAtt02,
    d.lotAtt03,
    d.lotAtt04,
    d.lotAtt05,

    -- Location and Allocation
    d.pickZone,
    d.location,
    d.traceId,
    d.packId,
    d.packUom,

    -- Quantities
    d.qtyOrdered,
    d.qtySoftAllocated,
    d.qtyAllocated,
    d.qtyPicked,
    d.qtyShipped,
    d.qtyOrdered_each,
    d.qtyAllocated_each,
    d.qtyPicked_each,
    d.qtyShipped_each,

    -- Pricing
    d.unitPrice,
    d.extendedPrice,

    -- Dates and Timestamps
    h.addTime AS order_created_date,
    h.editTime AS order_modified_date,
    h.expectedShipmentTime1,
    h.expectedShipmentTime2,
    h.actualShipmentTime,
    d.addTime AS line_created_date,
    d.editTime AS line_modified_date,

    -- Order Totals
    h.totalLines,
    h.totalSkuCount,
    h.totalCubic,
    h.totalGrossWeight,
    h.totalNetWeight,
    h.totalPrice,

    -- Notes and References
    h.noteText AS order_notes,
    d.noteText AS line_notes

FROM T_DOC_ORDER_HEADER h
LEFT JOIN DOC_ORDER_DETAILS d
    ON h.organizationId = d.organizationId
    AND h.orderNo = d.orderNo
WHERE h.orderStatus IN ('OP', 'PR', 'SH')  -- Open, Picking, Shipped
ORDER BY h.orderNo, d.orderLineNo;

-- ========================================
-- 3. ORDER SUMMARY WITH AGGREGATES
-- ========================================

SELECT
    -- Order Header Information
    h.organizationId,
    h.orderNo,
    h.wmsOrderNo,
    h.omsOrderNo,
    h.customerId,
    h.customerDescr1,
    h.orderStatus,
    h.orderType,
    h.priority,
    h.warehouseId,

    -- Order Dates
    h.addTime AS order_created_date,
    h.expectedShipmentTime1,
    h.expectedShipmentTime2,
    h.actualShipmentTime,

    -- Customer and Shipping
    h.receiverName,
    h.receiverCity,
    h.receiverProvince,
    h.carrierName,
    h.shipmentType,

    -- Aggregated Line Information
    COUNT(d.orderLineNo) AS total_lines,
    COUNT(DISTINCT d.sku) AS unique_skus,
    COALESCE(SUM(d.qtyOrdered), 0) AS total_qty_ordered,
    COALESCE(SUM(d.qtyAllocated), 0) AS total_qty_allocated,
    COALESCE(SUM(d.qtyPicked), 0) AS total_qty_picked,
    COALESCE(SUM(d.qtyShipped), 0) AS total_qty_shipped,

    -- Order Totals from Header
    h.totalLines AS header_total_lines,
    h.totalSkuCount,
    h.totalCubic,
    h.totalGrossWeight,
    h.totalNetWeight,
    h.totalPrice,

    -- Completion Status
    CASE
        WHEN COUNT(d.orderLineNo) = 0 THEN 'No Lines'
        WHEN SUM(CASE WHEN d.lineStatus = 'SH' THEN 1 ELSE 0 END) = COUNT(d.orderLineNo) THEN 'Fully Shipped'
        WHEN SUM(CASE WHEN d.lineStatus IN ('OP', 'PR', 'PK') THEN 1 ELSE 0 END) > 0 THEN 'Partially Shipped'
        ELSE 'Not Started'
    END AS shipment_status,

    -- Percentage Complete
    CASE
        WHEN COUNT(d.orderLineNo) = 0 THEN 0
        ELSE ROUND((SUM(CASE WHEN d.lineStatus = 'SH' THEN 1 ELSE 0 END) * 100.0) / COUNT(d.orderLineNo), 2)
    END AS percent_shipped

FROM T_DOC_ORDER_HEADER h
LEFT JOIN DOC_ORDER_DETAILS d
    ON h.organizationId = d.organizationId
    AND h.orderNo = d.orderNo
GROUP BY
    h.organizationId, h.orderNo, h.wmsOrderNo, h.omsOrderNo,
    h.customerId, h.customerDescr1, h.orderStatus, h.orderType,
    h.priority, h.warehouseId, h.addTime, h.expectedShipmentTime1,
    h.expectedShipmentTime2, h.actualShipmentTime, h.receiverName,
    h.receiverCity, h.receiverProvince, h.carrierName, h.shipmentType,
    h.totalLines, h.totalSkuCount, h.totalCubic, h.totalGrossWeight,
    h.totalNetWeight, h.totalPrice
ORDER BY h.orderNo;

-- ========================================
-- 4. ORDER ALLOCATION STATUS
-- ========================================

SELECT
    h.organizationId,
    h.orderNo,
    h.customerId,
    h.customerDescr1,
    h.orderStatus,
    h.warehouseId,
    h.expectedShipmentTime1,

    -- Line by Line Status
    d.orderLineNo,
    d.sku,
    d.lineStatus,
    d.location,
    d.pickZone,
    d.traceId,

    -- Allocation Status
    d.qtyOrdered,
    d.qtySoftAllocated,
    d.qtyAllocated,
    d.qtyPicked,
    d.qtyShipped,

    -- Allocation Completion
    CASE
        WHEN d.qtyOrdered = 0 THEN 'No Quantity'
        WHEN d.qtyShipped >= d.qtyOrdered THEN 'Complete'
        WHEN d.qtyPicked >= d.qtyOrdered THEN 'Picked - Not Shipped'
        WHEN d.qtyAllocated >= d.qtyOrdered THEN 'Allocated - Not Picked'
        WHEN d.qtySoftAllocated > 0 THEN 'Soft Allocated'
        ELSE 'Not Allocated'
    END AS allocation_status,

    -- Remaining to Allocate
    (d.qtyOrdered - d.qtyAllocated) AS remaining_to_allocate,

    -- Remaining to Pick
    (d.qtyAllocated - d.qtyPicked) AS remaining_to_pick,

    -- Remaining to Ship
    (d.qtyPicked - d.qtyShipped) AS remaining_to_ship

FROM T_DOC_ORDER_HEADER h
INNER JOIN DOC_ORDER_DETAILS d
    ON h.organizationId = d.organizationId
    AND h.orderNo = d.orderNo
WHERE h.orderStatus NOT IN ('CL', 'CA')  -- Not Closed or Cancelled
ORDER BY h.priority DESC, h.expectedShipmentTime1, h.orderNo, d.orderLineNo;

-- ========================================
-- 5. PICKING ZONE ANALYSIS
-- ========================================

SELECT
    d.pickZone,
    h.warehouseId,
    COUNT(DISTINCT h.orderNo) AS total_orders,
    COUNT(d.orderLineNo) AS total_lines,
    COUNT(DISTINCT d.sku) AS unique_skus,
    SUM(d.qtyOrdered) AS total_qty_ordered,
    SUM(d.qtyAllocated) AS total_qty_allocated,
    SUM(d.qtyPicked) AS total_qty_picked,
    SUM(d.qtyShipped) AS total_qty_shipped,

    -- Zone Efficiency
    CASE
        WHEN SUM(d.qtyOrdered) = 0 THEN 0
        ELSE ROUND((SUM(d.qtyPicked) * 100.0) / SUM(d.qtyOrdered), 2)
    END AS picking_completion_percentage,

    -- Allocation Efficiency
    CASE
        WHEN SUM(d.qtyOrdered) = 0 THEN 0
        ELSE ROUND((SUM(d.qtyAllocated) * 100.0) / SUM(d.qtyOrdered), 2)
    END AS allocation_percentage

FROM T_DOC_ORDER_HEADER h
INNER JOIN DOC_ORDER_DETAILS d
    ON h.organizationId = d.organizationId
    AND h.orderNo = d.orderNo
WHERE d.pickZone IS NOT NULL
    AND h.orderStatus NOT IN ('CL', 'CA')
GROUP BY d.pickZone, h.warehouseId
ORDER BY d.pickZone, total_qty_ordered DESC;

-- ========================================
-- 6. CUSTOMER ORDER ANALYSIS
-- ========================================

SELECT
    h.customerId,
    h.customerDescr1,
    h.warehouseId,

    -- Order Counts
    COUNT(DISTINCT h.orderNo) AS total_orders,
    COUNT(DISTINCT CASE WHEN h.orderStatus = 'OP' THEN h.orderNo END) AS open_orders,
    COUNT(DISTINCT CASE WHEN h.orderStatus = 'PR' THEN h.orderNo END) AS picking_orders,
    COUNT(DISTINCT CASE WHEN h.orderStatus = 'SH' THEN h.orderNo END) AS shipped_orders,

    -- Line and SKU Counts
    SUM(COUNT(d.orderLineNo)) OVER (PARTITION BY h.customerId, h.warehouseId) AS total_lines,
    COUNT(DISTINCT d.sku) AS unique_skus_ordered,

    -- Quantity Analysis
    COALESCE(SUM(d.qtyOrdered), 0) AS total_qty_ordered,
    COALESCE(SUM(d.qtyShipped), 0) AS total_qty_shipped,

    -- Financial Analysis
    COALESCE(SUM(h.totalPrice), 0) AS total_order_value,
    COALESCE(AVG(h.totalPrice), 0) AS avg_order_value,

    -- Date Range
    MIN(h.addTime) AS first_order_date,
    MAX(h.addTime) AS last_order_date

FROM T_DOC_ORDER_HEADER h
LEFT JOIN DOC_ORDER_DETAILS d
    ON h.organizationId = d.organizationId
    AND h.orderNo = d.orderNo
GROUP BY h.customerId, h.customerDescr1, h.warehouseId
ORDER BY total_orders DESC, total_order_value DESC;

-- ========================================
-- USAGE NOTES:
-- ========================================
/*
1. Query 1: Basic join showing order headers with their detail lines
2. Query 2: Complete order information with all available fields
3. Query 3: Order summary with aggregated quantities and status
4. Query 4: Detailed allocation status for each order line
5. Query 5: Picking zone analysis for warehouse operations
6. Query 6: Customer order analysis for business intelligence

Join Conditions:
- T_DOC_ORDER_HEADER.primary_key: (organizationId, orderNo)
- DOC_ORDER_DETAILS.primary_key: (organizationId, warehouseId, orderNo, orderLineNo)
- Join: ON h.organizationId = d.organizationId AND h.orderNo = d.orderNo

Common Order Status Values:
- OP: Open
- PR: Picking in Progress
- PK: Picked
- SH: Shipped
- CL: Closed
- CA: Cancelled

Common Line Status Values:
- OP: Open
- AL: Allocated
- PK: Picked
- SH: Shipped
- CA: Cancelled
*/