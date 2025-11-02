DELIMITER //

CREATE PROCEDURE z_Record_preAsnStock()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    INSERT INTO Z_InventoryBalance_preASN (
        organizationId,
        warehouseId,
        sortirId,
        sortirLineId,
        customerId,
        palletId,
        ean,
        qtyPallet,
        qtyEach,
        packey,
        sku,
        stockDate,
        status,
        stopCalculateDate,
        noteText,
        udf01,
        udf02,
        udf03,
        udf04,
        udf05,
        currentVersion,
        oprSeqFlag,
        addWho,
        addTime,
        editWho,
        editTime
    )
SELECT
  zsid.organizationId,
        zsid.warehouseId,
        zsid.sortirId,
        zsid.sortirLineId,
        zsid.customerId,
        zsid.palletId,
        zsid.ean,
        zsid.qty,
        (zsid.qty * bpd.qty) as qtyEach,
        bsm.packId,
        zsid.sku,
        DATE_SUB(NOW(), INTERVAL 1 DAY) as stockDate,
        '00' as status,
        NULL as stopCalculateDate,
        NULL as noteText,
        NULL as udf01,
        NULL as udf02,
        NULL as udf03,
        NULL as udf04,
        NULL as udf05,
        100 as currentVersion,
        2016 as oprSeqFlag,
        'UDFTIMER' as addWho,
        NOW() as addTime,
        NULL as editWho,
        NULL as editTime
  FROM Z_SORTIR_INBOUND_DETAILS zsid  
  INNER JOIN BAS_SKU bs ON zsid.organizationId = bs.organizationId
  AND zsid.customerId = bs.customerId
  AND zsid.sku = bs.sku
  LEFT OUTER JOIN BAS_SKU_MULTIWAREHOUSE bsm
  ON zsid.organizationId = bsm.organizationId
  AND zsid.customerId = bsm.customerId
  AND zsid.sku = bsm.sku
  AND zsid.warehouseId = bsm.warehouseId
  LEFT OUTER JOIN BAS_PACKAGE_DETAILS bpd
  ON zsid.organizationId = bpd.organizationId
  AND bs.organizationId = bpd.organizationId
  AND bsm.packId = bpd.packId
  AND bpd.packUom='PL'
  WHERE zsid.organizationId='OJV_CML'
  AND bsm.warehouseId IN ('CBT02','JBK01')
  AND bpd.customerId='MAP';

    COMMIT;
END //

DELIMITER ;