DELIMITER //

CREATE PROCEDURE CML_BILLSUMMARYPROCESS_FAST(
    IN IN_organizationId VARCHAR(30),
    IN IN_warehouseId VARCHAR(30),
    IN IN_USERID VARCHAR(30),
    IN IN_Language VARCHAR(30),
    IN IN_TariffID VARCHAR(30),
    IN IN_billingDate DATE,
    OUT OUT_RETURN VARCHAR(30)
)
BEGIN
    -- Declare variables
    DECLARE cutoff INT;
    DECLARE customerId VARCHAR(30);
    DECLARE _datefrom DATETIME;
    DECLARE _dateto DATETIME;
    DECLARE done INT DEFAULT FALSE;

    -- Declare exception handlers
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 OUT_RETURN = MESSAGE_TEXT;
        ROLLBACK;
    END;

    -- Start transaction
    START TRANSACTION;

    -- Initialize OUT_RETURN
    SET OUT_RETURN = 'SUCCESS';

    -- Step 1: Clear Z_CML_BILLINGSUMMARYID if there is data
    DELETE FROM Z_CML_BILLINGSUMMARYID
    WHERE organizationId = IN_organizationId
    AND warehouseId = IN_warehouseId;

    -- Step 2: Get cutoff day and customerId
    SELECT
        BIL_TARIFF_MASTER.udf02 AS CUTOFFDAY,
        BIL_TARIFF_MASTER.customerId AS CUSTOMERID
    INTO cutoff, customerId
    FROM BIL_TARIFF_HEADER
    INNER JOIN BIL_TARIFF_MASTER ON BIL_TARIFF_HEADER.organizationId = BIL_TARIFF_MASTER.organizationId
    AND BIL_TARIFF_HEADER.tariffMasterId = BIL_TARIFF_MASTER.tariffMasterId
    WHERE BIL_TARIFF_HEADER.organizationId = IN_organizationId
    AND BIL_TARIFF_HEADER.tariffId = IN_TariffID;

    -- Handle cutoff logic based on cutoff day
    IF cutoff = 25 THEN
        -- Rule for cutoff day 25 (which becomes 26): 26th of month to 25th of next month
        SET _datefrom = DATE_FORMAT(IN_billingDate, '%Y-%m-26 00:00:00');
        SET _dateto = DATE_FORMAT(DATE_ADD(IN_billingDate, INTERVAL 1 MONTH), '%Y-%m-25 23:59:59');

    ELSEIF cutoff = 1 THEN
        -- Rule for cutoff day 1: 1st of month to end of month
        SET _datefrom = DATE_FORMAT(IN_billingDate, '%Y-%m-01 00:00:00');
        SET _dateto = LAST_DAY(IN_billingDate);
        SET _dateto = DATE_FORMAT(_dateto, '%Y-%m-%d 23:59:59');

    ELSE
        -- Handle other cutoff days if needed
        SET OUT_RETURN = CONCAT('ERROR: Unsupported cutoff day - ', cutoff);
        ROLLBACK;
    END IF;

    -- Only proceed if no error occurred
    IF OUT_RETURN = 'SUCCESS' THEN
        -- Step 3: Insert into Z_CML_BILLINGSUMMARYID
        INSERT INTO Z_CML_BILLINGSUMMARYID (organizationId, warehouseId, customerId, billingSummaryId)
        SELECT
            bs.organizationId,
            bs.warehouseId,
            bs.customerId,
            bs.billingSummaryId
        FROM BIL_SUMMARY bs
        WHERE bs.organizationId = IN_organizationId
        AND bs.warehouseId = IN_warehouseId
        AND bs.customerId = customerId
        AND bs.chargeCategory IN ('OB','IB')
        AND bs.billingFromDate >= _datefrom
        AND bs.billingFromDate <= _dateto
        AND bs.arNo = '*';

        -- Step 4: Call CML_BILLSUMMARYPROCESS_MANUAL procedure
        CALL CML_BILLSUMMARYPROCESS_MANUAL(
            IN_organizationId,
            IN_warehouseId,
            customerId,
            'UDFTIMER',
            OUT_RETURN
        );

        -- Commit transaction
        COMMIT;
    END IF;

END //

DELIMITER ;