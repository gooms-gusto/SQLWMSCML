const { getDB1Pool, getDB2Pool } = require('./database');

async function copyIndexes(db1Conn, db2Conn, sourceTableName, targetTableName) {
    try {
        console.log(`[INFO] Checking for additional indexes in ${sourceTableName}...`);

        // Get the CREATE TABLE statement to see what indexes are already included
        const [createTableResult] = await db1Conn.execute(`SHOW CREATE TABLE \`${sourceTableName}\``);
        const createTableSQL = createTableResult[0]['Create Table'].toLowerCase();

        // Get all indexes from source table
        const [allIndexes] = await db1Conn.execute(`
            SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        `, [sourceTableName]);

        if (allIndexes.length === 0) {
            console.log(`[INFO] No indexes found in ${sourceTableName}`);
            return;
        }

        // Group indexes by index name
        const indexGroups = {};
        for (const index of allIndexes) {
            if (!indexGroups[index.INDEX_NAME]) {
                indexGroups[index.INDEX_NAME] = {
                    columns: [],
                    isUnique: index.NON_UNIQUE === 0
                };
            }
            indexGroups[index.INDEX_NAME].columns.push(index.COLUMN_NAME);
        }

        let createdIndexes = 0;
        let skippedIndexes = 0;

        // Create each index on the target table if it's not already in CREATE TABLE
        for (const [indexName, indexInfo] of Object.entries(indexGroups)) {
            // Skip PRIMARY key as it's included in CREATE TABLE
            if (indexName === 'PRIMARY') {
                skippedIndexes++;
                continue;
            }

            // Check if this index is already defined in the CREATE TABLE statement
            const indexKey = `key \`${indexName.toLowerCase()}\``;
            const indexIndex = `index \`${indexName.toLowerCase()}\``;
            const uniqueKey = `unique key \`${indexName.toLowerCase()}\``;
            const uniqueIndex = `unique index \`${indexName.toLowerCase()}\``;

            const indexExistsInCreateTable = createTableSQL.includes(indexKey) ||
                                          createTableSQL.includes(indexIndex) ||
                                          createTableSQL.includes(uniqueKey) ||
                                          createTableSQL.includes(uniqueIndex);

            if (indexExistsInCreateTable) {
                console.log(`[INFO] Index ${indexName} already exists in table structure, skipping...`);
                skippedIndexes++;
                continue;
            }

            try {
                const indexType = indexInfo.isUnique ? 'UNIQUE INDEX' : 'INDEX';
                const columnList = indexInfo.columns.map(col => `\`${col}\``).join(', ');
                const createIndexSQL = `ALTER TABLE \`${targetTableName}\` ADD ${indexType} \`${indexName}\` (${columnList})`;

                await db2Conn.execute(createIndexSQL);
                console.log(`[INFO] Created ${indexType.toLowerCase()} ${indexName} on ${targetTableName}`);
                createdIndexes++;

            } catch (indexError) {
                console.warn(`[WARNING] Failed to create index ${indexName}: ${indexError.message}`);
                skippedIndexes++;
            }
        }

        if (createdIndexes > 0) {
            console.log(`[SUCCESS] Created ${createdIndexes} additional indexes on ${targetTableName}`);
        } else {
            console.log(`[INFO] All indexes were already included in table structure`);
        }

        if (skippedIndexes > 0) {
            console.log(`[INFO] Skipped ${skippedIndexes} indexes (already exist or failed to create)`);
        }

    } catch (error) {
        console.error(`[ERROR] Failed to copy indexes: ${error.message}`);
        throw error;
    }
}

async function copyTableStructure(sourceTableName, targetTableName) {
    let db1Conn = null;
    let db2Conn = null;

    try {
        console.log(`[INFO] Copying table structure from ${sourceTableName} to ${targetTableName}`);

        db1Conn = await getDB1Pool().getConnection();
        db2Conn = await getDB2Pool().getConnection();

        // First, check if the source table exists (case-insensitive check)
        const [sourceTableCheck] = await db1Conn.execute(
            `SELECT TABLE_NAME FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND UPPER(TABLE_NAME) = UPPER(?)`,
            [sourceTableName]
        );

        if (sourceTableCheck.length === 0) {
            throw new Error(`[DB1_ERROR] Table ${sourceTableName} not found in source database`);
        }

        const actualSourceTableName = sourceTableCheck[0].TABLE_NAME;
        console.log(`[INFO] Found source table: ${actualSourceTableName}`);

        // Check if target table already exists
        const [targetTableCheck] = await db2Conn.execute(
            `SELECT TABLE_NAME FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND UPPER(TABLE_NAME) = UPPER(?)`,
            [targetTableName]
        );

        if (targetTableCheck.length > 0) {
            const actualTargetTableName = targetTableCheck[0].TABLE_NAME;
            throw new Error(`Target table ${actualTargetTableName} already exists. Use sync-table to sync data or copy-data to copy data to existing table.`);
        }

        const [createTableResult] = await db1Conn.execute(`SHOW CREATE TABLE \`${actualSourceTableName}\``);

        let createTableSQL = createTableResult[0]['Create Table'];

        // Replace the source table name with target table name, preserving case
        createTableSQL = createTableSQL.replace(
            new RegExp(`CREATE TABLE \`${actualSourceTableName}\``, 'i'),
            `CREATE TABLE \`${targetTableName}\``
        );

        // Remove AUTO_INCREMENT value
        createTableSQL = createTableSQL.replace(/AUTO_INCREMENT=\d+/i, '');

        console.log(`[INFO] Creating table ${targetTableName} in target database`);

        // Set SQL mode to preserve case for table names
        await db2Conn.execute("SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'");

        await db2Conn.execute(createTableSQL);

        // Copy indexes from source table
        console.log(`[INFO] Copying indexes from ${actualSourceTableName} to ${targetTableName}`);
        await copyIndexes(db1Conn, db2Conn, actualSourceTableName, targetTableName);

        // Verify the table was created and check for case conversion
        const [caseCheck] = await db2Conn.execute(
            `SELECT TABLE_NAME FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND UPPER(TABLE_NAME) = UPPER(?)`,
            [targetTableName]
        );

        if (caseCheck.length > 0) {
            const actualTableName = caseCheck[0].TABLE_NAME;

            if (actualTableName === targetTableName) {
                console.log(`[SUCCESS] Table ${targetTableName} created successfully with correct case and indexes`);
            } else {
                console.log(`[INFO] Table ${targetTableName} created as '${actualTableName}' (MySQL converted to lowercase due to lower_case_table_names=1)`);
                console.log(`[SUCCESS] Table structure and indexes created successfully`);

                // Check MySQL setting to provide better context
                try {
                    const [mysqlSetting] = await db2Conn.execute('SHOW VARIABLES LIKE "lower_case_table_names"');
                    const setting = mysqlSetting[0]?.Value;
                    if (setting === '1') {
                        console.log(`[NOTE] MySQL server has lower_case_table_names=${setting}, which forces table names to lowercase on this platform`);
                    }
                } catch (settingError) {
                    // Ignore if we can't check the setting
                }
            }
            return actualTableName; // Return the actual table name
        } else {
            console.log(`[ERROR] Table creation failed - table not found in database`);
            return targetTableName; // Fallback
        }

    } catch (error) {
        console.error(`[ERROR] Failed to copy table structure: ${error.message}`);
        throw error;
    } finally {
        if (db1Conn) db1Conn.release();
        if (db2Conn) db2Conn.release();
    }
}


async function copyTableData(sourceTableName, targetTableName, limit) {
    let db1Conn = null;
    let db2Conn = null;

    try {
        console.log(`[INFO] Copying data from ${sourceTableName} to ${targetTableName}`);

        db1Conn = await getDB1Pool().getConnection();
        db2Conn = await getDB2Pool().getConnection();

        // Check if target table exists and has data
        const [targetTableCheck] = await db2Conn.execute(
            `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
            [targetTableName]
        );

        if (targetTableCheck[0].count === 0) {
            throw new Error(`Target table ${targetTableName} does not exist. Use copy-table command to create structure first.`);
        }

        // Check if target table has existing data
        const [targetDataCheck] = await db2Conn.execute(`SELECT COUNT(*) as count FROM \`${targetTableName}\``);

        if (targetDataCheck[0].count > 0) {
            console.log(`[INFO] Target table ${targetTableName} has ${targetDataCheck[0].count} existing records.`);
            console.log(`[INFO] copy-data only works with empty target tables. Use sync-table to sync data to existing tables.`);
            return { copiedRows: 0, skippedRows: targetDataCheck[0].count };
        }

        console.log(`[INFO] Target table ${targetTableName} is empty. Starting data copy...`);

        let selectQuery = `SELECT * FROM \`${sourceTableName}\``;
        if (limit && Number.isInteger(limit) && limit > 0) {
            selectQuery += ` LIMIT ${limit}`;
            console.log(`[INFO] Limiting copy to ${limit} rows`);
        }

        const [rows, fields] = await db1Conn.execute(selectQuery);

        if (rows.length === 0) {
            console.log(`[INFO] No data found in ${sourceTableName}`);
            return { copiedRows: 0 };
        }

        console.log(`[INFO] Found ${rows.length} records to copy from source table`);

        const columns = fields.map(f => f.name);
        const colNamesSQL = columns.map(c => `\`${c}\``).join(', ');

        // Use chunking for large datasets
        const CHUNK_SIZE = 1000;
        let totalInserted = 0;

        await db2Conn.beginTransaction();

        try {
            for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
                const chunk = rows.slice(i, i + CHUNK_SIZE);
                const values = chunk.map(row => columns.map(col => row[col]));

                const valuePlaceholders = values.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                const flatValues = values.flat();

                const insertQuery = `INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ${valuePlaceholders}`;

                const [result] = await db2Conn.execute(insertQuery, flatValues);
                totalInserted += result.affectedRows;

                console.log(`[INFO] Inserted batch ${Math.floor(i / CHUNK_SIZE) + 1}: ${result.affectedRows} rows`);
            }

            await db2Conn.commit();
            console.log(`[SUCCESS] Copied ${totalInserted} rows from ${sourceTableName} to ${targetTableName}`);
            return { copiedRows: totalInserted };

        } catch (insertError) {
            await db2Conn.rollback();
            throw insertError;
        }

    } catch (error) {
        console.error(`[ERROR] Failed to copy table data: ${error.message}`);
        throw error;
    } finally {
        if (db1Conn) db1Conn.release();
        if (db2Conn) db2Conn.release();
    }
}

async function copyCustomQuery(selectQuery, targetTableName) {
    let db1Conn = null;
    let db2Conn = null;

    try {
        console.log(`[INFO] Executing custom query and inserting results into ${targetTableName}`);

        if (!selectQuery.trim().toLowerCase().startsWith('select')) {
            throw new Error('[ERROR] Custom query must be a SELECT statement');
        }

        db1Conn = await getDB1Pool().getConnection();
        db2Conn = await getDB2Pool().getConnection();

        const [rows, fields] = await db1Conn.execute(selectQuery);

        if (rows.length === 0) {
            console.log('[INFO] Custom query returned no results');
            return { copiedRows: 0 };
        }

        const columns = fields.map(f => f.name);
        const colNamesSQL = columns.map(c => `\`${c}\``).join(', ');

        // Check if target table exists
        const [tableCheck] = await db2Conn.execute(
            `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
            [targetTableName]
        );

        if (tableCheck[0].count === 0) {
            console.log(`[INFO] Target table ${targetTableName} does not exist. Creating table structure...`);

            // Create table based on query result structure
            const columnDefinitions = fields.map(field => {
                let definition = `\`${field.name}\` `;

                // Map MySQL types to appropriate CREATE TABLE types
                switch (field.type) {
                    case 'varchar':
                    case 'char':
                    case 'text':
                    case 'longtext':
                        definition += `VARCHAR(${field.length || 255})`;
                        break;
                    case 'int':
                    case 'tinyint':
                    case 'smallint':
                    case 'mediumint':
                    case 'bigint':
                        definition += 'INT';
                        break;
                    case 'decimal':
                    case 'float':
                    case 'double':
                        definition += `${field.type.toUpperCase()}`;
                        break;
                    case 'date':
                        definition += 'DATE';
                        break;
                    case 'datetime':
                    case 'timestamp':
                        definition += 'DATETIME';
                        break;
                    case 'time':
                        definition += 'TIME';
                        break;
                    case 'json':
                        definition += 'JSON';
                        break;
                    default:
                        definition += 'VARCHAR(255)';
                }

                // Add NULL/NOT NULL based on field flags
                try {
                    if (field.flags && (
                        (Array.isArray(field.flags) && field.flags.includes('NOT_NULL')) ||
                        (typeof field.flags === 'string' && field.flags.includes('NOT_NULL'))
                    )) {
                        definition += ' NOT NULL';
                    } else {
                        definition += ' NULL';
                    }
                } catch (flagError) {
                    // Default to NULL if there's an issue with flags
                    definition += ' NULL';
                }

                return definition;
            }).join(',\n  ');

            const createTableQuery = `CREATE TABLE \`${targetTableName}\` (\n  ${columnDefinitions}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`;

            await db2Conn.execute(createTableQuery);

            // Check actual table name created
            const [actualTableCheck] = await db2Conn.execute(
                'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND UPPER(TABLE_NAME) = UPPER(?)',
                [targetTableName]
            );

            if (actualTableCheck.length > 0) {
                const actualTableName = actualTableCheck[0].TABLE_NAME;
                if (actualTableName === targetTableName) {
                    console.log(`[SUCCESS] Created table ${targetTableName}`);
                } else {
                    console.log(`[INFO] Created table as '${actualTableName}' (MySQL converted to lowercase due to server configuration)`);
                }
            } else {
                console.log(`[SUCCESS] Created table ${targetTableName}`);
            }
        }

        // Prepare data for bulk insert with chunking
        const values = rows.map(row => columns.map(col => row[col]));

        // Define chunk size (1000 rows per batch to stay well under MySQL's placeholder limit)
        const CHUNK_SIZE = 1000;
        let totalInserted = 0;

        await db2Conn.beginTransaction();

        try {
            for (let i = 0; i < values.length; i += CHUNK_SIZE) {
                const chunk = values.slice(i, i + CHUNK_SIZE);

                // Build INSERT query for this chunk
                const valuePlaceholders = chunk.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                const flatValues = chunk.flat();

                const insertQuery = `INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ${valuePlaceholders}`;

                const [result] = await db2Conn.execute(insertQuery, flatValues);
                totalInserted += result.affectedRows;

                console.log(`[INFO] Inserted batch ${Math.floor(i / CHUNK_SIZE) + 1}: ${result.affectedRows} rows`);
            }

            await db2Conn.commit();
            console.log(`[SUCCESS] Inserted total of ${totalInserted} rows into ${targetTableName}`);
            return { copiedRows: totalInserted };

        } catch (insertError) {
            await db2Conn.rollback();
            throw insertError;
        }

    } catch (error) {
        console.error(`[ERROR] Failed to execute custom query: ${error.message}`);
        throw error;
    } finally {
        if (db1Conn) db1Conn.release();
        if (db2Conn) db2Conn.release();
    }
}

async function syncTable(tableName) {
    let db1Conn = null;
    let db2Conn = null;

    try {
        console.log(`[INFO] Starting table sync: ${tableName} from DB1 to DB2`);
        console.log(`[INFO] Sync direction: DB1 → DB2 (copy only missing records)`);

        db1Conn = await getDB1Pool().getConnection();
        db2Conn = await getDB2Pool().getConnection();

        // Check if source table exists in DB1
        const [sourceTableCheck] = await db1Conn.execute(
            `SELECT TABLE_NAME FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND UPPER(TABLE_NAME) = UPPER(?)`,
            [tableName]
        );

        if (sourceTableCheck.length === 0) {
            throw new Error(`[DB1_ERROR] Source table ${tableName} not found in DB1`);
        }

        const actualSourceTableName = sourceTableCheck[0].TABLE_NAME;
        console.log(`[INFO] Found source table in DB1: ${actualSourceTableName}`);

        // Check if target table exists in DB2
        const [targetTableCheck] = await db2Conn.execute(
            `SELECT TABLE_NAME FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND UPPER(TABLE_NAME) = UPPER(?)`,
            [tableName]
        );

        if (targetTableCheck.length === 0) {
            console.log(`[INFO] Target table ${tableName} not found in DB2. Creating table structure first...`);
            await copyTableStructure(actualSourceTableName, tableName);
            console.log(`[INFO] Table structure created. Now syncing data...`);
        } else {
            const actualTargetTableName = targetTableCheck[0].TABLE_NAME;
            console.log(`[INFO] Found target table in DB2: ${actualTargetTableName}`);
        }

        // Get primary key columns from source table
        const [sourceColumns] = await db1Conn.execute(
            `SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
            [actualSourceTableName]
        );

        const primaryKeyColumns = sourceColumns
            .filter(col => col.COLUMN_KEY === 'PRI')
            .map(col => col.COLUMN_NAME);

        if (primaryKeyColumns.length === 0) {
            console.log(`[WARNING] No primary key found in ${actualSourceTableName}. Sync will use full column comparison.`);
            return await syncTableWithoutPrimaryKey(actualSourceTableName, tableName, db1Conn, db2Conn, sourceColumns);
        }

        console.log(`[INFO] Using primary key columns for sync: ${primaryKeyColumns.join(', ')}`);

        // Start the sync process with batching
        await performSyncWithBatching(
            actualSourceTableName,
            tableName,
            db1Conn,
            db2Conn,
            primaryKeyColumns,
            sourceColumns.map(col => col.COLUMN_NAME)
        );

    } catch (error) {
        console.error(`[ERROR] Failed to sync table ${tableName}: ${error.message}`);
        throw error;
    } finally {
        if (db1Conn) db1Conn.release();
        if (db2Conn) db2Conn.release();
    }
}

async function performSyncWithBatching(sourceTableName, targetTableName, db1Conn, db2Conn, primaryKeyColumns, allColumns) {
    const BATCH_SIZE = 1000;
    let totalSynced = 0;
    let batchNumber = 1;

    // Build JOIN condition
    const joinConditions = primaryKeyColumns.map(col =>
        `s.\`${col}\` = t.\`${col}\``
    ).join(' AND ');

    // Count total missing records first
    console.log(`[INFO] Counting total records to sync...`);

    // Debug: Check row counts in both tables
    const [sourceCountResult] = await db1Conn.execute(`SELECT COUNT(*) as count FROM \`${sourceTableName}\``);
    const [targetCountResult] = await db2Conn.execute(`SELECT COUNT(*) as count FROM \`${targetTableName}\``);
    console.log(`[DEBUG] Source table (${sourceTableName}) has ${sourceCountResult[0].count} rows`);
    console.log(`[DEBUG] Target table (${targetTableName}) has ${targetCountResult[0].count} rows`);

    let totalMissing;

    // If target table is empty, sync all records from source
    if (targetCountResult[0].count === 0) {
        console.log(`[INFO] Target table is empty. Will sync all ${sourceCountResult[0].count} records from source.`);
        totalMissing = sourceCountResult[0].count;
    } else {
        // Use LEFT JOIN to find missing records
        const countQuery = `
            SELECT COUNT(*) as count FROM \`${sourceTableName}\` s
            LEFT JOIN \`${targetTableName}\` t ON ${joinConditions}
            WHERE t.\`${primaryKeyColumns[0]}\` IS NULL
        `;
        console.log(`[DEBUG] Sync query: ${countQuery}`);
        const [countResult] = await db1Conn.execute(countQuery);
        totalMissing = countResult[0].count;
        console.log(`[DEBUG] Missing records count: ${totalMissing}`);
    }

    if (totalMissing === 0) {
        console.log(`[SUCCESS] Table ${targetTableName} is already up to date. No records to sync.`);
        return { syncedRows: 0, totalBatches: 0 };
    }

    console.log(`[INFO] Found ${totalMissing} records to sync from ${sourceTableName} to ${targetTableName}`);
    console.log(`[INFO] Processing in batches of ${BATCH_SIZE} rows...`);

    // Get column names for INSERT
    const columnNames = allColumns;
    const colNamesSQL = columnNames.map(c => `\`${c}\``).join(', ');

    // Process in batches
    let offset = 0;
    const totalBatches = Math.ceil(totalMissing / BATCH_SIZE);

    while (offset < totalMissing) {
        console.log(`[BATCH ${batchNumber}/${totalBatches}] Syncing up to ${BATCH_SIZE} records...`);

        // Get missing records for this batch
        let batchQuery;

        if (targetCountResult[0].count === 0) {
            // Target table is empty, get all records from source
            batchQuery = `
                SELECT * FROM \`${sourceTableName}\`
                LIMIT ${BATCH_SIZE} OFFSET ${(batchNumber - 1) * BATCH_SIZE}
            `;
        } else {
            // Target table has data, find missing records using LEFT JOIN
            batchQuery = `
                SELECT s.* FROM \`${sourceTableName}\` s
                LEFT JOIN \`${targetTableName}\` t ON ${joinConditions}
                WHERE t.\`${primaryKeyColumns[0]}\` IS NULL
                LIMIT ${BATCH_SIZE}
            `;
        }

        console.log(`[DEBUG] Batch ${batchNumber} query: ${batchQuery}`);
        const [rows] = await db1Conn.execute(batchQuery);
        console.log(`[DEBUG] Batch ${batchNumber} found ${rows.length} rows to sync`);

        if (rows.length === 0) {
            console.log(`[BATCH ${batchNumber}] No more records to sync.`);
            break;
        }

        // Prepare data for insertion
        const values = rows.map(row => columnNames.map(col => row[col]));
        const valuePlaceholders = values.map(() => `(${columnNames.map(() => '?').join(', ')})`).join(', ');
        const flatValues = values.flat();

        const insertQuery = `INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ${valuePlaceholders}`;

        // Execute batch insertion
        await db2Conn.beginTransaction();

        try {
            const [result] = await db2Conn.execute(insertQuery, flatValues);
            await db2Conn.commit();

            totalSynced += result.affectedRows;
            console.log(`[BATCH ${batchNumber}] Synced ${result.affectedRows} records (Total: ${totalSynced}/${totalMissing})`);

            if (result.affectedRows === 0) {
                console.log(`[WARNING] Batch ${batchNumber} inserted 0 records. This might indicate a data issue.`);
            }

        } catch (insertError) {
            await db2Conn.rollback();
            console.error(`[ERROR] Batch ${batchNumber} failed: ${insertError.message}`);
            throw insertError;
        }

        offset += rows.length;
        batchNumber++;

        // Small delay to prevent overwhelming the database
        await new Promise(resolve => setTimeout(resolve, 10));
    }

    console.log(`[SUCCESS] Table sync completed! Synced ${totalSynced} records from ${sourceTableName} to ${targetTableName}`);
    return { syncedRows: totalSynced, totalBatches: batchNumber - 1 };
}

async function syncTableWithoutPrimaryKey(sourceTableName, targetTableName, db1Conn, db2Conn, sourceColumns) {
    console.log(`[INFO] Performing full column comparison sync for ${sourceTableName}`);

    const BATCH_SIZE = 1000;
    const columnNames = sourceColumns.map(col => col.COLUMN_NAME);
    const colNamesSQL = columnNames.map(c => `\`${c}\``).join(', ');

    // Get all records from source
    const [allSourceRecords] = await db1Conn.execute(`SELECT * FROM \`${sourceTableName}\``);

    if (allSourceRecords.length === 0) {
        console.log(`[INFO] No records found in source table ${sourceTableName}`);
        return { syncedRows: 0, totalBatches: 0 };
    }

    console.log(`[INFO] Checking ${allSourceRecords.length} records against target table...`);

    let recordsToSync = [];
    let processed = 0;

    for (const sourceRecord of allSourceRecords) {
        // Build WHERE clause for this record
        const whereConditions = columnNames.map(col => `\`${col}\` = ?`).join(' AND ');
        const whereValues = columnNames.map(col => sourceRecord[col]);

        // Check if record exists in target
        const [existingCheck] = await db2Conn.execute(
            `SELECT COUNT(*) as count FROM \`${targetTableName}\` WHERE ${whereConditions}`,
            whereValues
        );

        if (existingCheck[0].count === 0) {
            recordsToSync.push(sourceRecord);
        }

        processed++;
        if (processed % 1000 === 0) {
            console.log(`[PROGRESS] Checked ${processed}/${allSourceRecords.length} records...`);
        }
    }

    if (recordsToSync.length === 0) {
        console.log(`[SUCCESS] Table ${targetTableName} is already up to date. No records to sync.`);
        return { syncedRows: 0, totalBatches: 0 };
    }

    console.log(`[INFO] Found ${recordsToSync.length} records to sync. Processing in batches of ${BATCH_SIZE}...`);

    // Insert in batches
    let totalSynced = 0;
    const totalBatches = Math.ceil(recordsToSync.length / BATCH_SIZE);

    for (let i = 0; i < recordsToSync.length; i += BATCH_SIZE) {
        const batch = recordsToSync.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

        console.log(`[BATCH ${batchNumber}/${totalBatches}] Syncing ${batch.length} records...`);

        const values = batch.map(row => columnNames.map(col => row[col]));
        const valuePlaceholders = values.map(() => `(${columnNames.map(() => '?').join(', ')})`).join(', ');
        const flatValues = values.flat();

        const insertQuery = `INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ${valuePlaceholders}`;

        await db2Conn.beginTransaction();

        try {
            const [result] = await db2Conn.execute(insertQuery, flatValues);
            await db2Conn.commit();

            totalSynced += result.affectedRows;
            console.log(`[BATCH ${batchNumber}] Synced ${result.affectedRows} records (Total: ${totalSynced}/${recordsToSync.length})`);

        } catch (insertError) {
            await db2Conn.rollback();
            console.error(`[ERROR] Batch ${batchNumber} failed: ${insertError.message}`);
            throw insertError;
        }

        await new Promise(resolve => setTimeout(resolve, 10));
    }

    console.log(`[SUCCESS] Table sync completed! Synced ${totalSynced} records from ${sourceTableName} to ${targetTableName}`);
    return { syncedRows: totalSynced, totalBatches };
}

module.exports = {
    copyTableStructure,
    copyTableData,
    copyCustomQuery,
    syncTable
};