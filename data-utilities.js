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
        } else {
            console.log(`[ERROR] Table creation failed - table not found in database`);
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

        const columns = fields.map(f => f.name);
        const colNamesSQL = columns.map(c => `\`${c}\``).join(', ');
        const values = rows.map(row => columns.map(col => row[col]));

        const insertQuery = `INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ?`;

        await db2Conn.beginTransaction();

        try {
            const [result] = await db2Conn.execute(insertQuery, [values]);
            await db2Conn.commit();

            console.log(`[SUCCESS] Copied ${result.affectedRows} rows from ${sourceTableName} to ${targetTableName}`);
            return { copiedRows: result.affectedRows };

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

        // Prepare data for bulk insert
        const values = rows.map(row => columns.map(col => row[col]));

        // Build INSERT query with individual value placeholders
        const valuePlaceholders = values.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
        const flatValues = values.flat();

        const insertQuery = `INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ${valuePlaceholders}`;

        await db2Conn.beginTransaction();

        try {
            const [result] = await db2Conn.execute(insertQuery, flatValues);
            await db2Conn.commit();

            console.log(`[SUCCESS] Inserted ${result.affectedRows} rows into ${targetTableName}`);
            return { copiedRows: result.affectedRows };

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

module.exports = {
    copyTableStructure,
    copyTableData,
    copyCustomQuery
};