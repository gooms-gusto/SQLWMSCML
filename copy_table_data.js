const mysql = require('mysql2/promise');

/**
 * Copy table data from office server to home server
 * @param {string} tableName - Name of the table to copy data from
 * @param {Object} options - Copy options
 * @param {number} options.limit - Number of rows to copy (default: all rows)
 * @param {string} options.orderBy - Column to order by (default: none)
 * @param {string} options.orderDirection - ASC or DESC (default: ASC)
 * @param {string} options.whereClause - WHERE clause for filtering rows (default: none)
 * @param {boolean} options.truncateFirst - Truncate target table before inserting (default: false)
 * @param {boolean} options.skipDuplicates - Skip duplicate key errors (default: false)
 */
async function copyTableData(tableName, options = {}) {
    const {
        limit = null,
        orderBy = null,
        orderDirection = 'ASC',
        whereClause = null,
        truncateFirst = false,
        skipDuplicates = false
    } = options;

    // Office server connection (source)
    const officeConfig = {
        host: '172.31.9.92',
        port: 63306,
        user: 'sync.db',
        password: 'U8g01o8s*17u89010',
        database: 'wms_cml'
    };

    // Home server connection (target)
    const homeConfig = {
        host: 'omahkudewe.asia',
        port: 63306,
        user: 'middleware',
        password: 'U8g01o8s*17',
        database: 'wms_cml'
    };

    let officeConnection, homeConnection;

    try {
        console.log(`Starting table data copy for: ${tableName}`);
        console.log('Connecting to office server...');
        officeConnection = await mysql.createConnection(officeConfig);

        // Build SELECT query
        let selectSQL = `SELECT * FROM \`${tableName}\``;
        if (whereClause) {
            selectSQL += ` WHERE ${whereClause}`;
        }
        if (orderBy) {
            selectSQL += ` ORDER BY \`${orderBy}\` ${orderDirection}`;
        }
        if (limit) {
            selectSQL += ` LIMIT ${limit}`;
        }

        console.log(`Getting data from office server: ${selectSQL}`);
        const [rows] = await officeConnection.execute(selectSQL);

        if (rows.length === 0) {
            console.log(`No rows found in ${tableName} table on office server with specified criteria`);
            return { copiedRows: 0, totalRows: 0 };
        }

        console.log(`Found ${rows.length} rows to copy from office server`);

        console.log('\nConnecting to home server...');
        homeConnection = await mysql.createConnection(homeConfig);

        // Truncate table if requested
        if (truncateFirst) {
            console.log(`Truncating ${tableName} table on home server...`);
            await homeConnection.execute(`TRUNCATE TABLE \`${tableName}\``);
        }

        // Get column names and prepare INSERT query
        const columns = Object.keys(rows[0]);
        const placeholders = columns.map(() => '?').join(', ');
        const columnNames = columns.map(col => `\`${col}\``).join(', ');

        let insertSQL = `INSERT`;
        if (skipDuplicates) {
            insertSQL += ` IGNORE`;
        }
        insertSQL += ` INTO \`${tableName}\` (${columnNames}) VALUES (${placeholders})`;

        console.log(`Inserting rows into home server...`);

        let successCount = 0;
        let errorCount = 0;

        // Insert rows in batches for better performance
        const batchSize = 1000;
        for (let i = 0; i < rows.length; i += batchSize) {
            const batch = rows.slice(i, i + batchSize);

            for (const row of batch) {
                try {
                    const values = columns.map(col => row[col]);
                    await homeConnection.execute(insertSQL, values);
                    successCount++;

                    if (successCount % 100 === 0 || successCount === rows.length) {
                        console.log(`Progress: ${successCount}/${rows.length} rows copied`);
                    }
                } catch (error) {
                    if (!skipDuplicates || !error.message.includes('Duplicate entry')) {
                        console.error(`Error inserting row: ${error.message}`);
                        errorCount++;
                    }
                }
            }
        }

        console.log(`✅ Data copy completed!`);
        console.log(`Successfully copied: ${successCount} rows`);
        if (errorCount > 0) {
            console.log(`Failed to copy: ${errorCount} rows`);
        }

        return { copiedRows: successCount, failedRows: errorCount, totalRows: rows.length };

    } catch (error) {
        console.error(`❌ Error copying table data: ${error.message}`);
        throw error;
    } finally {
        if (officeConnection) {
            await officeConnection.end();
            console.log('Office server connection closed');
        }
        if (homeConnection) {
            await homeConnection.end();
            console.log('Home server connection closed');
        }
    }
}

// Export function for use in other modules
module.exports = copyTableData;

// Example usage when run directly
if (require.main === module) {
    const tableName = process.argv[2];

    if (!tableName) {
        console.log('Usage: node copy_table_data.js <table_name> [options]');
        console.log('');
        console.log('Examples:');
        console.log('  node copy_table_data.js BAS_SKU');
        console.log('  node copy_table_data.js BAS_SKU --limit 100');
        console.log('  node copy_table_data.js BAS_SKU --orderBy addTime --orderDirection DESC');
        console.log('  node copy_table_data.js BAS_SKU --where "activeFlag=\'Y\'"');
        console.log('  node copy_table_data.js BAS_SKU --truncateFirst');
        console.log('  node copy_table_data.js BAS_SKU --skipDuplicates');
        console.log('');
        console.log('Options:');
        console.log('  --limit <number>           Limit number of rows to copy');
        console.log('  --orderBy <column>         Order by this column');
        console.log('  --orderDirection <ASC|DESC> Order direction (default: ASC)');
        console.log('  --where <condition>        WHERE clause for filtering');
        console.log('  --truncateFirst            Truncate target table before inserting');
        console.log('  --skipDuplicates           Skip duplicate key errors');
        process.exit(1);
    }

    // Parse command line options
    const options = {};
    const args = process.argv.slice(3);

    for (let i = 0; i < args.length; i += 2) {
        const option = args[i];
        const value = args[i + 1];

        switch (option) {
            case '--limit':
                options.limit = parseInt(value);
                break;
            case '--orderBy':
                options.orderBy = value;
                break;
            case '--orderDirection':
                options.orderDirection = value.toUpperCase();
                break;
            case '--where':
                options.whereClause = value;
                break;
            case '--truncateFirst':
                options.truncateFirst = true;
                i--; // No value for this flag
                break;
            case '--skipDuplicates':
                options.skipDuplicates = true;
                i--; // No value for this flag
                break;
        }
    }

    copyTableData(tableName, options)
        .then((result) => {
            console.log('✅ Table data copy completed successfully!');
            console.log(`Result: ${result.copiedRows}/${result.totalRows} rows copied`);
        })
        .catch((error) => {
            console.error('❌ Table data copy failed:', error.message);
            process.exit(1);
        });
}