const mysql = require('mysql2/promise');

/**
 * Copy table structure from office server to home server
 * @param {string} tableName - Name of the table to copy
 * @param {boolean} dropIfExists - Whether to drop existing table on home server (default: true)
 */
async function copyTableStructure(tableName, dropIfExists = true) {
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
        console.log(`Starting table structure copy for: ${tableName}`);
        console.log('Connecting to office server...');
        officeConnection = await mysql.createConnection(officeConfig);

        console.log(`Getting ${tableName} table structure from office server...`);
        const [rows] = await officeConnection.execute(`SHOW CREATE TABLE \`${tableName}\``);

        if (rows.length === 0) {
            throw new Error(`Table ${tableName} not found on office server`);
        }

        const createTableSQL = rows[0]['Create Table'];
        console.log(`Table structure found for ${tableName}`);

        console.log('\nConnecting to home server...');
        homeConnection = await mysql.createConnection(homeConfig);

        if (dropIfExists) {
            console.log(`Dropping existing ${tableName} table on home server (if exists)...`);
            await homeConnection.execute(`DROP TABLE IF EXISTS \`${tableName}\``);
        }

        console.log(`Creating ${tableName} table on home server...`);
        await homeConnection.execute(createTableSQL);

        console.log(`✅ Table ${tableName} successfully created on home server!`);

    } catch (error) {
        console.error(`❌ Error copying table structure: ${error.message}`);
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
module.exports = copyTableStructure;

// Example usage when run directly
if (require.main === module) {
    const tableName = process.argv[2];

    if (!tableName) {
        console.log('Usage: node copy_table_structure.js <table_name> [drop_if_exists]');
        console.log('Example: node copy_table_structure.js BAS_SKU true');
        console.log('Example: node copy_table_structure.js CUSTOMERS false');
        process.exit(1);
    }

    const dropIfExists = process.argv[3] !== 'false';

    copyTableStructure(tableName, dropIfExists)
        .then(() => {
            console.log('✅ Table structure copy completed successfully!');
        })
        .catch((error) => {
            console.error('❌ Table structure copy failed:', error.message);
            process.exit(1);
        });
}