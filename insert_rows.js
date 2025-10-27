const mysql = require('mysql2/promise');

async function insertTop2Rows() {
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
        console.log('Connecting to office server...');
        officeConnection = await mysql.createConnection(officeConfig);

        console.log('Getting top 2 rows from BAS_SKU ordered by addTime...');
        const [rows] = await officeConnection.execute(`
            SELECT * FROM BAS_SKU
            ORDER BY addTime ASC
            LIMIT 2
        `);

        if (rows.length === 0) {
            console.log('No rows found in BAS_SKU table on office server');
            return;
        }

        console.log(`Found ${rows.length} rows to insert:`);
        rows.forEach((row, index) => {
            console.log(`Row ${index + 1}: ${row.organizationId}, ${row.customerId}, ${row.sku}, addTime: ${row.addTime}`);
        });

        console.log('\nConnecting to home server...');
        homeConnection = await mysql.createConnection(homeConfig);

        // Get column names from the rows
        if (rows.length > 0) {
            const columns = Object.keys(rows[0]);
            const placeholders = columns.map(() => '?').join(', ');
            const columnNames = columns.map(col => `\`${col}\``).join(', ');

            console.log('Inserting rows into home server...');

            for (const row of rows) {
                const values = columns.map(col => row[col]);
                const insertSQL = `INSERT INTO BAS_SKU (${columnNames}) VALUES (${placeholders})`;

                await homeConnection.execute(insertSQL, values);
                console.log(`Inserted row: ${row.organizationId}, ${row.customerId}, ${row.sku}`);
            }

            console.log('Successfully inserted all rows into home server!');
        }

    } catch (error) {
        console.error('Error:', error.message);
        console.error('Stack:', error.stack);
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

insertTop2Rows();