const mysql = require('mysql2/promise');

async function getTableStructure() {
    console.log('Getting BAS_SKU table structure from Office Server...');
    console.log('=====================================================');

    const connection = await mysql.createConnection({
        host: '172.31.9.92',
        port: 63306,
        user: 'sync.db',
        password: 'U8g01o8s*17u89010',
        database: 'wms_cml'
    });

    try {
        console.log('🔌 Connected to office server...');

        // Check if table exists
        const [tables] = await connection.execute("SHOW TABLES LIKE 'bas_sku'");
        if (tables.length === 0) {
            console.log('❌ BAS_SKU table not found on office server');

            // Let's check for similar table names
            const [similarTables] = await connection.execute("SHOW TABLES LIKE '%sku%'");
            if (similarTables.length > 0) {
                console.log('📋 Similar tables found:');
                similarTables.forEach(table => {
                    console.log(`   - ${Object.values(table)[0]}`);
                });
            }

            // Let's check for tables starting with 'bas_'
            const [basTables] = await connection.execute("SHOW TABLES LIKE 'bas_%'");
            console.log(`📋 Found ${basTables.length} tables starting with 'bas_':`);
            basTables.slice(0, 10).forEach(table => {
                console.log(`   - ${Object.values(table)[0]}`);
            });
            if (basTables.length > 10) {
                console.log(`   ... and ${basTables.length - 10} more tables`);
            }

            return null;
        }

        // Get table structure
        console.log('🔍 Getting table structure...');
        const [structure] = await connection.execute('DESCRIBE bas_sku');

        console.log('📋 Table Structure:');
        console.log('==================');

        let createTableSQL = `CREATE TABLE bas_sku (\n`;

        structure.forEach((column, index) => {
            console.log(`${column.Field} | ${column.Type} | ${column.Null} | ${column.Key} | ${column.Default} | ${column.Extra}`);

            // Build CREATE TABLE statement
            let columnDef = `  ${column.Field} ${column.Type}`;
            if (column.Null === 'NO') columnDef += ' NOT NULL';
            if (column.Default !== null) columnDef += ` DEFAULT ${column.Default}`;
            if (column.Extra) columnDef += ` ${column.Extra}`;

            createTableSQL += columnDef;
            if (index < structure.length - 1) createTableSQL += ',';
            createTableSQL += '\n';
        });

        // Get primary key and indexes
        const [indexes] = await connection.execute('SHOW INDEX FROM bas_sku');
        const primaryKeys = indexes.filter(idx => idx.Key_name === 'PRIMARY');
        const uniqueIndexes = indexes.filter(idx => idx.Key_name !== 'PRIMARY' && idx.Non_unique === 0);
        const regularIndexes = indexes.filter(idx => idx.Key_name !== 'PRIMARY' && idx.Non_unique === 1);

        // Add primary key
        if (primaryKeys.length > 0) {
            createTableSQL += ',\n  PRIMARY KEY (';
            createTableSQL += primaryKeys.map(pk => pk.Column_name).join(', ');
            createTableSQL += ')';
        }

        // Add unique indexes
        const indexNames = new Set();
        uniqueIndexes.forEach(idx => {
            if (!indexNames.has(idx.Key_name)) {
                const sameIndex = uniqueIndexes.filter(i => i.Key_name === idx.Key_name);
                createTableSQL += ',\n  UNIQUE KEY ' + idx.Key_name + ' (';
                createTableSQL += sameIndex.map(i => i.Column_name).join(', ');
                createTableSQL += ')';
                indexNames.add(idx.Key_name);
            }
        });

        // Add regular indexes
        const regularIndexNames = new Set();
        regularIndexes.forEach(idx => {
            if (!regularIndexNames.has(idx.Key_name)) {
                const sameIndex = regularIndexes.filter(i => i.Key_name === idx.Key_name);
                createTableSQL += ',\n  KEY ' + idx.Key_name + ' (';
                createTableSQL += sameIndex.map(i => i.Column_name).join(', ');
                createTableSQL += ')';
                regularIndexNames.add(idx.Key_name);
            }
        });

        createTableSQL += '\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;';

        console.log('\n📝 CREATE TABLE SQL:');
        console.log('====================');
        console.log(createTableSQL);

        // Save SQL to file
        const fs = require('fs');
        fs.writeFileSync('bas_sku_structure.sql', createTableSQL);
        console.log('\n💾 SQL structure saved to bas_sku_structure.sql');

        return createTableSQL;

    } catch (error) {
        console.error('❌ Error:', error.message);
        return null;
    } finally {
        await connection.end();
        console.log('🔌 Connection closed.');
    }
}

getTableStructure();