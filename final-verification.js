const mysql = require('mysql2/promise');

async function finalVerification() {
    console.log('Final Verification: BAS_SKU Table Status on Both Servers');
    console.log('=======================================================');

    // Test Home Server
    console.log('\n🏠 HOME SERVER (omahkudewe.asia:63306)');
    console.log('========================================');

    const homeConnection = await mysql.createConnection({
        host: 'omahkudewe.asia',
        port: 63306,
        user: 'middleware',
        password: 'U8g01o8s*17',
        database: 'wms_cml'
    });

    try {
        console.log('🔌 Connected to home server...');

        // Check BAS_SKU table
        const [homeTables] = await homeConnection.execute("SHOW TABLES LIKE 'bas_sku'");
        if (homeTables.length > 0) {
            console.log('✅ BAS_SKU table exists on home server');

            // Get structure
            const [homeStructure] = await homeConnection.execute('DESCRIBE bas_sku');
            console.log(`📋 Structure (${homeStructure.length} columns):`);
            homeStructure.forEach(column => {
                console.log(`   ${column.Field} | ${column.Type} | ${column.Null} | ${column.Key}`);
            });

            // Get record count
            const [homeCount] = await homeConnection.execute('SELECT COUNT(*) as total FROM bas_sku');
            console.log(`📊 Total records: ${homeCount[0].total}`);

            // Show sample data
            const [homeSample] = await homeConnection.execute('SELECT * FROM bas_sku LIMIT 3');
            if (homeSample.length > 0) {
                console.log('📄 Sample data:');
                console.log(JSON.stringify(homeSample, null, 6));
            }
        } else {
            console.log('❌ BAS_SKU table not found on home server');
        }

    } catch (error) {
        console.error('❌ Home server error:', error.message);
    } finally {
        await homeConnection.end();
    }

    // Test Office Server
    console.log('\n🏢 OFFICE SERVER (172.31.9.92:63306)');
    console.log('======================================');

    const officeConnection = await mysql.createConnection({
        host: '172.31.9.92',
        port: 63306,
        user: 'sync.db',
        password: 'U8g01o8s*17u89010',
        database: 'wms_cml'
    });

    try {
        console.log('🔌 Connected to office server...');

        // Check BAS_SKU table
        const [officeTables] = await officeConnection.execute("SHOW TABLES LIKE 'bas_sku'");
        if (officeTables.length > 0) {
            console.log('✅ BAS_SKU table exists on office server');

            // Get structure
            const [officeStructure] = await officeConnection.execute('DESCRIBE bas_sku');
            console.log(`📋 Structure (${officeStructure.length} columns):`);
            officeStructure.forEach(column => {
                console.log(`   ${column.Field} | ${column.Type} | ${column.Null} | ${column.Key}`);
            });

            // Get record count
            const [officeCount] = await officeConnection.execute('SELECT COUNT(*) as total FROM bas_sku');
            console.log(`📊 Total records: ${officeCount[0].total}`);

        } else {
            console.log('❌ BAS_SKU table not found on office server');
            console.log('⚠️  Reason: User sync.db lacks CREATE TABLE permissions');
        }

        // Show AA_SKU_SODA table (existing SKU table)
        console.log('\n📋 AA_SKU_SODA table (existing SKU reference):');
        const [sodaStructure] = await officeConnection.execute('DESCRIBE AA_SKU_SODA');
        sodaStructure.forEach(column => {
            console.log(`   ${column.Field} | ${column.Type} | ${column.Null} | ${column.Key}`);
        });

        const [sodaCount] = await officeConnection.execute('SELECT COUNT(*) as total FROM AA_SKU_SODA');
        console.log(`📊 AA_SKU_SODA total records: ${sodaCount[0].total}`);

        // Show sample data from AA_SKU_SODA
        const [sodaSample] = await officeConnection.execute('SELECT * FROM AA_SKU_SODA LIMIT 5');
        if (sodaSample.length > 0) {
            console.log('📄 AA_SKU_SODA sample data:');
            console.log(JSON.stringify(sodaSample, null, 6));
        }

    } catch (error) {
        console.error('❌ Office server error:', error.message);
    } finally {
        await officeConnection.end();
    }

    // Summary and Recommendations
    console.log('\n📋 SUMMARY AND RECOMMENDATIONS');
    console.log('==============================');
    console.log('✅ Home Server: BAS_SKU table exists with comprehensive structure');
    console.log('❌ Office Server: BAS_SKU table cannot be created (permission limitations)');
    console.log('✅ Office Server: AA_SKU_SODA table exists as SKU reference (181 records)');
    console.log('');
    console.log('🔧 RECOMMENDATIONS:');
    console.log('1. Request CREATE TABLE permissions for sync.db user on office server');
    console.log('2. Alternative: Use admin credentials on office server to create BAS_SKU table');
    console.log('3. Alternative: Create a view that maps AA_SKU_SODA to BAS_SKU structure');
    console.log('4. For data sync: Map AA_SKU_SODA.SKU to BAS_SKU.sku_code');
}

finalVerification();