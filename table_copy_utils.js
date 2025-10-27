const copyTableStructure = require('./copy_table_structure');
const copyTableData = require('./copy_table_data');

/**
 * Complete table copy (structure + data) from office to home server
 * @param {string} tableName - Name of the table to copy
 * @param {Object} options - Copy options
 */
async function copyTableComplete(tableName, options = {}) {
    const {
        copyStructure = true,
        copyData = true,
        dropIfExists = true,
        dataOptions = {}
    } = options;

    console.log(`🚀 Starting complete table copy for: ${tableName}`);
    console.log(`Options: Structure=${copyStructure}, Data=${copyData}, DropIfExists=${dropIfExists}`);

    try {
        // Step 1: Copy table structure
        if (copyStructure) {
            console.log('\n=== Step 1: Copying Table Structure ===');
            await copyTableStructure(tableName, dropIfExists);
            console.log('✅ Table structure copied successfully');
        }

        // Step 2: Copy table data
        if (copyData) {
            console.log('\n=== Step 2: Copying Table Data ===');
            const result = await copyTableData(tableName, dataOptions);
            console.log('✅ Table data copied successfully');
            console.log(`📊 Summary: ${result.copiedRows}/${result.totalRows} rows copied`);
        }

        console.log(`\n🎉 Complete table copy for ${tableName} finished successfully!`);

    } catch (error) {
        console.error(`❌ Complete table copy failed: ${error.message}`);
        throw error;
    }
}

// Export functions
module.exports = {
    copyTableStructure,
    copyTableData,
    copyTableComplete
};

// Example usage when run directly
if (require.main === module) {
    const tableName = process.argv[2];

    if (!tableName) {
        console.log('Usage: node table_copy_utils.js <table_name> [options]');
        console.log('');
        console.log('Examples:');
        console.log('  node table_copy_utils.js BAS_SKU');
        console.log('  node table_copy_utils.js BAS_SKU --structureOnly');
        console.log('  node table_copy_utils.js BAS_SKU --dataOnly');
        console.log('  node table_copy_utils.js BAS_SKU --limit 100 --orderBy addTime');
        console.log('');
        console.log('Options:');
        console.log('  --structureOnly     Only copy table structure (no data)');
        console.log('  --dataOnly          Only copy data (assumes structure exists)');
        console.log('  --noDrop            Don\'t drop existing table');
        console.log('  All data copy options are also available (--limit, --orderBy, etc.)');
        process.exit(1);
    }

    // Parse command line options
    const options = {
        copyStructure: true,
        copyData: true,
        dropIfExists: true,
        dataOptions: {}
    };

    const args = process.argv.slice(3);
    for (let i = 0; i < args.length; i++) {
        const option = args[i];
        const value = args[i + 1];

        switch (option) {
            case '--structureOnly':
                options.copyData = false;
                i--; // No value for this flag
                break;
            case '--dataOnly':
                options.copyStructure = false;
                options.dropIfExists = false;
                i--; // No value for this flag
                break;
            case '--noDrop':
                options.dropIfExists = false;
                i--; // No value for this flag
                break;
            case '--limit':
                options.dataOptions.limit = parseInt(value);
                break;
            case '--orderBy':
                options.dataOptions.orderBy = value;
                break;
            case '--orderDirection':
                options.dataOptions.orderDirection = value.toUpperCase();
                break;
            case '--where':
                options.dataOptions.whereClause = value;
                break;
            case '--truncateFirst':
                options.dataOptions.truncateFirst = true;
                i--; // No value for this flag
                break;
            case '--skipDuplicates':
                options.dataOptions.skipDuplicates = true;
                i--; // No value for this flag
                break;
        }
    }

    copyTableComplete(tableName, options)
        .then(() => {
            console.log('✅ All operations completed successfully!');
        })
        .catch((error) => {
            console.error('❌ Operation failed:', error.message);
            process.exit(1);
        });
}