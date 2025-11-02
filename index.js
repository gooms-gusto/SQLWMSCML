#!/usr/bin/env node

const { initializeConnections, closeConnections } = require('./database');
const { copyTableStructure, copyTableData, copyCustomQuery, syncTable } = require('./data-utilities');

function printUsage() {
    console.log(`
MySQL Data Utility - Usage:

Node.js utility for transferring schema and data between MySQL databases

Commands:

1. Copy table structure only (target table must NOT exist):
   node index.js copy-structure <source_table> <target_table>

2. Copy table data only (target table must exist and be EMPTY):
   node index.js copy-data <source_table> <target_table> [limit]

3. Copy both structure and data (target table must NOT exist):
   node index.js copy-table <source_table> <target_table> [limit]

4. Execute custom query and insert results:
   node index.js custom-query "<SELECT_statement>" <target_table>

5. Sync table data from DB1 to DB2 (copy only missing records):
   node index.js sync-table <table_name>

Examples:
  node index.js copy-structure users users_backup      # Creates new empty table
  node index.js copy-data users users_backup 1000      # Copies data to existing empty table
  node index.js copy-table users users_copy             # Creates new table and copies all data
  node index.js custom-query "SELECT * FROM users WHERE active = 1" active_users
  node index.js sync-table users                        # Syncs missing records between existing tables
`);
}

async function main() {
    const args = process.argv.slice(2);

    if (args.length === 0) {
        printUsage();
        process.exit(0);
    }

    const command = args[0];

    try {
        await initializeConnections();

        switch (command) {
            case 'copy-structure':
                if (args.length !== 3) {
                    console.error('[ERROR] copy-structure requires source_table and target_table arguments');
                    process.exit(1);
                }
                await copyTableStructure(args[1], args[2]);
                break;

            case 'copy-data':
                if (args.length < 3) {
                    console.error('[ERROR] copy-data requires source_table and target_table arguments');
                    process.exit(1);
                }
                const limit = args[3] ? parseInt(args[3]) : undefined;
                await copyTableData(args[1], args[2], limit);
                break;

            case 'copy-table':
                if (args.length < 3) {
                    console.error('[ERROR] copy-table requires source_table and target_table arguments');
                    process.exit(1);
                }
                const tableLimit = args[3] ? parseInt(args[3]) : undefined;

                console.log('[STEP 1] Copying table structure...');
                await copyTableStructure(args[1], args[2]);

                console.log('[STEP 2] Copying table data...');
                await copyTableData(args[1], args[2], tableLimit);
                break;

            case 'custom-query':
                if (args.length !== 3) {
                    console.error('[ERROR] custom-query requires SELECT_statement and target_table arguments');
                    process.exit(1);
                }
                await copyCustomQuery(args[1], args[2]);
                break;

            case 'sync-table':
                if (args.length !== 2) {
                    console.error('[ERROR] sync-table requires table_name argument');
                    process.exit(1);
                }
                await syncTable(args[1]);
                break;

            default:
                console.error(`[ERROR] Unknown command: ${command}`);
                printUsage();
                process.exit(1);
        }

        console.log('[SUCCESS] Operation completed successfully');

    } catch (error) {
        console.error('[FATAL ERROR]', error.message);
        process.exit(1);
    } finally {
        await closeConnections();
    }
}

if (require.main === module) {
    main().catch(error => {
        console.error('[FATAL ERROR]', error.message);
        process.exit(1);
    });
}

module.exports = { main };