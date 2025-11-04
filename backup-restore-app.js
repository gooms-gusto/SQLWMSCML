#!/usr/bin/env node

const fs = require('fs').promises;
const path = require('path');
const readline = require('readline');
const { initializeConnections, closeConnections, getDB1Pool, getDB2Pool } = require('./database');

class BackupRestoreApp {
    constructor() {
        this.rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });
    }

    async question(query) {
        return new Promise(resolve => this.rl.question(query, resolve));
    }

    async showMenu() {
        console.log('\n' + '='.repeat(60));
        console.log('🗄️  MySQL Backup & Restore Utility');
        console.log('='.repeat(60));
        console.log('1. 📤 Backup database (Custom query to CSV)');
        console.log('2. 📥 Restore database (CSV to table)');
        console.log('3. 🔙 Test database connections');
        console.log('4. ❌ Exit');
        console.log('='.repeat(60));
    }

    async selectDatabase() {
        console.log('\n📋 Select Database Connection:');
        console.log('1. DB1 (Source: 172.31.9.92:63306)');
        console.log('2. DB2 (Target: 192.168.50.38:3306)');

        const choice = await this.question('Enter choice (1 or 2): ');

        switch (choice) {
            case '1':
                return { pool: getDB1Pool(), name: 'DB1', config: 'Source Database' };
            case '2':
                return { pool: getDB2Pool(), name: 'DB2', config: 'Target Database' };
            default:
                throw new Error('Invalid database selection');
        }
    }

    async backupDatabase() {
        try {
            console.log('\n📤 Backup Database - Custom Query to CSV');
            console.log('-'.repeat(50));

            // Select database
            const db = await this.selectDatabase();
            console.log(`✅ Selected: ${db.config} (${db.name})`);

            // Get custom query from user
            const query = await this.question('\nEnter your SELECT query: ');

            if (!query.trim().toLowerCase().startsWith('select')) {
                throw new Error('Query must start with SELECT');
            }

            // Get output filename
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
            const defaultFilename = `backup_${timestamp}.csv`;
            const filename = await this.question(`Enter output filename (default: ${defaultFilename}): `);
            const finalFilename = filename.trim() || defaultFilename;

            console.log('\n🔄 Executing backup...');

            // Execute backup
            const connection = await db.pool.getConnection();

            try {
                // Get column names first
                const describeQuery = `SELECT * FROM (${query}) AS temp_table LIMIT 1`;
                const [rows] = await connection.execute(describeQuery);

                if (rows.length === 0) {
                    throw new Error('Query returned no results');
                }

                const columns = Object.keys(rows[0]);

                // Create CSV content
                let csvContent = columns.join(',') + '\n';

                // Get all data
                const [allRows] = await connection.execute(query);

                for (const row of allRows) {
                    const values = columns.map(col => {
                        const value = row[col];
                        if (value === null || value === undefined) {
                            return '';
                        }

                        let stringValue;
                        // Handle Date objects properly
                        if (value instanceof Date) {
                            if (!isNaN(value.getTime())) {
                                // Format as MySQL datetime
                                const year = value.getFullYear();
                                const month = String(value.getMonth() + 1).padStart(2, '0');
                                const day = String(value.getDate()).padStart(2, '0');
                                const hours = String(value.getHours()).padStart(2, '0');
                                const minutes = String(value.getMinutes()).padStart(2, '0');
                                const seconds = String(value.getSeconds()).padStart(2, '0');
                                stringValue = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                            } else {
                                stringValue = '';
                            }
                        } else {
                            stringValue = String(value);
                        }

                        // Escape commas and quotes in values
                        return `"${stringValue.replace(/"/g, '""')}"`;
                    });
                    csvContent += values.join(',') + '\n';
                }

                // Write to file
                await fs.writeFile(finalFilename, csvContent);

                console.log(`✅ Backup completed successfully!`);
                console.log(`📁 File saved: ${finalFilename}`);
                console.log(`📊 Records exported: ${allRows.length}`);
                console.log(`📋 Columns: ${columns.join(', ')}`);

            } finally {
                connection.release();
            }

        } catch (error) {
            console.error('❌ Backup failed:', error.message);
            throw error;
        }
    }

    async restoreDatabase() {
        try {
            console.log('\n📥 Restore Database - CSV to Table');
            console.log('-'.repeat(50));

            // Select database
            const db = await this.selectDatabase();
            console.log(`✅ Selected: ${db.config} (${db.name})`);

            // Get CSV file path
            const csvPath = await this.question('Enter CSV file path: ');

            // Check if file exists
            try {
                await fs.access(csvPath);
            } catch {
                throw new Error('CSV file not found');
            }

            // Get target table name
            const tableName = await this.question('Enter target table name: ');

            if (!tableName.trim()) {
                throw new Error('Table name cannot be empty');
            }

            // Read CSV file
            const csvContent = await fs.readFile(csvPath, 'utf8');
            const lines = csvContent.trim().split('\n');

            if (lines.length < 2) {
                throw new Error('CSV file must have header and at least one data row');
            }

            const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));

            console.log(`\n📋 CSV Headers: ${headers.join(', ')}`);
            console.log(`📊 Data rows: ${lines.length - 1}`);

            const confirm = await this.question(`\nContinue restoring to table '${tableName}'? (y/N): `);

            if (confirm.toLowerCase() !== 'y' && confirm.toLowerCase() !== 'yes') {
                console.log('❌ Restore cancelled');
                return;
            }

            console.log('\n🔄 Executing restore...');

            const connection = await db.pool.getConnection();

            try {
                // Get table schema to identify datetime columns
                console.log('🔍 Analyzing table structure...');
                const tableSchema = await this.getTableSchema(connection, tableName);

                // Read and parse CSV data
                const dataRows = [];

                for (let i = 1; i < lines.length; i++) {
                    const values = this.parseCSVLine(lines[i]);
                    if (values.length === headers.length) {
                        const row = {};
                        headers.forEach((header, index) => {
                            let value = values[index] || null;

                            // Parse datetime columns
                            if (value && tableSchema[header]) {
                                const dataType = tableSchema[header].toLowerCase();
                                if (dataType.includes('datetime') ||
                                    dataType.includes('timestamp') ||
                                    dataType.includes('date')) {
                                    value = this.parseDateForMySQL(value);
                                }
                            }

                            row[header] = value;
                        });
                        dataRows.push(row);
                    }
                }

                if (dataRows.length === 0) {
                    throw new Error('No valid data rows found in CSV');
                }

                // Insert data in batches
                const batchSize = 1000;
                let insertedCount = 0;

                for (let i = 0; i < dataRows.length; i += batchSize) {
                    const batch = dataRows.slice(i, i + batchSize);

                    const placeholders = headers.map(() => '?').join(', ');
                    const sql = `INSERT INTO ${tableName} (${headers.join(', ')}) VALUES (${placeholders})`;

                    for (const row of batch) {
                        const values = headers.map(header => row[header]);
                        await connection.execute(sql, values);
                        insertedCount++;
                    }

                    console.log(`📝 Processed ${Math.min(i + batchSize, dataRows.length)} / ${dataRows.length} rows`);
                }

                console.log(`✅ Restore completed successfully!`);
                console.log(`📊 Records inserted: ${insertedCount}`);
                console.log(`🎯 Target table: ${tableName}`);
                if (Object.keys(tableSchema).length > 0) {
                    const datetimeCols = Object.keys(tableSchema).filter(col =>
                        tableSchema[col].toLowerCase().includes('datetime') ||
                        tableSchema[col].toLowerCase().includes('timestamp') ||
                        tableSchema[col].toLowerCase().includes('date')
                    );
                    if (datetimeCols.length > 0) {
                        console.log(`📅 Date columns processed: ${datetimeCols.join(', ')}`);
                    }
                }

            } finally {
                connection.release();
            }

        } catch (error) {
            console.error('❌ Restore failed:', error.message);
            throw error;
        }
    }

    parseCSVLine(line) {
        const result = [];
        let current = '';
        let inQuotes = false;

        for (let i = 0; i < line.length; i++) {
            const char = line[i];

            if (char === '"') {
                if (inQuotes && line[i + 1] === '"') {
                    current += '"';
                    i++; // Skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (char === ',' && !inQuotes) {
                result.push(current);
                current = '';
            } else {
                current += char;
            }
        }

        result.push(current);
        return result;
    }

    parseDateForMySQL(dateString) {
        if (!dateString || dateString.trim() === '') {
            return null;
        }

        // Check if already in MySQL datetime format
        if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(dateString)) {
            return dateString;
        }

        // Check if just a date (YYYY-MM-DD)
        if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
            return dateString + ' 00:00:00';
        }

        // Handle JavaScript Date.toString() format
        if (dateString.includes('GMT') && dateString.includes('(')) {
            try {
                const date = new Date(dateString);
                if (!isNaN(date.getTime())) {
                    // Format as MySQL datetime
                    const year = date.getFullYear();
                    const month = String(date.getMonth() + 1).padStart(2, '0');
                    const day = String(date.getDate()).padStart(2, '0');
                    const hours = String(date.getHours()).padStart(2, '0');
                    const minutes = String(date.getMinutes()).padStart(2, '0');
                    const seconds = String(date.getSeconds()).padStart(2, '0');
                    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                }
            } catch (error) {
                console.warn(`⚠️  Could not parse date: ${dateString}`);
            }
        }

        // Handle ISO date strings
        if (dateString.includes('T') || dateString.includes('Z')) {
            try {
                const date = new Date(dateString);
                if (!isNaN(date.getTime())) {
                    const year = date.getFullYear();
                    const month = String(date.getMonth() + 1).padStart(2, '0');
                    const day = String(date.getDate()).padStart(2, '0');
                    const hours = String(date.getHours()).padStart(2, '0');
                    const minutes = String(date.getMinutes()).padStart(2, '0');
                    const seconds = String(date.getSeconds()).padStart(2, '0');
                    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                }
            } catch (error) {
                console.warn(`⚠️  Could not parse ISO date: ${dateString}`);
            }
        }

        // Handle common formats: MM/DD/YYYY, DD/MM/YYYY
        if (/^\d{1,2}[\/-]\d{1,2}[\/-]\d{4}/.test(dateString)) {
            try {
                const date = new Date(dateString);
                if (!isNaN(date.getTime())) {
                    const year = date.getFullYear();
                    const month = String(date.getMonth() + 1).padStart(2, '0');
                    const day = String(date.getDate()).padStart(2, '0');
                    const hours = String(date.getHours()).padStart(2, '0');
                    const minutes = String(date.getMinutes()).padStart(2, '0');
                    const seconds = String(date.getSeconds()).padStart(2, '0');
                    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                }
            } catch (error) {
                console.warn(`⚠️  Could not parse common date format: ${dateString}`);
            }
        }

        console.warn(`⚠️  Unknown date format, using as-is: ${dateString}`);
        return dateString;
    }

    async getTableSchema(connection, tableName) {
        try {
            const [columns] = await connection.execute(
                `SELECT COLUMN_NAME, DATA_TYPE
                 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE()
                 AND TABLE_NAME = ?`,
                [tableName]
            );

            const schema = {};
            columns.forEach(col => {
                schema[col.COLUMN_NAME] = col.DATA_TYPE;
            });
            return schema;
        } catch (error) {
            console.warn(`⚠️  Could not retrieve table schema for ${tableName}: ${error.message}`);
            return {};
        }
    }

    async testConnections() {
        try {
            console.log('\n🔧 Testing Database Connections');
            console.log('-'.repeat(40));

            // Test DB1
            console.log('Testing DB1 connection...');
            try {
                const db1Conn = await getDB1Pool().getConnection();
                await db1Conn.ping();
                const [rows] = await db1Conn.execute('SELECT 1 as test');
                db1Conn.release();
                console.log('✅ DB1 (Source): Connection successful');
            } catch (error) {
                console.log(`❌ DB1 (Source): Connection failed - ${error.message}`);
            }

            // Test DB2
            console.log('Testing DB2 connection...');
            try {
                const db2Conn = await getDB2Pool().getConnection();
                await db2Conn.ping();
                const [rows] = await db2Conn.execute('SELECT 1 as test');
                db2Conn.release();
                console.log('✅ DB2 (Target): Connection successful');
            } catch (error) {
                console.log(`❌ DB2 (Target): Connection failed - ${error.message}`);
            }

        } catch (error) {
            console.error('❌ Connection test failed:', error.message);
        }
    }

    async run() {
        try {
            console.log('🔄 Initializing database connections...');
            await initializeConnections();
            console.log('✅ Database connections initialized\n');

            while (true) {
                await this.showMenu();

                const choice = await this.question('Enter your choice (1-4): ');

                try {
                    switch (choice) {
                        case '1':
                            await this.backupDatabase();
                            break;
                        case '2':
                            await this.restoreDatabase();
                            break;
                        case '3':
                            await this.testConnections();
                            break;
                        case '4':
                            console.log('\n👋 Goodbye!');
                            return;
                        default:
                            console.log('\n❌ Invalid choice. Please try again.');
                    }
                } catch (error) {
                    console.error('\n❌ Error:', error.message);
                }

                if (choice !== '4') {
                    await this.question('\nPress Enter to continue...');
                }
            }

        } catch (error) {
            console.error('❌ Application error:', error.message);
        } finally {
            await closeConnections();
            this.rl.close();
        }
    }
}

// Run the application if called directly
if (require.main === module) {
    const app = new BackupRestoreApp();
    app.run().catch(error => {
        console.error('❌ Fatal error:', error.message);
        process.exit(1);
    });
}

module.exports = BackupRestoreApp;