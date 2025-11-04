#!/usr/bin/env node

const fs = require('fs').promises;
const readline = require('readline');
const { initializeConnections, closeConnections, getDB1Pool, getDB2Pool } = require('./database');

class LargeDataBackupApp {
    constructor() {
        this.rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });
        this.BATCH_SIZE = 10000; // Optimized for large datasets
        this.PROGRESS_UPDATE_INTERVAL = 10000; // Update progress every 10k rows
    }

    async question(query) {
        return new Promise(resolve => this.rl.question(query, resolve));
    }

    async showMenu() {
        console.log('\n' + '='.repeat(70));
        console.log('🗄️  Large Data MySQL Backup & Restore Utility');
        console.log('='.repeat(70));
        console.log('💪 Optimized for huge datasets (millions of rows)');
        console.log('');
        console.log('1. 📤 Stream backup (Custom query to CSV - Memory efficient)');
        console.log('2. 📥 Stream restore (CSV to table - Memory efficient)');
        console.log('3. 🔍 Estimate backup size and time');
        console.log('4. ⚙️  Configure batch size and performance');
        console.log('5. 🔙 Test database connections');
        console.log('6. ❌ Exit');
        console.log('='.repeat(70));
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

    async estimateBackupSize() {
        try {
            console.log('\n📊 Estimate Backup Size and Time');
            console.log('-'.repeat(50));

            const db = await this.selectDatabase();
            const query = await this.question('Enter your SELECT query for estimation: ');

            if (!query.trim().toLowerCase().startsWith('select')) {
                throw new Error('Query must start with SELECT');
            }

            console.log('\n🔄 Estimating...');
            const connection = await db.pool.getConnection();

            try {
                // Get row count
                const countQuery = `SELECT COUNT(*) as total_rows FROM (${query}) as count_query`;
                const [countResult] = await connection.execute(countQuery);
                const totalRows = countResult[0].total_rows;

                // Get sample row to estimate average row size
                const sampleQuery = `SELECT * FROM (${query}) as sample_query LIMIT 100`;
                const [sampleRows] = await connection.execute(sampleQuery);

                if (sampleRows.length === 0) {
                    console.log('⚠️  Query returned no results');
                    return;
                }

                // Calculate average row size
                let totalSize = 0;
                for (const row of sampleRows) {
                    const rowString = JSON.stringify(row);
                    totalSize += rowString.length;
                }
                const avgRowSize = totalSize / sampleRows.length;

                // Estimate CSV size (with CSV overhead)
                const estimatedCSVSize = Math.round(totalRows * avgRowSize * 1.5); // 50% overhead for CSV formatting

                console.log('\n📈 Estimation Results:');
                console.log(`📊 Total rows: ${totalRows.toLocaleString()}`);
                console.log(`📏 Average row size: ${avgRowSize.toFixed(0)} bytes`);
                console.log(`💾 Estimated CSV size: ${this.formatFileSize(estimatedCSVSize)}`);

                // Estimate time based on row count
                let estimatedTime = '';
                let rowsPerSecond = 50000; // Base estimation

                if (totalRows < 100000) {
                    rowsPerSecond = 80000;
                } else if (totalRows < 1000000) {
                    rowsPerSecond = 60000;
                } else if (totalRows < 10000000) {
                    rowsPerSecond = 40000;
                } else {
                    rowsPerSecond = 20000;
                }

                const seconds = totalRows / rowsPerSecond;
                estimatedTime = this.formatDuration(seconds);

                console.log(`⏱️  Estimated time: ${estimatedTime}`);
                console.log(`🚀 Processing speed: ~${rowsPerSecond.toLocaleString()} rows/second`);

                // Memory usage warning
                const memoryMB = Math.round(estimatedCSVSize / (1024 * 1024));
                if (memoryMB > 1000) {
                    console.log(`\n⚠️  Warning: Large dataset detected (~${memoryMB} MB)`);
                    console.log('💡 This utility uses streaming to handle large files efficiently');
                }

            } finally {
                connection.release();
            }

        } catch (error) {
            console.error('❌ Estimation failed:', error.message);
        }
    }

    async streamBackup() {
        try {
            console.log('\n📤 Stream Backup - Memory Efficient CSV Export');
            console.log('-'.repeat(60));

            const db = await this.selectDatabase();
            console.log(`✅ Selected: ${db.config} (${db.name})`);

            const query = await this.question('\nEnter your SELECT query: ');

            if (!query.trim().toLowerCase().startsWith('select')) {
                throw new Error('Query must start with SELECT');
            }

            const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
            const defaultFilename = `large_backup_${timestamp}.csv`;
            const filename = await this.question(`Enter output filename (default: ${defaultFilename}): `);
            const finalFilename = filename.trim() || defaultFilename;

            console.log('\n🔄 Starting streaming backup...');
            console.log(`📁 Writing to: ${finalFilename}`);

            const connection = await db.pool.getConnection();
            const fileHandle = await fs.open(finalFilename, 'w');

            try {
                // Get column names first
                const describeQuery = `SELECT * FROM (${query}) AS temp_table LIMIT 1`;
                const [rows] = await connection.execute(describeQuery);

                if (rows.length === 0) {
                    throw new Error('Query returned no results');
                }

                const columns = Object.keys(rows[0]);

                // Write CSV header
                await fileHandle.writeFile(columns.join(',') + '\n');

                // Setup pagination for large dataset
                let offset = 0;
                let totalProcessed = 0;
                const startTime = Date.now();

                console.log(`📋 Columns: ${columns.join(', ')}`);
                console.log(`🚀 Batch size: ${this.BATCH_SIZE.toLocaleString()} rows`);

                while (true) {
                    const paginatedQuery = `${query} LIMIT ${this.BATCH_SIZE} OFFSET ${offset}`;
                    const [batchRows] = await connection.execute(paginatedQuery);

                    if (batchRows.length === 0) {
                        break;
                    }

                    // Process batch
                    let batchContent = '';
                    for (const row of batchRows) {
                        const values = columns.map(col => {
                            const value = row[col];
                            if (value === null || value === undefined) {
                                return '';
                            }

                            let stringValue;
                            if (value instanceof Date) {
                                if (!isNaN(value.getTime())) {
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

                            return `"${stringValue.replace(/"/g, '""')}"`;
                        });
                        batchContent += values.join(',') + '\n';
                    }

                    // Write batch to file
                    await fileHandle.writeFile(batchContent);

                    totalProcessed += batchRows.length;
                    offset += this.BATCH_SIZE;

                    // Progress update
                    if (totalProcessed % this.PROGRESS_UPDATE_INTERVAL === 0 || batchRows.length < this.BATCH_SIZE) {
                        const elapsed = (Date.now() - startTime) / 1000;
                        const rowsPerSecond = Math.round(totalProcessed / elapsed);
                        const progress = batchRows.length < this.BATCH_SIZE ? '100%' : 'Processing...';

                        console.log(`📝 ${progress} ${totalProcessed.toLocaleString()} rows processed (${rowsPerSecond.toLocaleString()} rows/sec)`);
                    }

                    // If this batch was smaller than requested, we're done
                    if (batchRows.length < this.BATCH_SIZE) {
                        break;
                    }

                    // Small delay to prevent overwhelming the database
                    await new Promise(resolve => setTimeout(resolve, 10));
                }

                const elapsed = (Date.now() - startTime) / 1000;
                const finalSpeed = Math.round(totalProcessed / elapsed);

                console.log(`\n✅ Streaming backup completed successfully!`);
                console.log(`📁 File: ${finalFilename}`);
                console.log(`📊 Total rows: ${totalProcessed.toLocaleString()}`);
                console.log(`⏱️  Time: ${this.formatDuration(elapsed)}`);
                console.log(`🚀 Final speed: ${finalSpeed.toLocaleString()} rows/sec`);

                // File size info
                const stats = await fs.stat(finalFilename);
                console.log(`💾 File size: ${this.formatFileSize(stats.size)}`);

            } finally {
                await fileHandle.close();
                connection.release();
            }

        } catch (error) {
            console.error('❌ Streaming backup failed:', error.message);
            throw error;
        }
    }

    async streamRestore() {
        try {
            console.log('\n📥 Stream Restore - Memory Efficient CSV Import');
            console.log('-'.repeat(60));

            const db = await this.selectDatabase();
            console.log(`✅ Selected: ${db.config} (${db.name})`);

            const csvPath = await this.question('Enter CSV file path: ');

            // Check if file exists and get file info
            try {
                const stats = await fs.stat(csvPath);
                console.log(`📊 File size: ${this.formatFileSize(stats.size)}`);
            } catch {
                throw new Error('CSV file not found');
            }

            const tableName = await this.question('Enter target table name: ');

            if (!tableName.trim()) {
                throw new Error('Table name cannot be empty');
            }

            // Get table schema
            const connection = await db.pool.getConnection();
            let tableSchema = {};

            try {
                console.log('🔍 Analyzing table structure...');
                const [columns] = await connection.execute(
                    `SELECT COLUMN_NAME, DATA_TYPE
                     FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_SCHEMA = DATABASE()
                     AND TABLE_NAME = ?`,
                    [tableName]
                );

                columns.forEach(col => {
                    tableSchema[col.COLUMN_NAME] = col.DATA_TYPE;
                });

                const datetimeCols = Object.keys(tableSchema).filter(col =>
                    tableSchema[col].toLowerCase().includes('datetime') ||
                    tableSchema[col].toLowerCase().includes('timestamp') ||
                    tableSchema[col].toLowerCase().includes('date')
                );

                if (datetimeCols.length > 0) {
                    console.log(`📅 Date columns detected: ${datetimeCols.join(', ')}`);
                }

            } catch (error) {
                console.warn(`⚠️  Could not retrieve table schema: ${error.message}`);
            }

            const confirm = await this.question(`\nContinue streaming restore to table '${tableName}'? (y/N): `);

            if (confirm.toLowerCase() !== 'y' && confirm.toLowerCase() !== 'yes') {
                console.log('❌ Restore cancelled');
                return;
            }

            console.log('\n🔄 Starting streaming restore...');

            try {
                let totalProcessed = 0;
                const startTime = Date.now();
                let headers = [];

                // Read file line by line
                const fileStream = fs.createReadStream(csvPath);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });

                let lineCount = 0;
                const batchBuffer = [];

                for await (const line of rl) {
                    lineCount++;

                    if (lineCount === 1) {
                        // Process header
                        headers = line.split(',').map(h => h.trim().replace(/"/g, ''));
                        console.log(`📋 CSV Headers: ${headers.join(', ')}`);
                        continue;
                    }

                    // Parse data line
                    const values = this.parseCSVLine(line);
                    if (values.length !== headers.length) {
                        console.warn(`⚠️  Skipping malformed line ${lineCount}: ${values.length} values, expected ${headers.length}`);
                        continue;
                    }

                    // Process row with date parsing
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

                    batchBuffer.push(row);

                    // Process batch when buffer is full
                    if (batchBuffer.length >= this.BATCH_SIZE) {
                        await this.insertBatch(connection, tableName, headers, batchBuffer);
                        totalProcessed += batchBuffer.length;
                        batchBuffer.length = 0;

                        // Progress update
                        if (totalProcessed % this.PROGRESS_UPDATE_INTERVAL === 0) {
                            const elapsed = (Date.now() - startTime) / 1000;
                            const rowsPerSecond = Math.round(totalProcessed / elapsed);
                            console.log(`📝 ${totalProcessed.toLocaleString()} rows inserted (${rowsPerSecond.toLocaleString()} rows/sec)`);
                        }
                    }
                }

                // Process remaining rows in buffer
                if (batchBuffer.length > 0) {
                    await this.insertBatch(connection, tableName, headers, batchBuffer);
                    totalProcessed += batchBuffer.length;
                }

                const elapsed = (Date.now() - startTime) / 1000;
                const finalSpeed = Math.round(totalProcessed / elapsed);

                console.log(`\n✅ Streaming restore completed successfully!`);
                console.log(`📊 Records inserted: ${totalProcessed.toLocaleString()}`);
                console.log(`🎯 Target table: ${tableName}`);
                console.log(`⏱️  Time: ${this.formatDuration(elapsed)}`);
                console.log(`🚀 Final speed: ${finalSpeed.toLocaleString()} rows/sec`);

            } finally {
                connection.release();
            }

        } catch (error) {
            console.error('❌ Streaming restore failed:', error.message);
            throw error;
        }
    }

    async insertBatch(connection, tableName, headers, batch) {
        const placeholders = headers.map(() => '?').join(', ');
        const sql = `INSERT INTO ${tableName} (${headers.join(', ')}) VALUES (${placeholders})`;

        for (const row of batch) {
            const values = headers.map(header => row[header]);
            await connection.execute(sql, values);
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
                    i++;
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

        if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(dateString)) {
            return dateString;
        }

        if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
            return dateString + ' 00:00:00';
        }

        if (dateString.includes('GMT') && dateString.includes('(')) {
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
                console.warn(`⚠️  Could not parse date: ${dateString}`);
            }
        }

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

    async configurePerformance() {
        console.log('\n⚙️  Performance Configuration');
        console.log('-'.repeat(40));

        const currentBatchSize = this.BATCH_SIZE;
        console.log(`Current batch size: ${currentBatchSize.toLocaleString()} rows`);

        const newBatchSize = await this.question('Enter new batch size (press Enter to keep current): ');

        if (newBatchSize && !isNaN(newBatchSize)) {
            const size = parseInt(newBatchSize);
            if (size > 0 && size <= 100000) {
                this.BATCH_SIZE = size;
                console.log(`✅ Batch size updated to ${size.toLocaleString()} rows`);

                // Update progress interval based on batch size
                this.PROGRESS_UPDATE_INTERVAL = Math.max(10000, this.BATCH_SIZE * 2);
            } else {
                console.log('❌ Invalid batch size. Must be between 1 and 100,000');
            }
        }

        // Show memory usage
        const memoryUsage = process.memoryUsage();
        console.log('\n💾 Current Memory Usage:');
        console.log(`   RSS: ${this.formatFileSize(memoryUsage.rss)}`);
        console.log(`   Heap Used: ${this.formatFileSize(memoryUsage.heapUsed)}`);
        console.log(`   Heap Total: ${this.formatFileSize(memoryUsage.heapTotal)}`);
    }

    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    formatDuration(seconds) {
        if (seconds < 60) {
            return `${seconds.toFixed(1)} seconds`;
        } else if (seconds < 3600) {
            const minutes = Math.floor(seconds / 60);
            const remainingSeconds = Math.round(seconds % 60);
            return `${minutes}m ${remainingSeconds}s`;
        } else {
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            return `${hours}h ${minutes}m`;
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

                const choice = await this.question('Enter your choice (1-6): ');

                try {
                    switch (choice) {
                        case '1':
                            await this.streamBackup();
                            break;
                        case '2':
                            await this.streamRestore();
                            break;
                        case '3':
                            await this.estimateBackupSize();
                            break;
                        case '4':
                            await this.configurePerformance();
                            break;
                        case '5':
                            await this.testConnections();
                            break;
                        case '6':
                            console.log('\n👋 Goodbye!');
                            return;
                        default:
                            console.log('\n❌ Invalid choice. Please try again.');
                    }
                } catch (error) {
                    console.error('\n❌ Error:', error.message);
                }

                if (choice !== '6') {
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
    const app = new LargeDataBackupApp();
    app.run().catch(error => {
        console.error('❌ Fatal error:', error.message);
        process.exit(1);
    });
}

module.exports = LargeDataBackupApp;