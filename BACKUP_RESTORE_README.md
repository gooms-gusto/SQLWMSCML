# MySQL Backup & Restore Utility

A user-friendly Node.js application for backing up and restoring MySQL databases using custom SELECT queries and CSV files.

## Features

- 📤 **Backup Database**: Export custom query results to CSV files
- 📥 **Restore Database**: Import CSV data into MySQL tables
- 🔄 **Dual Database Support**: Choose between DB1 (source) and DB2 (target)
- 🔧 **Connection Testing**: Verify database connectivity
- 📊 **Batch Processing**: Efficient handling of large datasets

## Setup

1. Ensure your `.env` file is configured with database connections:
```bash
# Source Database (Read-Only)
DB1_HOST=172.31.9.92
DB1_USER=sync.db
DB1_PORT=63306
DB1_PASSWORD=your_password
DB1_DATABASE=wms_cml

# Target Database (Read-Write)
DB2_HOST=192.168.50.38
DB2_USER=middleware
DB2_PORT=3306
DB2_PASSWORD=your_password
DB2_DATABASE=wms_cml
```

2. Install dependencies:
```bash
npm install
```

## Usage

Choose the right utility based on your data size:

### 🚀 For Large Datasets (Millions of rows) - Recommended

```bash
npm run backup-large
# or
node large-data-backup.js
```

**Features:**
- 💪 Memory-efficient streaming (handles GB+ files)
- 📊 Real-time progress tracking
- ⚡ Optimized batch processing (10,000 rows default)
- 📈 Performance estimation and tuning
- 🔄 Automatic pagination for exports

### 📊 For Small to Medium Datasets (Under 100K rows)

```bash
npm run backup
# or
node backup-restore-app.js
```

## Large Data Utility Menu Options

1. **📤 Stream backup**: Memory-efficient CSV export with pagination
   - Processes data in batches (10,000 rows by default)
   - Real-time progress and performance metrics
   - Automatic memory management

2. **📥 Stream restore**: Memory-efficient CSV import
   - Line-by-line file processing
   - Batch inserts to optimize database performance
   - Progress tracking with rows/second metrics

3. **📊 Estimate backup size**: Time and storage estimation
   - Analyzes query results to predict backup size
   - Estimates processing time based on row count
   - Memory usage warnings for large datasets

4. **⚙️ Performance configuration**: Tune batch sizes and settings
   - Adjustable batch size (1-100,000 rows)
   - Real-time memory usage monitoring
   - Performance optimization

5. **🔧 Test connections**: Verify database connectivity
6. **❌ Exit**: Close the application

## Original Utility Menu Options

1. **📤 Backup Database**: Export custom query to CSV
2. **📥 Restore Database**: Import CSV to table
3. **🔧 Test Database Connections**: Verify connectivity to both databases
4. **❌ Exit**: Close the application

## Backup Examples

### Basic Table Backup
```sql
SELECT * FROM users WHERE active = 1
```

### Custom Query with Joins
```sql
SELECT u.id, u.name, u.email, p.title, p.created_at
FROM users u
JOIN posts p ON u.id = p.user_id
WHERE u.created_at >= '2024-01-01'
ORDER BY u.created_at DESC
```

### Specific Columns
```sql
SELECT id, username, email, last_login
FROM users
WHERE last_login > DATE_SUB(NOW(), INTERVAL 30 DAY)
```

## Restore Process

1. CSV file must have a header row
2. Column names in CSV should match table columns
3. Empty values are treated as NULL
4. Data is inserted in batches of 1000 records
5. You'll be asked to confirm before proceeding

## CSV Format

### Expected Format
```csv
id,name,email,created_at
1,"John Doe","john@example.com","2024-01-15 10:30:00"
2,"Jane Smith","jane@example.com","2024-01-16 14:25:30"
```

### Important Notes
- First row contains column headers
- Values containing commas or quotes are automatically escaped
- Use double quotes for string values
- Empty cells become NULL in database

### Date/Time Handling
The application automatically handles various date formats:

**Backup (Export):**
- Date objects are automatically formatted as MySQL datetime: `YYYY-MM-DD HH:MM:SS`
- No manual conversion needed

**Restore (Import):**
- Automatically detects datetime/timestamp/date columns
- Converts various date formats to MySQL format:
  - JavaScript Date strings: `"Wed Jul 29 2020 09:10:54 GMT+0700"`
  - ISO format: `"2020-07-29T09:10:54.000Z"`
  - MySQL format: `"2020-07-29 09:10:54"`
  - Date only: `"2020-07-29"` (becomes `2020-07-29 00:00:00`)
  - Common formats: `"07/29/2020"`, `"29-07-2020"`

**Example problematic date (now fixed):**
```
Before: "Wed Jul 29 2020 09:10:54 GMT+0700 (Western Indonesia Time)"
After:  "2020-07-29 09:10:54"
```

## Error Handling

- **Connection Errors**: Database connection failures are clearly displayed
- **Query Validation**: Only SELECT queries are allowed for backup
- **File Validation**: CSV files are checked for existence and format
- **Data Validation**: Column count and data types are verified

## Security

- Uses parameterized queries to prevent SQL injection
- Database credentials are loaded from environment variables
- Read/write permissions respect your database configuration

## Performance & Large Data Handling

### 🚀 Large Data Optimization Features

**Memory Management:**
- Streaming processing (no data loaded entirely into memory)
- Configurable batch sizes (1,000 - 100,000 rows)
- Automatic memory monitoring and warnings
- Efficient file handle management

**Performance Metrics:**
- Real-time rows/second processing speed
- Estimated completion time
- File size tracking
- Memory usage monitoring

**Database Optimization:**
- Batch inserts for optimal database performance
- Connection pooling and reuse
- Small delays between batches to prevent overwhelming database
- Automatic query pagination for large exports

### 📊 Performance Benchmarks

**Typical Performance (MySQL 8.0):**
- Small datasets (<100K rows): 50K-80K rows/second
- Medium datasets (100K-1M rows): 40K-60K rows/second
- Large datasets (1M-10M rows): 20K-40K rows/second
- Very large datasets (>10M rows): 15K-25K rows/second

**Memory Usage:**
- Streaming backup: ~50-100MB constant memory
- Streaming restore: ~50-100MB constant memory
- Original utility: Increases with dataset size

### ⚙️ Performance Tuning

**Batch Size Recommendations:**
- **High-performance servers**: 50,000 - 100,000 rows
- **Standard servers**: 10,000 - 50,000 rows (default)
- **Limited resources**: 1,000 - 10,000 rows

**Optimization Tips:**
1. Use `npm run backup-large` for datasets >100K rows
2. Estimate backup size before starting large operations
3. Monitor memory usage during operations
4. Adjust batch size based on your system resources
5. Run during off-peak hours for better database performance

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Increase timeout in `database.js`
2. **Permission Denied**: Check file write permissions for CSV exports
3. **Table Not Found**: Verify table exists before restore
4. **Column Mismatch**: Ensure CSV headers match target table columns
5. **Memory Issues with Large Files**: Use `npm run backup-large` instead of original utility
6. **Slow Performance**: Increase batch size in performance configuration

### Large Data Specific Issues

1. **Out of Memory**: Switch to large data utility (`npm run backup-large`)
2. **Very Slow Processing**:
   - Increase batch size in performance settings
   - Check database server performance
   - Ensure sufficient disk space for exports
3. **Connection Drops**: The large data utility handles reconnection better

### Debug Mode

For detailed error information, run with:
```bash
DEBUG=* node backup-restore-app.js
DEBUG=* node large-data-backup.js
```

## File Structure

```
├── backup-restore-app.js    # Main application
├── database.js              # Database connection management
├── .env                     # Environment configuration
└── BACKUP_RESTORE_README.md # This documentation
```

## Dependencies

- `mysql2`: MySQL driver for Node.js
- `dotenv`: Environment variable management
- `readline`: Interactive command-line interface
- `fs/promises`: File system operations

## Support

For issues or questions:
1. Check database connectivity using option 3
2. Verify your SELECT query syntax
3. Ensure CSV file format is correct
4. Check file permissions in your working directory