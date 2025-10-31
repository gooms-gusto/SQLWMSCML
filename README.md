# MySQL Data Utility

Node.js utility for transferring schema (table structures) and data between two separate MySQL 8.0 database servers.

## Features

- Copy table structures between databases
- Copy table data with optional row limiting
- Execute custom SELECT queries and insert results
- Transaction-based operations for data integrity
- Comprehensive error handling and logging

## Installation

1. Clone or download this project
2. Install dependencies:
   ```bash
   npm install
   ```
3. Configure database connections in `.env` file

## Configuration

The application uses a `.env` file for database connection settings:

```env
# Source Database (Read-Only)
DB1_HOST=172.31.9.92
DB1_USER=sync.db
DB1_PORT=63306
DB1_PASSWORD=your_password
DB1_DATABASE=wms_cml

# Target Database (Read-Write)
DB2_HOST=omahkudewe.asia
DB2_USER=middleware
DB2_PORT=63306
DB2_PASSWORD=your_password
DB2_DATABASE=wms_cml
```

## Usage

### Command Line Interface

```bash
# Copy table structure only
node index.js copy-structure <source_table> <target_table>

# Copy table data only (assumes target table exists)
node index.js copy-data <source_table> <target_table> [limit]

# Copy both structure and data
node index.js copy-table <source_table> <target_table> [limit]

# Execute custom query and insert results
node index.js custom-query "<SELECT_statement>" <target_table>
```

### Examples

```bash
# Copy structure of 'users' table to 'users_backup'
node index.js copy-structure users users_backup

# Copy 1000 rows from 'users' to 'users_backup'
node index.js copy-data users users_backup 1000

# Copy entire table (structure + data)
node index.js copy-table users users_copy

# Copy only active users to a new table
node index.js custom-query "SELECT * FROM users WHERE active = 1" active_users
```

## API

The utility also provides programmatic access through its modules:

```javascript
const { copyTableStructure, copyTableData, copyCustomQuery } = require('./data-utilities');

// Copy table structure
await copyTableStructure('source_table', 'target_table');

// Copy table data with optional limit
await copyTableData('source_table', 'target_table', 1000);

// Execute custom query
await copyCustomQuery('SELECT * FROM users WHERE active = 1', 'active_users');
```

## Table Name Case Sensitivity

**Important Note**: MySQL server configuration affects table name case sensitivity.

### MySQL `lower_case_table_names` Setting

- **Value 0**: Table names are stored as specified and comparison is case sensitive
- **Value 1** (Default on Windows): Table names are stored in lowercase but comparison is case insensitive
- **Value 2**: Table names are stored as given but comparison is case insensitive

### Current Behavior

When `lower_case_table_names=1` (common on Windows), the utility will:
- Accept any case for table names (e.g., `DOC_ORDER_HEADER`, `doc_order_header`, `Doc_Order_Header`)
- Create tables with lowercase names (e.g., `DOC_ORDER_HEADER` → `doc_order_header`)
- Provide clear feedback about the actual table name created
- Show informational messages about case conversion

### Example Output

```
[INFO] Table DOC_ORDER_HEADER created as 'doc_order_header' (MySQL converted to lowercase due to lower_case_table_names=1)
[NOTE] MySQL server has lower_case_table_names=1, which forces table names to lowercase on this platform
```

### Recommendations

1. **Use lowercase names** when working with MySQL on Windows platforms
2. **Check your MySQL configuration** with: `SHOW VARIABLES LIKE 'lower_case_table_names'`
3. **Be consistent** with table naming conventions across your environment

## Security Considerations

- **Source Database (DB1)**: Should have read-only permissions (SELECT, SHOW VIEW, SHOW CREATE TABLE)
- **Target Database (DB2)**: Requires CREATE, INSERT, ALTER, and TRUNCATE permissions
- **SQL Injection**: All data values are parameterized. Custom queries should only be used with trusted users
- **Credentials**: Store database credentials securely in `.env` file (never commit to version control)

## Error Handling

- All operations use transactions for data integrity
- Comprehensive error logging with clear prefixes
- Automatic connection pooling and cleanup
- Graceful error recovery with rollback capabilities

## Requirements

- Node.js 18.x or later
- MySQL 8.0
- NPM packages: mysql2, dotenv

## License

ISC