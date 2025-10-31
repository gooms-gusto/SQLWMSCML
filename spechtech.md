Node.js MySQL Data Utility: Functional Specification

1. Overview

This document outlines the functional specifications for a Node.js utility designed to transfer schema (table structures) and data between two separate MySQL 8.0 database servers.

The utility will connect to a source database (DB1) in a read-only capacity and a target database (DB2) with write permissions to replicate structures and insert data.

2. Technical Stack

Runtime: Node.js (v18.x or later)

Database: MySQL 8.0

Key NPM Packages:

mysql2/promise: For modern, async/await-based MySQL connections and queries.

dotenv: For managing database credentials securely.

3. Configuration

Database connection details for both servers will be managed via a .env file in the project root. The application will read this file to establish connections.

Example .env file:

# Source Database (Read-Only)
DB1_HOST=172.31.9.92
DB1_USER=sync.db
DB1_PORT=63306
DB1_PASSWORD=U8g01o8s*17u89010
DB1_DATABASE=wms_cml

# Target Database (Read-Write)
DB2_HOST=omahkudewe.asia
DB2_USER=middleware
DB1_PORT=63306
DB2_PASSWORD=U8g01o8s*17
DB2_DATABASE=wms_cml


The application will initialize two mysql2/promise connection pools, one for DB1 and one for DB2, on startup.

4. Function Specifications

This section details the core functions of the utility.

Function 1: copyTableStructure

Copies the CREATE TABLE statement from a table in DB1 and executes it in DB2 to create a new, empty table.

/**
 * Fetches the CREATE TABLE statement for a table in DB1, modifies it
 * with a new table name, and executes it on DB2 to create the table.
 *
 * @param {string} sourceTableName The name of the table to copy from DB1.
 * @param {string} targetTableName The desired name for the new table in DB2.
 * @returns {Promise<void>} A promise that resolves when the table is created.
 */
async function copyTableStructure(sourceTableName, targetTableName) {
    // 1. Get a connection from the DB1 pool.
    // 2. Execute: SHOW CREATE TABLE \`${sourceTableName}\`;
    // 3. Extract the 'Create Table' string from the result.
    // 4. Use a regular expression to replace the original table name:
    //    e.g., `CREATE TABLE \`${sourceTableName}\`` becomes `CREATE TABLE \`${targetTableName}\``
    // 5. Get a connection from the DB2 pool.
    // 6. Execute the modified CREATE TABLE statement on DB2.
    // 7. Release both connections.
}


Function 2: copyTableData

Selects data from a table in DB1 and bulk-inserts it into a specified table in DB2. Supports row limiting.

/**
 * Copies data from a source table in DB1 to a target table in DB2.
 * Assumes the target table structure is compatible.
 *
 * @param {string} sourceTableName The name of the table to select from in DB1.
 * @param {string} targetTableName The name of the table to insert into in DB2.
 * @param {number} [limit] An optional number of rows to limit the copy.
 * @returns {Promise<{copiedRows: number}>} A promise that resolves with the count of copied rows.
 */
async function copyTableData(sourceTableName, targetTableName, limit) {
    // 1. Get a connection from the DB1 pool.
    // 2. Build the SELECT query:
    //    let selectQuery = `SELECT * FROM \`${sourceTableName}\``;
    //    if (limit && Number.isInteger(limit) && limit > 0) {
    //        selectQuery += ` LIMIT ${limit}`;
    //    }
    // 3. Execute the SELECT query on DB1: `const [rows, fields] = await db1.query(selectQuery);`
    // 4. If `rows.length === 0`, return { copiedRows: 0 }.
    //
    // 5. Prepare for bulk insert:
    //    - Get column names: `const columns = fields.map(f => f.name);`
    //    - Format column list for SQL: `const colNamesSQL = columns.map(c => \`\${c}\`).join(', ');`
    //    - Map row objects to arrays: `const values = rows.map(row => columns.map(col => row[col]));`
    //
    // 6. Build the INSERT query:
    //    `const insertQuery = \`INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ?\`;`
    //
    // 7. Get a connection from the DB2 pool.
    // 8. Start a transaction on DB2: `await db2.beginTransaction();`
    // 9. Try to execute the bulk insert: `const [result] = await db2.query(insertQuery, [values]);`
    // 10. If successful, commit: `await db2.commit();`
    // 11. Return { copiedRows: result.affectedRows }.
    // 12. If any error, rollback: `await db2.rollback();` and throw the error.
    // 13. Release both connections.
}


Function 3: copyCustomQuery

Executes a user-provided SELECT query on DB1 and inserts the results into a specified table in DB2.

/**
 * Executes a custom SELECT query on DB1 and inserts the results into a table on DB2.
 *
 * @param {string} selectQuery The full, custom SELECT statement to run on DB1.
 * @param {string} targetTableName The name of the table to insert the results into in DB2.
 * @returns {Promise<{copiedRows: number}>} A promise that resolves with the count of copied rows.
 */
async function copyCustomQuery(selectQuery, targetTableName) {
    // 1. Get a connection from the DB1 pool.
    // 2. Execute the user's query: `const [rows, fields] = await db1.query(selectQuery);`
    //    - Note: This is a potential SQL injection risk if the user is not trusted.
    //    - The DB1 user's permissions *must* be strictly read-only.
    //
    // 3. If `rows.length === 0`, return { copiedRows: 0 }.
    //
    // 4. Prepare for bulk insert (same as copyTableData):
    //    - Get column names: `const columns = fields.map(f => f.name);`
    //    - Format column list for SQL: `const colNamesSQL = columns.map(c => \`\${c}\`).join(', ');`
    //    - Map row objects to arrays: `const values = rows.map(row => columns.map(col => row[col]));`
    //
    // 5. Build the INSERT query:
    //    `const insertQuery = \`INSERT INTO \`${targetTableName}\` (${colNamesSQL}) VALUES ?\`;`
    //
    // 6. Get a connection from the DB2 pool.
    // 7. Start a transaction on DB2: `await db2.beginTransaction();`
    // 8. Try to execute the bulk insert: `const [result] = await db2.query(insertQuery, [values]);`
    // 9. If successful, commit: `await db2.commit();`
    // 10. Return { copiedRows: result.affectedRows }.
    // 11. If any error, rollback: `await db2.rollback();` and throw the error.
    // 12. Release both connections.
}


5. Error Handling

All asynchronous functions will use async/await and be wrapped in try...catch blocks.

Errors will be logged to the console with clear prefixes (e.g., [DB1_ERROR], [DB2_ERROR]).

Functions will re-throw errors to be handled by the caller (e.g., the main script or API endpoint).

All write operations to DB2 (schema changes, data inserts) must be wrapped in transactions. If any part of the operation fails, a ROLLBACK will be issued to prevent partial data.

6. Security Considerations

DB1 (Source): The MySQL user specified in .env for DB1 MUST have read-only permissions. Recommended permissions: SELECT, SHOW VIEW, SHOW CREATE TABLE. This is critical to enforcing requirement #6.

DB2 (Target): The MySQL user for DB2 will require CREATE, INSERT, ALTER, and TRUNCATE permissions on the target database.

SQL Injection:

Data: All data values will be parameterized using the mysql2 library's ? placeholder, which prevents data-based SQLi.

Identifiers: Table and column names cannot be parameterized. The application must sanitize or escape these identifiers (e.g., by wrapping them in backticks ``). The copyCustomQuery function carries inherent risk and should only be exposed to trusted users.