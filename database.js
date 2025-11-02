const mysql = require('mysql2/promise');
require('dotenv').config();

const db1Config = {
    host: process.env.DB1_HOST,
    user: process.env.DB1_USER,
    port: process.env.DB1_PORT,
    password: process.env.DB1_PASSWORD,
    database: process.env.DB1_DATABASE,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 60000,
    acquireTimeout: 60000
};

const db2Config = {
    host: process.env.DB2_HOST,
    user: process.env.DB2_USER,
    port: process.env.DB2_PORT,
    password: process.env.DB2_PASSWORD,
    database: process.env.DB2_DATABASE,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 60000,
    acquireTimeout: 60000
};

let db1Pool = null;
let db2Pool = null;

async function initializeConnections() {
    try {
        console.log('[INFO] Initializing database connections...');

        db1Pool = mysql.createPool(db1Config);
        db2Pool = mysql.createPool(db2Config);

        console.log('[INFO] Testing DB1 connection...');
        const db1Conn = await db1Pool.getConnection();
        await db1Conn.ping();
        db1Conn.release();
        console.log('[SUCCESS] DB1 connection established');

        console.log('[INFO] Testing DB2 connection...');
        const db2Conn = await db2Pool.getConnection();
        await db2Conn.ping();
        db2Conn.release();
        console.log('[SUCCESS] DB2 connection established');

    } catch (error) {
        console.error('[ERROR] Failed to initialize database connections:', error.message);
        throw error;
    }
}

function getDB1Pool() {
    if (!db1Pool) {
        throw new Error('[ERROR] DB1 pool not initialized. Call initializeConnections() first.');
    }
    return db1Pool;
}

function getDB2Pool() {
    if (!db2Pool) {
        throw new Error('[ERROR] DB2 pool not initialized. Call initializeConnections() first.');
    }
    return db2Pool;
}

async function closeConnections() {
    try {
        if (db1Pool) {
            await db1Pool.end();
            console.log('[INFO] DB1 pool closed');
        }
        if (db2Pool) {
            await db2Pool.end();
            console.log('[INFO] DB2 pool closed');
        }
    } catch (error) {
        console.error('[ERROR] Error closing connections:', error.message);
        throw error;
    }
}

module.exports = {
    initializeConnections,
    getDB1Pool,
    getDB2Pool,
    closeConnections
};