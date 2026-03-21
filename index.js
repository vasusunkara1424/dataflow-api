const express = require('express')
const cors = require('cors')
const { Pool } = require('pg')
const { WebSocketServer } = require('ws')
const { LogicalReplicationService, PgoutputPlugin } = require('pg-logical-replication')

const app = express()
app.use(cors())
app.use(express.json())

const pool = new Pool({
  database: 'dataflow_db',
  host: 'localhost',
  port: 5432,
})

// WebSocket server on port 4001
const wss = new WebSocketServer({ port: 4001 })
const clients = new Set()

wss.on('connection', (ws) => {
  clients.add(ws)
  console.log('🔌 Frontend connected via WebSocket')
  ws.on('close', () => clients.delete(ws))
})

function broadcast(event) {
  const msg = JSON.stringify(event)
  clients.forEach(ws => ws.send(msg))
}

// CDC Listener
const service = new LogicalReplicationService({
  database: 'dataflow_db',
  host: 'localhost',
  port: 5432,
})

const plugin = new PgoutputPlugin({
  protoVersion: 1,
  publicationNames: ['datagrid_pub'],
})

service.on('data', (lsn, log) => {
  if (log.tag === 'insert') {
    console.log('✅ INSERT:', log.relation.name, log.new)
    broadcast({ type: 'insert', table: log.relation.name, data: log.new })
  }
  if (log.tag === 'update') {
    console.log('✏️  UPDATE:', log.relation.name, log.new)
    broadcast({ type: 'update', table: log.relation.name, data: log.new })
  }
  if (log.tag === 'delete') {
    console.log('🗑️  DELETE:', log.relation.name)
    broadcast({ type: 'delete', table: log.relation.name, data: log.old })
  }
})

service.subscribe(plugin, 'datagrid_slot')
console.log('👂 CDC listener active...')

// REST Routes
app.get('/', (req, res) => {
  res.json({ message: 'DataFlow API running! ⚡', version: '2.0.0' })
})

app.get('/api/pipelines', async (req, res) => {
  const result = await pool.query('SELECT * FROM pipelines ORDER BY id')
  res.json(result.rows)
})

app.get('/api/sources', async (req, res) => {
  const result = await pool.query('SELECT * FROM sources ORDER BY id')
  res.json(result.rows)
})

app.get('/api/stats', async (req, res) => {
  const sources = await pool.query('SELECT COUNT(*) FROM sources')
  const pipelines = await pool.query('SELECT COUNT(*) FROM pipelines')
  res.json({
    sources: sources.rows[0].count,
    pipelines: pipelines.rows[0].count,
  })
})

app.listen(4000, () => {
  console.log('🚀 DataFlow API running on http://localhost:4000')
  console.log('🔌 WebSocket server running on ws://localhost:4001')
})
