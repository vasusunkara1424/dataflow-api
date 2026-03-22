const express = require('express')
const cors = require('cors')
const { Pool } = require('pg')
const { WebSocketServer } = require('ws')
const OpenAI = require('openai')

const app = express()
app.use(cors())
app.use(express.json())

const pool = new Pool({ connectionString: process.env.DATABASE_URL })

// WebSocket server
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

// AI Auto SQL
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

app.post('/api/ai/sql', async (req, res) => {
  const { question } = req.body
  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: `You are a SQL expert. Convert natural language to PostgreSQL queries.
Tables:
- pipelines (id, name, status, records, latency, created_at)
- sources (id, name, type, status, created_at)
Return ONLY the SQL query, nothing else.`
        },
        { role: 'user', content: question }
      ]
    })
    const sql = completion.choices[0].message.content.trim().replace(/```sql\n?/g, "").replace(/```/g, "").trim()
    const result = await pool.query(sql)
    res.json({ success: true, sql, rows: result.rows })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.listen(4000, () => {
  console.log('🚀 DataFlow API running on http://localhost:4000')
  console.log('🔌 WebSocket server running on ws://localhost:4001')
})
//cache bust Sat Mar 21 19:24:49 EDT 2026

// Team Workspaces
app.get('/api/workspaces', async (req, res) => {
  const result = await pool.query('SELECT * FROM workspaces ORDER BY created_at DESC')
  res.json(result.rows)
})

app.post('/api/workspaces', async (req, res) => {
  const { name, userId, email } = req.body
  const slug = name.toLowerCase().replace(/\s+/g, '-') + '-' + Date.now()
  const workspace = await pool.query(
    'INSERT INTO workspaces (name, slug) VALUES ($1, $2) RETURNING *',
    [name, slug]
  )
  await pool.query(
    'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
    [workspace.rows[0].id, userId, email, 'owner']
  )
  res.json({ success: true, workspace: workspace.rows[0] })
})

app.post('/api/workspaces/:id/invite', async (req, res) => {
  const { email } = req.body
  const { id } = req.params
  await pool.query(
    'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
    [id, 'pending', email, 'member']
  )
  res.json({ success: true, message: `Invited ${email}` })
})

app.get('/api/workspaces/:id/members', async (req, res) => {
  const result = await pool.query(
    'SELECT * FROM workspace_members WHERE workspace_id = $1',
    [req.params.id]
  )
  res.json(result.rows)
})

// Team Workspaces
app.get('/api/workspaces', async (req, res) => {
  const result = await pool.query('SELECT * FROM workspaces ORDER BY created_at DESC')
  res.json(result.rows)
})

app.post('/api/workspaces', async (req, res) => {
  const { name, userId, email } = req.body
  const slug = name.toLowerCase().replace(/\s+/g, '-') + '-' + Date.now()
  const workspace = await pool.query(
    'INSERT INTO workspaces (name, slug) VALUES ($1, $2) RETURNING *',
    [name, slug]
  )
  await pool.query(
    'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
    [workspace.rows[0].id, userId, email, 'owner']
  )
  res.json({ success: true, workspace: workspace.rows[0] })
})

app.post('/api/workspaces/:id/invite', async (req, res) => {
  const { email } = req.body
  const { id } = req.params
  await pool.query(
    'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
    [id, 'pending', email, 'member']
  )
  res.json({ success: true, message: `Invited ${email}` })
})

app.get('/api/workspaces/:id/members', async (req, res) => {
  const result = await pool.query(
    'SELECT * FROM workspace_members WHERE workspace_id = $1',
    [req.params.id]
  )
  res.json(result.rows)
})

// Team Workspaces
app.post('/api/workspaces', async (req, res) => {
  const { name, userId, email } = req.body
  const slug = name.toLowerCase().replace(/\s+/g, '-')
  try {
    const ws = await pool.query(
      'INSERT INTO workspaces (name, slug) VALUES ($1, $2) RETURNING *',
      [name, slug]
    )
    await pool.query(
      'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
      [ws.rows[0].id, userId, email, 'owner']
    )
    res.json({ success: true, workspace: ws.rows[0] })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.get('/api/workspaces/:userId', async (req, res) => {
  const { userId } = req.params
  try {
    const result = await pool.query(
      `SELECT w.*, wm.role FROM workspaces w
       JOIN workspace_members wm ON w.id = wm.workspace_id
       WHERE wm.user_id = $1`,
      [userId]
    )
    res.json({ success: true, workspaces: result.rows })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.post('/api/workspaces/:id/invite', async (req, res) => {
  const { id } = req.params
  const { email } = req.body
  try {
    await pool.query(
      'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
      [id, email, email, 'member']
    )
    res.json({ success: true, message: `${email} invited!` })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

// Team Workspaces
app.post('/api/workspaces', async (req, res) => {
  const { name, userId, email } = req.body
  const slug = name.toLowerCase().replace(/\s+/g, '-')
  try {
    const ws = await pool.query(
      'INSERT INTO workspaces (name, slug) VALUES ($1, $2) RETURNING *',
      [name, slug]
    )
    await pool.query(
      'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
      [ws.rows[0].id, userId, email, 'owner']
    )
    res.json({ success: true, workspace: ws.rows[0] })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.get('/api/workspaces/:userId', async (req, res) => {
  const { userId } = req.params
  try {
    const result = await pool.query(
      `SELECT w.*, wm.role FROM workspaces w
       JOIN workspace_members wm ON w.id = wm.workspace_id
       WHERE wm.user_id = $1`,
      [userId]
    )
    res.json({ success: true, workspaces: result.rows })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.post('/api/workspaces/:id/invite', async (req, res) => {
  const { id } = req.params
  const { email } = req.body
  try {
    await pool.query(
      'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
      [id, email, email, 'member']
    )
    res.json({ success: true, message: `${email} invited!` })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

// Team Workspaces
app.post('/api/workspaces', async (req, res) => {
  const { name, userId, email } = req.body
  const slug = name.toLowerCase().replace(/\s+/g, '-')
  try {
    const ws = await pool.query(
      'INSERT INTO workspaces (name, slug) VALUES ($1, $2) RETURNING *',
      [name, slug]
    )
    await pool.query(
      'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
      [ws.rows[0].id, userId, email, 'owner']
    )
    res.json({ success: true, workspace: ws.rows[0] })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.get('/api/workspaces/:userId', async (req, res) => {
  const { userId } = req.params
  try {
    const result = await pool.query(
      `SELECT w.*, wm.role FROM workspaces w
       JOIN workspace_members wm ON w.id = wm.workspace_id
       WHERE wm.user_id = $1`,
      [userId]
    )
    res.json({ success: true, workspaces: result.rows })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})

app.post('/api/workspaces/:id/invite', async (req, res) => {
  const { id } = req.params
  const { email } = req.body
  try {
    await pool.query(
      'INSERT INTO workspace_members (workspace_id, user_id, email, role) VALUES ($1, $2, $3, $4)',
      [id, email, email, 'member']
    )
    res.json({ success: true, message: `${email} invited!` })
  } catch (err) {
    res.json({ success: false, error: err.message })
  }
})
// Sun Mar 22 15:46:28 EDT 2026
// Sun Mar 22 15:47:46 EDT 2026
// Sun Mar 22 15:47:52 EDT 2026
