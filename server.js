import express from 'express'
import cors from 'cors'
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'

const app = express()
const PORT = Number(process.env.PORT || 10000)
const ROOT = process.env.STREAM_DIR || '/tmp/trabzon-stream'
const FFPROBE = process.env.FFPROBE_PATH || 'ffprobe'
const FFMPEG = process.env.FFMPEG_PATH || 'ffmpeg'
const sessions = new Map()
const DEBUG = process.env.DEBUG_PLAYER === '1'

fs.mkdirSync(ROOT, { recursive: true })
app.use(cors({ origin: true }))
app.use(express.json({ limit: '32kb' }))

function validSource(raw) {
  try {
    const u = new URL(raw)
    return ['http:', 'https:'].includes(u.protocol)
  } catch { return false }
}

function cleanup(id) {
  const s = sessions.get(id)
  if (!s) return
  try { s.child?.kill('SIGTERM') } catch {}
  clearTimeout(s.timer)
  fs.rmSync(s.dir, { recursive: true, force: true })
  sessions.delete(id)
}

function safeJson(res, status, body) { res.status(status).json(body) }

function sourceSummary(raw) {
  try {
    const u = new URL(raw)
    return `${u.protocol}//${u.hostname}${u.port ? `:${u.port}` : ''}${u.pathname.split('/').slice(0, 2).join('/')}`
  } catch { return 'invalid-source' }
}

app.get('/health', (_req, res) => res.json({ ok: true, sessions: sessions.size }))

app.options('/api/stream/start', cors({ origin: true, methods: ['POST','OPTIONS'], allowedHeaders: ['Content-Type'] }))

app.post('/api/stream/start', async (req, res) => {
  const { sourceUrl, type = 'movie' } = req.body || {}
  console.log(`[STREAM] start type=${type} source=${sourceSummary(sourceUrl || '')}`)
  if (!validSource(sourceUrl)) return safeJson(res, 400, { error: 'Geçerli bir HTTP/HTTPS kaynak URL gerekli.' })
  if (!['movie', 'episode', 'live'].includes(type)) return safeJson(res, 400, { error: 'Geçersiz yayın tipi.' })
  if (sessions.size >= 1) {
    for (const id of sessions.keys()) cleanup(id)
  }

  const id = crypto.randomBytes(10).toString('hex')
  const dir = path.join(ROOT, id)
  fs.mkdirSync(dir, { recursive: true })
  const playlist = path.join(dir, 'index.m3u8')
  const isLive = type === 'live'
  const args = [
    '-hide_banner', '-loglevel', 'error',
    '-nostdin',
    '-user_agent', 'Mozilla/5.0',
    ...(isLive || !isLive ? ['-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5'] : []),
    '-i', sourceUrl,
    '-map', '0:v:0',
    '-map', '0:a?',
    '-c:v', 'copy',
    '-c:a', 'aac', '-b:a', '160k',
    ...(isLive
      ? ['-f', 'hls', '-hls_time', '4', '-hls_list_size', '8', '-hls_flags', 'delete_segments+append_list']
      : ['-f', 'hls', '-hls_time', '6', '-hls_list_size', '0', '-hls_playlist_type', 'vod']) ,
    playlist,
  ]

  console.log(`[STREAM] ffmpeg=${FFMPEG} output=${playlist}`)
  if (DEBUG) console.log(`[STREAM] args=${JSON.stringify(args)}`)
  const child = spawn(FFMPEG, args, { stdio: ['ignore', 'ignore', 'pipe'] })
  let stderr = ''
  child.stderr.on('data', b => { stderr = (stderr + b.toString()).slice(-12000); if (DEBUG) console.log(`[FFMPEG] ${b.toString().trim()}`) })
  child.on('error', err => console.error(`[STREAM] spawn error: ${err.message}`))
  const session = { id, dir, child, createdAt: Date.now(), timer: null, stderr }
  session.timer = setTimeout(() => cleanup(id), isLive ? 4 * 60 * 60_000 : 6 * 60 * 60_000)
  sessions.set(id, session)

  child.on('exit', (code, signal) => {
    console.log(`[STREAM] ffmpeg exit code=${code} signal=${signal || 'none'} id=${id} stderr=${stderr.slice(-3000)}`)
    const current = sessions.get(id)
    if (current && fs.existsSync(playlist) && isLive) setTimeout(() => cleanup(id), 30_000)
    else if (current) setTimeout(() => cleanup(id), 5 * 60_000)
  })

  const started = await waitForFile(playlist, 60_000)
  if (!started) {
    const err = stderr || `FFmpeg HLS üretemedi (exit=${child.exitCode ?? 'n/a'}).`
    console.error(`[STREAM] start failed id=${id}: ${err}`)
    cleanup(id)
    return safeJson(res, 502, { error: err })
  }

  safeJson(res, 200, { sessionId: id, hlsUrl: `/hls/${id}/index.m3u8` })
  console.log(`[STREAM] ready id=${id}`)
})

app.delete('/api/stream/:id', (req, res) => {
  cleanup(req.params.id)
  res.json({ ok: true })
})

app.use('/hls', express.static(ROOT, { fallthrough: false, setHeaders: res => {
  res.setHeader('Cache-Control', 'no-store, max-age=0')
  res.setHeader('Access-Control-Allow-Origin', '*')
} }))

app.use((_req, res) => safeJson(res, 404, { error: 'Bulunamadı.' }))

function waitForFile(file, timeout) {
  return new Promise(resolve => {
    const started = Date.now()
    const timer = setInterval(() => {
      if (fs.existsSync(file) && fs.statSync(file).size > 0) {
        clearInterval(timer); resolve(true)
      } else if (Date.now() - started > timeout) {
        clearInterval(timer); resolve(false)
      }
    }, 250)
  })
}

setInterval(() => {
  for (const [id, session] of sessions) {
    if (Date.now() - session.createdAt > 6 * 60 * 60_000) cleanup(id)
  }
}, 60_000).unref()

app.listen(PORT, '0.0.0.0', () => console.log(`TRABZON STREAM player backend listening on ${PORT}`))
