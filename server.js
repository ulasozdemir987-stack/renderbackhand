import express from 'express'
import cors from 'cors'
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'

const app = express()
const PORT = Number(process.env.PORT || 10000)
const ROOT = process.env.STREAM_DIR || '/tmp/trabzon-stream'
const FFmpeg = process.env.FFMPEG_PATH || 'ffmpeg'
const sessions = new Map()
const DEBUG = process.env.DEBUG_PLAYER === '1'

fs.mkdirSync(ROOT, { recursive: true })
app.use(cors({ origin: true, methods: ['GET', 'POST', 'DELETE', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Range'] }))
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
  try { fs.rmSync(s.dir, { recursive: true, force: true }) } catch {}
  sessions.delete(id)
}

function safeJson(res, status, body) { res.status(status).json(body) }

function sourceSummary(raw) {
  try {
    const u = new URL(raw)
    return `${u.protocol}//${u.hostname}${u.port ? `:${u.port}` : ''}${u.pathname.split('/').slice(0, 2).join('/')}`
  } catch { return 'invalid-source' }
}

function publicBase(req) {
  const configured = String(process.env.PUBLIC_BASE_URL || '').trim().replace(/\/$/, '')
  if (configured) return configured
  const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'https').split(',')[0].trim()
  const host = String(req.headers['x-forwarded-host'] || req.get('host') || '').split(',')[0].trim()
  return `${proto}://${host}`
}

app.get('/health', (_req, res) => res.json({ ok: true, sessions: sessions.size }))

app.options(/.*/, cors())

// This proxy sits between FFmpeg and the Xtream server. FFmpeg requests this
// URL with Range headers, allowing the upstream connection to be retried by
// FFmpeg instead of having FFmpeg talk directly to a fragile origin server.
async function proxySource(req, res) {
  const id = String(req.params.id || '')
  const session = sessions.get(id)
  if (!session) return safeJson(res, 404, { error: 'Yayın oturumu bulunamadı.' })

  const headers = {
    'User-Agent': 'VLC/3.0.20 LibVLC/3.0.20',
    'Accept': '*/*',
    'Accept-Encoding': 'identity',
  }
  const range = req.headers.range
  if (range) headers.Range = String(range)
  const ifRange = req.headers['if-range']
  if (ifRange) headers['If-Range'] = String(ifRange)

  if (DEBUG) console.log(`[PROXY] ${req.method} id=${id} range=${range || 'none'} source=${sourceSummary(session.sourceUrl)}`)

  try {
    const upstream = await fetch(session.sourceUrl, {
      method: req.method === 'HEAD' ? 'HEAD' : 'GET',
      headers,
      redirect: 'follow',
      cache: 'no-store',
      signal: AbortSignal.timeout(45_000),
    })

    if (!upstream.ok && upstream.status !== 206) {
      const text = req.method === 'HEAD' ? '' : await upstream.text().catch(() => '')
      console.error(`[PROXY] upstream HTTP ${upstream.status} id=${id} ${text.slice(0, 500)}`)
      return safeJson(res, 502, { error: `Kaynak HTTP ${upstream.status}` })
    }

    res.status(upstream.status)
    const passthrough = [
      'accept-ranges', 'content-length', 'content-range', 'content-type',
      'etag', 'last-modified', 'cache-control', 'content-disposition',
    ]
    for (const key of passthrough) {
      const value = upstream.headers.get(key)
      if (value) res.setHeader(key, value)
    }
    if (!res.getHeader('content-type')) res.setHeader('Content-Type', 'application/octet-stream')
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader('Cache-Control', 'no-store')

    if (req.method === 'HEAD' || !upstream.body) return res.end()

    const reader = upstream.body.getReader()
    req.on('close', () => { try { reader.cancel() } catch {} })
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      if (!res.write(Buffer.from(value))) await new Promise(resolve => res.once('drain', resolve))
    }
    res.end()
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Kaynak proxy hatası'
    console.error(`[PROXY] fetch error id=${id}: ${message}`)
    if (!res.headersSent) safeJson(res, 502, { error: message })
    else res.destroy()
  }
}

app.head('/source/:id', proxySource)
app.get('/source/:id', proxySource)

app.post('/api/stream/start', async (req, res) => {
  const { sourceUrl, type = 'movie' } = req.body || {}
  console.log(`[STREAM] start type=${type} source=${sourceSummary(sourceUrl || '')}`)
  if (!validSource(sourceUrl)) return safeJson(res, 400, { error: 'Geçerli bir HTTP/HTTPS kaynak URL gerekli.' })
  if (!['movie', 'episode', 'live'].includes(type)) return safeJson(res, 400, { error: 'Geçersiz yayın tipi.' })

  // Prevent duplicate starts for the exact same source while the first one is preparing.
  for (const [id, current] of sessions) {
    if (current.sourceUrl === sourceUrl && current.type === type && !current.failed) {
      const ready = await waitForHealthyPlaylist(current.playlist, current.child, 5_000)
      if (ready) {
        return safeJson(res, 200, { sessionId: id, hlsUrl: `${publicBase(req)}/hls/${id}/index.m3u8` })
      }
      if (current.startingPromise) {
        try {
          const hlsUrl = await current.startingPromise
          return safeJson(res, 200, { sessionId: id, hlsUrl })
        } catch {}
      }
    }
  }

  // Only one viewer/session is expected. Stop the previous session cleanly.
  for (const id of [...sessions.keys()]) cleanup(id)

  const id = crypto.randomBytes(10).toString('hex')
  const dir = path.join(ROOT, id)
  fs.mkdirSync(dir, { recursive: true })
  const playlist = path.join(dir, 'index.m3u8')
  const isLive = type === 'live'

  // Use FFmpeg directly against the Xtream source. The Xtream endpoints used here
  // are HTTP media files; disabling HTTP seeking avoids broken/incomplete Range
  // responses that can make MKV parsing fail at the EBML header.
  const args = [
    '-hide_banner', '-loglevel', DEBUG ? 'warning' : 'error', '-nostdin',
    '-rw_timeout', '60000000',
    '-http_seekable', '0',
    '-reconnect', '1',
    '-reconnect_streamed', '1',
    '-reconnect_at_eof', '0',
    '-reconnect_delay_max', '10',
    '-user_agent', 'Mozilla/5.0',
    '-probesize', '50M',
    '-analyzeduration', '20M',
    '-i', sourceUrl,
    '-map', '0:v:0',
    '-map', '0:a?',
    '-c:v', 'copy',
    '-c:a', 'aac', '-b:a', '160k',
    ...(isLive
      ? ['-f', 'hls', '-hls_time', '4', '-hls_list_size', '8', '-hls_flags', 'delete_segments+append_list']
      : ['-f', 'hls', '-hls_time', '6', '-hls_list_size', '0', '-hls_playlist_type', 'vod']),
    playlist,
  ]

  console.log(`[STREAM] ffmpeg=${FFmpeg} direct-source output=${playlist}`)
  if (DEBUG) console.log(`[STREAM] args=${JSON.stringify(args)}`)

  const child = spawn(FFmpeg, args, { stdio: ['ignore', 'ignore', 'pipe'] })
  let stderr = ''
  child.stderr.on('data', b => {
    const text = b.toString()
    stderr = (stderr + text).slice(-20000)
    if (DEBUG) console.log(`[FFMPEG] ${text.trim()}`)
  })
  child.on('error', err => console.error(`[STREAM] spawn error: ${err.message}`))

  const session = { id, dir, child, createdAt: Date.now(), timer: null, sourceUrl, type, stderr, playlist, startingPromise: null }
  sessions.set(id, session)
  session.timer = setTimeout(() => cleanup(id), isLive ? 4 * 60 * 60_000 : 6 * 60 * 60_000)

  child.on('exit', (code, signal) => {
    console.log(`[STREAM] ffmpeg exit code=${code} signal=${signal || 'none'} id=${id} stderr=${stderr.slice(-5000)}`)
    const current = sessions.get(id)
    if (!current) return
    if (signal !== 'SIGTERM' && code !== 0 && !fs.existsSync(playlist)) {
      current.failed = true
    }
    if (fs.existsSync(playlist)) {
      setTimeout(() => cleanup(id), isLive ? 30_000 : 5 * 60_000)
    } else {
      setTimeout(() => cleanup(id), 5_000)
    }
  })

  const startingPromise = (async () => {
    const started = await waitForHealthyPlaylist(playlist, child, 120_000)
    if (!started) {
      const err = stderr.trim() || `FFmpeg HLS üretemedi (exit=${child.exitCode ?? 'n/a'}).`
      console.error(`[STREAM] start failed id=${id}: ${err}`)
      cleanup(id)
      throw new Error(err)
    }
    const hlsUrl = `${publicBase(req)}/hls/${id}/index.m3u8`
    console.log(`[STREAM] ready id=${id} hls=${hlsUrl}`)
    return hlsUrl
  })()
  session.startingPromise = startingPromise

  try {
    const hlsUrl = await startingPromise
    safeJson(res, 200, { sessionId: id, hlsUrl })
  } catch (error) {
    safeJson(res, 502, { error: error instanceof Error ? error.message : 'Player başlatılamadı.' })
  }
})

app.delete('/api/stream/:id', (req, res) => {
  cleanup(req.params.id)
  res.json({ ok: true })
})

app.use('/hls', express.static(ROOT, {
  fallthrough: false,
  setHeaders: res => {
    res.setHeader('Cache-Control', 'no-store, max-age=0')
    res.setHeader('Access-Control-Allow-Origin', '*')
  },
}))

app.use((_req, res) => safeJson(res, 404, { error: 'Bulunamadı.' }))

function waitForHealthyPlaylist(file, child, timeout) {
  return new Promise(resolve => {
    const started = Date.now()
    const timer = setInterval(() => {
      if (fs.existsSync(file) && fs.statSync(file).size > 0) {
        clearInterval(timer)
        resolve(true)
      } else if (child.exitCode != null && child.exitCode !== 0) {
        clearInterval(timer)
        resolve(false)
      } else if (Date.now() - started > timeout) {
        clearInterval(timer)
        resolve(false)
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
