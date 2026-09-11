TRABZON STREAM PLAYER BACKEND – PROXY GÜNCELLEMESİ

Render Web Service aynı şekilde çalışır (Docker). Yeni sürümde FFmpeg doğrudan Xtream URL
sunucusuna gitmez; /source/:id üzerinden Range destekli bir proxy ile bağlanır.

Render Environment Variables:
DEBUG_PLAYER=1
PUBLIC_BASE_URL=https://renderbackhand.onrender.com

PUBLIC_BASE_URL sayesinde HLS adresi Vercel yerine doğrudan Render alan adını kullanır.

Deploy sonrası test:
https://renderbackhand.onrender.com/health
→ {"ok":true,"sessions":0}

Film açarken Logs'ta şunları görmelisin:
[STREAM] start ...
[PROXY] GET ... range=...
[STREAM] ready id=... hls=https://renderbackhand.onrender.com/hls/...

Not: Aynı anda tek aktif player session tutulur.
