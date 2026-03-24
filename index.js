import express from 'express';
import fetch from 'node-fetch';

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN  = process.env.TELEGRAM_BOT_TOKEN;
const SUPABASE_URL    = process.env.SUPABASE_URL;
const SUPABASE_ANON   = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SVC    = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ALLOWED_CHAT_ID = process.env.ALLOWED_CHAT_ID; // optional whitelist

const TG = `https://api.telegram.org/bot${TELEGRAM_TOKEN}`;

/* ─── Telegram helpers ───────────────────────────────────────────────────────── */

async function sendMessage(chatId, text, replyTo) {
  await fetch(`${TG}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      reply_to_message_id: replyTo,
      parse_mode: 'Markdown',
    }),
  });
}

async function getFileBase64(fileId) {
  const res  = await fetch(`${TG}/getFile?file_id=${fileId}`);
  const json = await res.json();
  const path = json.result.file_path;
  const imgRes = await fetch(`https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${path}`);
  const buffer = await imgRes.arrayBuffer();
  return Buffer.from(buffer).toString('base64');
}

/* ─── Caption parser ─────────────────────────────────────────────────────────── */

/*
  Supported caption formats (case-insensitive):

  ARTIST DAILY METRICS:
    artist: Van Gogh | metric: streams | from: 2025-03-01 | to: 2025-03-24

  TRACK SCREENSHOTS:
    artist: Van Gogh | type: track | track: B4 | screen: overview_totals
    artist: Van Gogh | type: track | track: B4 | screen: overview_chart | metric: streams | from: 2025-03-01 | to: 2025-03-24
    artist: Van Gogh | type: track | track: B4 | screen: location_countries
    artist: Van Gogh | type: track | track: B4 | screen: location_cities
    artist: Van Gogh | type: track | track: B4 | screen: playlists

  Shorthand aliases:
    metric aliases:  str=streams, lis=listeners, sav=saves, pl=playlist_adds, fol=followers, mal=monthly_active_listeners, spl=streams_per_listener
    screen aliases:  ov=overview_totals, chart=overview_chart, countries=location_countries, cities=location_cities, play=playlists
*/

const METRIC_ALIASES = {
  str:     'streams',
  streams: 'streams',
  lis:     'listeners',
  listeners: 'listeners',
  sav:     'saves',
  saves:   'saves',
  pl:      'playlist_adds',
  playlist_adds: 'playlist_adds',
  fol:     'followers',
  followers: 'followers',
  mal:     'monthly_active_listeners',
  monthly_active_listeners: 'monthly_active_listeners',
  spl:     'streams_per_listener',
  streams_per_listener: 'streams_per_listener',
};

const SCREEN_ALIASES = {
  ov:               'overview_totals',
  overview_totals:  'overview_totals',
  chart:            'overview_chart',
  overview_chart:   'overview_chart',
  countries:        'location_countries',
  location_countries: 'location_countries',
  cities:           'location_cities',
  location_cities:  'location_cities',
  play:             'playlists',
  playlists:        'playlists',
};

function parseCaption(raw) {
  if (!raw) return null;

  // split on | or newline, trim each
  const parts = raw.split(/[|\n]/).map(p => p.trim()).filter(Boolean);
  const kv = {};
  for (const part of parts) {
    const idx = part.indexOf(':');
    if (idx === -1) continue;
    const k = part.slice(0, idx).trim().toLowerCase().replace(/\s+/g, '_');
    const v = part.slice(idx + 1).trim();
    kv[k] = v;
  }

  const artist = kv.artist;
  if (!artist) return null;

  const isTrack = kv.type?.toLowerCase() === 'track' || kv.track || kv.screen;

  if (isTrack) {
    const screen = SCREEN_ALIASES[kv.screen?.toLowerCase()] ?? null;
    if (!screen) return { error: `Unknown screen type: "${kv.screen}". Use: ov, chart, countries, cities, play` };
    return {
      mode:        'track',
      artistName:  artist,
      trackName:   kv.track ?? null,
      screenType:  screen,
      metricKey:   METRIC_ALIASES[kv.metric?.toLowerCase()] ?? null,
      periodStart: kv.from ?? null,
      periodEnd:   kv.to   ?? null,
    };
  }

  // Artist daily metric
  const metric = METRIC_ALIASES[kv.metric?.toLowerCase()] ?? null;
  if (!metric) return { error: `Unknown metric: "${kv.metric}". Use: str, lis, sav, pl, fol, mal, spl` };
  if (!kv.from || !kv.to) return { error: 'Missing date range. Add: from: YYYY-MM-DD | to: YYYY-MM-DD' };

  return {
    mode:        'artist',
    artistName:  artist,
    metric,
    periodStart: kv.from,
    periodEnd:   kv.to,
  };
}

/* ─── Supabase helpers ───────────────────────────────────────────────────────── */

async function getArtistId(name) {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/artists?name=ilike.${encodeURIComponent(name)}&select=id,name&limit=1`,
    { headers: { apikey: SUPABASE_SVC, Authorization: `Bearer ${SUPABASE_SVC}` } }
  );
  const data = await res.json();
  return data?.[0] ?? null;
}

async function getTrackId(artistId, trackName) {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/tracks?artist_id=eq.${artistId}&title=ilike.${encodeURIComponent(trackName)}&select=id,title&limit=1`,
    { headers: { apikey: SUPABASE_SVC, Authorization: `Bearer ${SUPABASE_SVC}` } }
  );
  const data = await res.json();
  return data?.[0] ?? null;
}

/* ─── Edge function callers ──────────────────────────────────────────────────── */

async function callHyperWorker({ artistId, metric, periodStart, periodEnd, base64, mimeType }) {
  const res = await fetch(`${SUPABASE_URL}/functions/v1/hyper-worker`, {
    method: 'POST',
    headers: {
      'Content-Type':  'application/json',
      Authorization:   `Bearer ${SUPABASE_SVC}`,
      apikey:          SUPABASE_ANON,
    },
    body: JSON.stringify({
      artist_id:    artistId,
      metric,
      period:       `${periodStart}_${periodEnd}`,
      period_start: periodStart,
      period_end:   periodEnd,
      base64,
      mimeType,
    }),
  });
  return res.json();
}

async function callTrackWorker({ artistId, trackId, screenType, metricKey, periodStart, periodEnd, base64, mimeType }) {
  const snapshotDate = new Date().toISOString().slice(0, 10);
  const res = await fetch(`${SUPABASE_URL}/functions/v1/smooth-function`, {
    method: 'POST',
    headers: {
      'Content-Type':  'application/json',
      Authorization:   `Bearer ${SUPABASE_SVC}`,
      apikey:          SUPABASE_ANON,
    },
    body: JSON.stringify({
      artist_id:     artistId,
      track_id:      trackId,
      snapshot_date: snapshotDate,
      files: [{
        name:         'screenshot.jpg',
        type:         screenType,
        metric_key:   metricKey ?? undefined,
        period_start: periodStart ?? undefined,
        period_end:   periodEnd   ?? undefined,
        base64,
        mimeType,
      }],
    }),
  });
  const text = await res.text();
  console.log('smooth-function status:', res.status);
  console.log('smooth-function response:', text.slice(0, 300));
  try { return JSON.parse(text); }
  catch { return { error: `Edge function error: ${text.slice(0, 200)}` }; }
}

/* ─── Main handler ───────────────────────────────────────────────────────────── */

async function handleUpdate(update) {
  const msg = update.message ?? update.channel_post;
  if (!msg) return;

  const chatId    = msg.chat.id;
  const messageId = msg.message_id;
  const caption   = msg.caption ?? msg.text ?? '';
  const photo     = msg.photo;
  const document  = msg.document;

  // Whitelist check
  if (ALLOWED_CHAT_ID && String(chatId) !== String(ALLOWED_CHAT_ID)) return;

  // Help command
  if (caption.toLowerCase().startsWith('/help') || caption.toLowerCase().startsWith('/start')) {
    await sendMessage(chatId, [
      '*Pulse Bot* 🎵',
      '',
      '*Artist daily metric:*',
      '`artist: Nome | metric: str | from: 2025-03-01 | to: 2025-03-24`',
      '',
      '*Track screenshot:*',
      '`artist: Nome | type: track | track: NomeTraccia | screen: ov`',
      '`artist: Nome | type: track | track: NomeTraccia | screen: chart | metric: str | from: 2025-03-01 | to: 2025-03-24`',
      '',
      '*Metric shortcuts:* str, lis, sav, pl, fol, mal, spl',
      '*Screen shortcuts:* ov, chart, countries, cities, play',
    ].join('\n'), messageId);
    return;
  }

  // Must have an image
  if (!photo && !(document?.mime_type?.startsWith('image/'))) {
    if (caption && caption.includes('artist:')) {
      await sendMessage(chatId, '⚠️ Caption rilevata ma manca l\'immagine. Allega lo screenshot.', messageId);
    }
    return;
  }

  // Parse caption
  const parsed = parseCaption(caption);
  if (!parsed) {
    await sendMessage(chatId, '⚠️ Caption non riconosciuta. Manda `/help` per il formato.', messageId);
    return;
  }
  if (parsed.error) {
    await sendMessage(chatId, `⚠️ ${parsed.error}`, messageId);
    return;
  }

  await sendMessage(chatId, '⏳ Processing…', messageId);

  try {
    // Resolve artist
    const artist = await getArtistId(parsed.artistName);
    if (!artist) {
      await sendMessage(chatId, `❌ Artista non trovato: *${parsed.artistName}*\nVerifica il nome esatto nel DB.`, messageId);
      return;
    }

    // Get image base64
    const fileId  = photo ? photo[photo.length - 1].file_id : document.file_id;
    const base64  = await getFileBase64(fileId);
    const mimeType = 'image/jpeg';

    /* ── ARTIST DAILY ── */
    if (parsed.mode === 'artist') {
      const result = await callHyperWorker({
        artistId:    artist.id,
        metric:      parsed.metric,
        periodStart: parsed.periodStart,
        periodEnd:   parsed.periodEnd,
        base64,
        mimeType,
      });

      if (result.error) {
        await sendMessage(chatId, `❌ Errore: ${result.error}`, messageId);
        return;
      }

      await sendMessage(chatId, [
        `✅ *${artist.name}* — ${parsed.metric}`,
        `📅 ${parsed.periodStart} → ${parsed.periodEnd}`,
        `💾 ${result.rows_saved} righe salvate`,
        result.note ? `\n⚠️ ${result.note}` : '',
      ].filter(Boolean).join('\n'), messageId);
      return;
    }

    /* ── TRACK ── */
    if (parsed.mode === 'track') {
      if (!parsed.trackName) {
        await sendMessage(chatId, '⚠️ Specifica la traccia: `track: NomeTraccia`', messageId);
        return;
      }

      const track = await getTrackId(artist.id, parsed.trackName);
      if (!track) {
        await sendMessage(chatId, `❌ Traccia non trovata: *${parsed.trackName}*\nVerifica il titolo esatto.`, messageId);
        return;
      }

      const result = await callTrackWorker({
        artistId:    artist.id,
        trackId:     track.id,
        screenType:  parsed.screenType,
        metricKey:   parsed.metricKey,
        periodStart: parsed.periodStart,
        periodEnd:   parsed.periodEnd,
        base64,
        mimeType,
      });

      if (result.error) {
        await sendMessage(chatId, `❌ Errore: ${result.error}`, messageId);
        return;
      }

      const ok  = result.results?.filter(r => r.status === 'success').length ?? 0;
      const tot = result.results?.length ?? 1;

      await sendMessage(chatId, [
        `✅ *${artist.name}* — *${track.title}*`,
        `📊 ${parsed.screenType}`,
        `💾 ${ok}/${tot} screenshot salvati`,
      ].join('\n'), messageId);
      return;
    }

  } catch (err) {
    console.error('Handler error:', err);
    await sendMessage(chatId, `❌ Errore imprevisto: ${err.message}`, messageId);
  }
}

/* ─── Routes ─────────────────────────────────────────────────────────────────── */

app.post('/webhook', async (req, res) => {
  res.sendStatus(200); // ack Telegram immediately
  try { await handleUpdate(req.body); } catch (e) { console.error(e); }
});

app.get('/health', (_, res) => res.json({ ok: true, ts: new Date().toISOString() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Pulse bot listening on :${PORT}`));