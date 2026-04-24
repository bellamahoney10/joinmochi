const APPS_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbz8l2y8IhW-bwmK1q6q0vSBx59zTJ-uXcKRWrKwCzhc04KeYdEtt0xm4q5Mwaen6IH7qQ/exec';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    const body = JSON.stringify(req.body);
    const opts = { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body, redirect: 'manual', signal: controller.signal };
    let response = await fetch(APPS_SCRIPT_URL, opts);
    if (response.status === 302 || response.status === 301) {
      const location = response.headers.get('location');
      if (location) response = await fetch(location, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body, signal: controller.signal });
    }
    clearTimeout(timeout);
    const text = await response.text();
    console.log('Apps Script response:', response.status, text);
    res.status(200).json({ ok: true, result: text });
  } catch (e) {
    console.error('Apps Script error:', e.message);
    res.status(200).json({ ok: false, error: e.message });
  }
};
