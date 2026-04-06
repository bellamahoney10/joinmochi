const APPS_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbz8l2y8IhW-bwmK1q6q0vSBx59zTJ-uXcKRWrKwCzhc04KeYdEtt0xm4q5Mwaen6IH7qQ/exec';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  try {
    const response = await fetch(APPS_SCRIPT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      body: JSON.stringify(req.body),
      redirect: 'follow'
    });
    const text = await response.text();
    console.log('Apps Script response:', response.status, text);
    res.status(200).json({ ok: true, result: text });
  } catch (e) {
    console.error('Apps Script error:', e);
    res.status(500).json({ error: e.message });
  }
};
