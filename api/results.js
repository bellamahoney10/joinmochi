const SHEET_ID = '1SjRvsyHCAM3ahdYamWwRGiTHPZtcpIAQmML1lBh6hlQ';

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      result.push(current); current = '';
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { agent_name } = req.query;
  if (!agent_name) return res.status(400).json({ error: 'agent_name required' });

  // Cutoff: 5 PT days ago
  const nowPt = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
  const cutoff = new Date(nowPt);
  cutoff.setDate(cutoff.getDate() - 5);

  try {
    const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:csv&sheet=${encodeURIComponent(agent_name)}&t=${Date.now()}`;
    const r = await fetch(url);
    const text = await r.text();

    const rows = [];
    for (const line of text.split('\n').slice(1)) {
      if (!line.trim()) continue;
      const cols = parseCSVLine(line);
      const rawDate = cols[0] || '';        // MM/DD/YYYY
      const name    = cols[2] || '';
      const phone   = (cols[3] || '').replace(/\D/g, '').slice(-10);
      const result  = (cols[4] || '').trim();
      const notes   = (cols[5] || '').trim();

      if (!rawDate || !phone) continue;

      // Parse MM/DD/YYYY → Date for cutoff check
      const p = rawDate.split('/');
      if (p.length !== 3) continue;
      const rowDate = new Date(`${p[2]}-${p[0].padStart(2,'0')}-${p[1].padStart(2,'0')}`);
      if (rowDate < cutoff) continue;

      rows.push({ date: rawDate, name, phone, result, notes });
    }

    res.json({ rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message, rows: [] });
  }
};
