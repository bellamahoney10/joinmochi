# joinmochi — Mochi Outreach Call Tool

## Project
Vercel-deployed outreach calling tool for Mochi health coaches (AJ Ciar, Marien Tolentino).
- Repo: `bellamahoney10/joinmochi` (main branch → auto-deploys to Vercel)
- All UI: `/Users/bellamahoney/joinmochi/index.html` (single-file vanilla JS app)
- API: `/Users/bellamahoney/joinmochi/api/*.js` (Vercel serverless functions)

## Key files
- `index.html` — entire frontend: agent selector, contact list, call script, log result flow, dashboard
- `api/contacts.js` — fetches today's assigned contacts for an agent
- `api/assign.js` — morning batch assignment (AJ + Marien, 20 each, cron-triggered)
- `api/refresh.js` — top-up contacts on demand (capped: blocks if agent has 10+ callable uncalled contacts)
- `api/sheet.js` — proxy to Google Apps Script for logging outcomes to Google Sheet
- `api/agents.js` — returns active outreach agents from DB
- `api/lib/tzConfig.js` — callable states, timezone priority logic

## Database
- Host: `db-prod.ourmochi.com`
- DB: `postgres`, user: `bella_mahoney_prod`
- Key tables: `outreach_call_queue`, `admins`, `outreach_agents`, `subscriptions`, `adult_eligibility`

## Google Sheet logging
- Sheet ID: `1SjRvsyHCAM3ahdYamWwRGiTHPZtcpIAQmML1lBh6hlQ`
- Apps Script URL (active): `https://script.google.com/macros/s/AKfycbz8l2y8IhW-bwmK1q6q0vSBx59zTJ-uXcKRWrKwCzhc04KeYdEtt0xm4q5Mwaen6IH7qQ/exec`
- Tab names use full agent name (first + last), e.g. "AJ Ciar", "Marien Tolentino"
- Apps Script actions: `initSession` (on load) and `updateResult` (on outcome log)
- `sheet.js` manually follows the 302 redirect with POST (fetch `redirect:'manual'` then re-POST to Location header)

## Outcome persistence
- Call outcomes saved to `localStorage` keyed by `mochi_oc_{agentId}_{month}_{day}_{year}`
- Restored on every `loadContacts` call so refresh doesn't wipe logged results

## Rules
- Never edit the Downloads copy (`~/Downloads/mochi_call_script*.html`) — always edit this repo
- Always push to main; Vercel auto-deploys
- When UI changes don't appear to work, instruct user to hard refresh: Cmd+Shift+R
