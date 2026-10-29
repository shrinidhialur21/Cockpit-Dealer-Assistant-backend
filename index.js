/**
 * ============================================================
 * COCKPIT DEALER DIAGNOSTIC ASSISTANT — Backend
 * ============================================================
 * Role     : Custom App on Condense (exposes HTTPS port), same
 *            category as Automotive-data-output — NOT part of
 *            the Kafka pipeline. No input/output topics here.
 * Deploy   : Condense → Apps → Custom → expose port 3006
 *
 * PURPOSE:
 *   Dealer-side L1/L2/L3 diagnostic triage tool for RBIN's
 *   digital cockpit (Maruti Suzuki Victoris/Brezza). Separate
 *   from the fleet-style Digital Cockpit dashboard — this is a
 *   single-vehicle, conversational tool for a technician
 *   standing at the car.
 *
 *   L1 — reasons from the dealer's description + a seeded case
 *        library only (no telemetry).
 *   L2 — additionally fetches that VIN's LIVE state from the
 *        already-deployed Automotive-Insights-Output app (the
 *        same REST endpoint the dashboard uses:
 *        GET {TELEMETRY_API_URL}/api/assets/:vin) and grounds
 *        the recommendation in it.
 *   L3 — emails a full case summary (symptom, L1/L2 exchange,
 *        telemetry snapshot) to Bosch for escalation.
 *
 *   Every call to Claude is a single, stateless request — the
 *   frontend sends the full conversation history each time, and
 *   this backend re-assembles the system prompt (case library +
 *   telemetry, when relevant) fresh per request. Nothing is
 *   fine-tuned or persisted; see project docs for why that's
 *   the right call here.
 * ============================================================
 */

'use strict';

const express = require('express');
const cors = require('cors');
const nodemailer = require('nodemailer');
require('dotenv').config();

const PORT = parseInt(process.env.HTTPS_PORT || process.env.PORT || '8081');
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-5';
const TELEMETRY_API_URL = (process.env.TELEMETRY_API_URL || '').replace(/\/$/, '');
const MOCK_MODE = process.env.MOCK_MODE === 'true';
const CORS_ORIGIN = (process.env.CORS_ORIGIN || '*').replace(/\/$/, '');

const caseLibrary = require('./data/case-library.json');

// ── Structured logger ─────────────────────────────────────────
const log = {
  info: (m, c = {}) => console.log(JSON.stringify({ level: 'INFO', ts: new Date().toISOString(), svc: 'cockpit-dealer-assistant', msg: m, ...c })),
  warn: (m, c = {}) => console.log(JSON.stringify({ level: 'WARN', ts: new Date().toISOString(), svc: 'cockpit-dealer-assistant', msg: m, ...c })),
  error: (m, c = {}) => console.error(JSON.stringify({ level: 'ERROR', ts: new Date().toISOString(), svc: 'cockpit-dealer-assistant', msg: m, ...c })),
};

// ── Case library formatting (small enough to stuff in the prompt directly —
// no vector search needed at this size; see project docs for when that changes) ──
const CASE_LIBRARY_TEXT = caseLibrary.cases.map(c =>
  `- [${c.id}][${c.tier_hint}][${c.category}] Symptom: "${c.dealer_symptom}" | ` +
  `Guidance: ${c.resolution_summary} | OTA-fixable: ${c.ota_fixable ? `yes (${c.ota_package})` : 'no'}`
).join('\n');

// ── Live telemetry fetch — reuses the ALREADY-DEPLOYED output connector ──
async function fetchTelemetry(vin) {
  if (!TELEMETRY_API_URL) return { ok: false, reason: 'TELEMETRY_API_URL not configured' };
  try {
    const res = await fetch(`${TELEMETRY_API_URL}/api/assets/${encodeURIComponent(vin)}`, {
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) return { ok: false, reason: `telemetry API returned ${res.status}` };
    const data = await res.json();
    return { ok: true, data };
  } catch (err) {
    return { ok: false, reason: err.message };
  }
}

function summarizeTelemetry(telemetry) {
  if (!telemetry.ok) return `No live telemetry available (${telemetry.reason}). Reason honestly if asked — do not invent readings.`;
  const a = telemetry.data;
  const k = a.kpis || {};
  return [
    `asset_id: ${a.asset_id}`,
    `vehicle_model: ${a.vehicle_model || 'unknown'}`,
    `cockpit_health_score: ${k.cockpit_health_score ?? 'n/a'}`,
    `active_dtc_count: ${k.active_dtc_count ?? 'n/a'}`,
    `highest_severity: ${k.highest_severity ?? 'n/a'}`,
    `ota_fix_recommended: ${k.ota_fix_recommended ?? 'n/a'}`,
    `ota_fix_package: ${k.ota_fix_package ?? 'none'}`,
    `cpu_utilization_pct: ${k.cpu_utilization_pct ?? 'n/a'}`,
    `thermal_c: ${k.thermal_c ?? 'n/a'}`,
    `voltage_status: ${k.voltage_status ?? 'n/a'}`,
    `active dtc_codes: ${(a.dtc_codes || []).map(d => `${d.code} (${d.severity})`).join(', ') || 'none'}`,
  ].join('\n');
}

// ── System prompt assembly — deterministic per tier, not left to the model ──
function buildSystemPrompt(tier, vin, telemetrySummary) {
  const base = `You are a diagnostic assistant embedded in a dealer service tool for Maruti Suzuki digital cockpit systems (Bosch RBIN, TCU: Suzuki Connect). You are helping a service technician who is physically with the vehicle (VIN: ${vin}).

Reference case library (synthetic seed data pending real Bosch/MSIL case history — treat as illustrative guidance, not a complete or authoritative list):
${CASE_LIBRARY_TEXT}

General rules:
- Be concise and practical — the technician is standing at the car, not reading a report. Keep replies under about 150 words.
- Write plain text only: no markdown, no asterisks, no headings. Use short lines starting with "- " for lists.
- End with one clear next step for the technician.
- Never claim to perform an action yourself — you cannot push an OTA update or send an email. Only recommend what the technician or Bosch should do next.
- If nothing in the case library matches and (at L2) telemetry doesn't clarify things either, say so plainly and suggest escalating rather than guessing.`;

  if (tier === 'L1') {
    return `${base}

Current tier: L1 — you only have the dealer's description and the case library. You do NOT have live telemetry yet. Do not reference specific sensor values. If the description plausibly matches a known case, recommend that fix. If it's ambiguous or could be several things, say what live telemetry would help confirm, and note that the next step is pulling the vehicle's live data.`;
  }

  if (tier === 'L2') {
    return `${base}

Current tier: L2 — live telemetry has been pulled for this vehicle:
${telemetrySummary}

Ground your recommendation in this data alongside the case library. Call out the specific reading(s) that support your reasoning. If the live data contradicts the reported symptom or doesn't match any case, say so and suggest escalation instead of forcing a match.`;
  }

  // L3 — used only for the assistant's framing commentary before the actual
  // escalation email is sent by the /api/escalate endpoint below.
  return `${base}

Current tier: L3 — this case is being escalated to Bosch. Live telemetry:
${telemetrySummary}

Briefly summarize, in 2-3 sentences, why this case warrants escalation (what was tried, what remains unresolved or unclear). Do not attempt a new diagnosis — that's now Bosch's job.`;
}

// ── Claude call ────────────────────────────────────────────────
let anthropic = null;
function getClient() {
  if (!ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not configured');
  if (!anthropic) {
    const Anthropic = require('@anthropic-ai/sdk');
    anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
  }
  return anthropic;
}

async function callClaude(systemPrompt, history, message) {
  if (MOCK_MODE) {
    return `[MOCK_MODE reply — no real Claude call made] Received: "${message}". This is a canned response for local wiring tests only; set MOCK_MODE=false with a real ANTHROPIC_API_KEY for actual reasoning.`;
  }
  const client = getClient();
  const messages = [
    ...history.map(h => ({ role: h.role === 'assistant' ? 'assistant' : 'user', content: h.content })),
    { role: 'user', content: message },
  ];
  const resp = await client.messages.create({
    model: ANTHROPIC_MODEL,
    max_tokens: 900,
    system: systemPrompt,
    messages,
  });
  return resp.content.map(b => (b.type === 'text' ? b.text : '')).join('').trim();
}

// ── Express app ────────────────────────────────────────────────
const app = express();
app.use(cors({ origin: CORS_ORIGIN }));
app.use(express.json());

app.get('/health', (_, res) => res.json({
  status: 'ok',
  mock_mode: MOCK_MODE,
  telemetry_configured: Boolean(TELEMETRY_API_URL),
  anthropic_configured: Boolean(ANTHROPIC_API_KEY),
  case_count: caseLibrary.cases.length,
}));

app.post('/api/chat', async (req, res) => {
  const { vin, tier, history, message } = req.body || {};
  if (!vin || !message || !['L1', 'L2', 'L3'].includes(tier)) {
    return res.status(400).json({ error: 'vin, tier (L1|L2|L3), and message are required' });
  }

  let telemetrySummary = null;
  let telemetryData = null;
  if (tier === 'L2' || tier === 'L3') {
    const telemetry = await fetchTelemetry(vin);
    telemetryData = telemetry.ok ? telemetry.data : null;
    telemetrySummary = summarizeTelemetry(telemetry);
  }

  const systemPrompt = buildSystemPrompt(tier, vin, telemetrySummary);

  try {
    const reply = await callClaude(systemPrompt, Array.isArray(history) ? history : [], message);
    log.info('Chat turn processed', { vin, tier, telemetry_used: Boolean(telemetryData) });
    res.json({ reply, tier, telemetry_used: Boolean(telemetryData), telemetry: telemetryData });
  } catch (err) {
    log.error('Chat turn failed', { vin, tier, error: err.message });
    res.status(502).json({ error: 'Assistant call failed', detail: err.message });
  }
});

app.get('/api/telemetry/:vin', async (req, res) => {
  const telemetry = await fetchTelemetry(req.params.vin);
  if (!telemetry.ok) return res.status(502).json({ error: telemetry.reason });
  res.json(telemetry.data);
});

app.post('/api/escalate', async (req, res) => {
  const { vin, transcript, telemetry_snapshot } = req.body || {};
  if (!vin || !Array.isArray(transcript)) {
    return res.status(400).json({ error: 'vin and transcript are required' });
  }

  const transcriptText = transcript
    .map(t => `[${t.role === 'assistant' ? 'Assistant' : 'Dealer'}] ${t.content}`)
    .join('\n\n');

  const subject = `[L3 Escalation] Cockpit case for ${vin}`;
  const body = [
    `Vehicle: ${vin}`,
    `Escalated at: ${new Date().toISOString()}`,
    '',
    '--- Live telemetry snapshot at time of escalation ---',
    telemetry_snapshot ? JSON.stringify(telemetry_snapshot, null, 2) : 'Not available',
    '',
    '--- Full conversation transcript (L1 → L2) ---',
    transcriptText,
  ].join('\n');

  if (MOCK_MODE) {
    log.info('Escalation (mock) — email not actually sent', { vin });
    return res.json({ status: 'mocked', to: process.env.ESCALATION_TO || null, subject });
  }

  const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ESCALATION_FROM, ESCALATION_TO } = process.env;
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS || !ESCALATION_TO) {
    return res.status(500).json({ error: 'Email is not configured (SMTP_* / ESCALATION_TO env vars missing)' });
  }

  try {
    const transporter = nodemailer.createTransport({
      host: SMTP_HOST,
      port: parseInt(SMTP_PORT || '587'),
      secure: parseInt(SMTP_PORT || '587') === 465,
      auth: { user: SMTP_USER, pass: SMTP_PASS },
    });
    const info = await transporter.sendMail({
      from: ESCALATION_FROM || SMTP_USER,
      to: ESCALATION_TO,
      subject,
      text: body,
    });
    log.info('Escalation email sent', { vin, message_id: info.messageId });
    res.json({ status: 'sent', to: ESCALATION_TO, message_id: info.messageId });
  } catch (err) {
    log.error('Escalation email failed', { vin, error: err.message });
    res.status(502).json({ error: 'Failed to send escalation email', detail: err.message });
  }
});

app.listen(PORT, () => log.info('HTTP server started', { port: PORT, mock_mode: MOCK_MODE }));
