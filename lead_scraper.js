/**
 * lead_scraper.js
 * ─────────────────────────────────────────────────────────────
 * FLOW:
 *   1. Fetch leads from Apify dataset API
 *   2. Scrape each website for email + phone
 *   3. Save to output_leads.csv + output_leads.tsv
 *
 * OUTPUT COLUMNS:
 *   company_name | company_web_url | email | phone | country | industry
 *
 * SETUP:
 *   npm install axios csv-stringify
 *
 * RUN:
 *   node lead_scraper.js
 * ─────────────────────────────────────────────────────────────
 */

"use strict";

const fs            = require("fs");
const axios         = require("axios");
require('dotenv').config();
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const { parsePhoneNumberFromString } = require("libphonenumber-js");
const { stringify } = require("csv-stringify/sync");

// ─── CONFIG ───────────────────────────────────────────────────
const CONFIG = {
  apifyUrl:    `https://api.apify.com/v2?token=${APIFY_TOKEN}`,
  outputFile:  "output_leads.csv",
  tsvFile:     "output_leads.tsv",
  skippedFile: "skipped_leads.csv",
  concurrency: 3,
  timeoutMs:   20000,
};
// ─────────────────────────────────────────────────────────────

// ── OUTPUT COLUMNS (exact order) ─────────────────────────────
const OUTPUT_HEADERS = [
  "company_name",
  "company_web_url",
  "email",
  "phone",
  "country",
  "industry",
];

// ── Fetch all leads from Apify (handles pagination) ───────────
async function fetchFromApify() {
  console.log("   Fetching leads from Apify…");
  const allItems = [];
  let offset = 0;
  const limit = 1000;

  while (true) {
    try {
      const url = `${CONFIG.apifyUrl}&limit=${limit}&offset=${offset}`;
      const res  = await axios.get(url, { timeout: 30000 });
      const items = Array.isArray(res.data) ? res.data : (res.data.items || []);
      if (items.length === 0) break;
      allItems.push(...items);
      process.stdout.write(`\r   Fetched ${allItems.length} items…`);
      if (items.length < limit) break;
      offset += limit;
    } catch (err) {
      console.error(`\n❌  Apify fetch failed: ${err.message}`);
      if (err.response?.status === 401) {
        console.error("   → Invalid token. Update CONFIG.apifyUrl with your token.");
      }
      process.exit(1);
    }
  }

  console.log(`\r   ✔  Fetched ${allItems.length} items from Apify      \n`);
  return allItems;
}

// ── Map Apify item fields → lead object ───────────────────────
function mapApifyItem(item) {
  return {
    // ✅ company_name now from title
    company_name: item.title || "",

    // ✅ website stays same
    raw_url: item.websiteUrl || item.website || item.websiteURL || item.web || "",

    // ✅ country stays same
    country: item.headquarter?.country || item.country || item.countryCode || "",

    // ✅ industry now from categoryName
    industry: item.categoryName || "",
  };
}

// ── Clean URL ─────────────────────────────────────────────────
function cleanUrl(raw) {
  if (!raw) return "";
  let url = raw.trim().replace(/\s+/g, "").replace(/^_+|_+$/g, "").replace(/^\.+|\.+$/g, "");
  if (!/^https?:\/\//i.test(url)) url = "https://" + url;
  try {
    const parsed = new URL(url);
    let clean = parsed.origin;
    if (parsed.pathname && parsed.pathname !== "/") clean += parsed.pathname.replace(/\/$/, "");
    return clean;
  } catch { return url; }
}

// ── Fetch HTML (returns html + final URL after redirects) ─────
async function fetchHTML(url) {
  try {
    const res = await axios.get(url, {
      timeout: CONFIG.timeoutMs,
      headers: {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
      },
      maxRedirects: 5,
    });
    const finalUrl = res.request?.res?.responseUrl || res.config?.url || url;
    return { html: typeof res.data === "string" ? res.data : JSON.stringify(res.data), finalUrl };
  } catch { return null; }
}

// ── Helpers ───────────────────────────────────────────────────
function decodeHTML(text) {
  return text
    .replace(/&amp;/gi, "&").replace(/&lt;/gi, "<").replace(/&gt;/gi, ">")
    .replace(/&nbsp;/gi, " ").replace(/&#(\d+);/g, (_, d) => String.fromCharCode(Number(d)));
}

function uniqCase(arr) {
  const seen = new Set();
  return arr.map(s => String(s).trim()).filter(v => {
    const k = v.toLowerCase();
    if (!k || seen.has(k)) return false;
    seen.add(k); return true;
  });
}

function normalizePhone(raw, countryHint = "US") {
  if (!raw) return null;

  let cleaned = String(raw)
    .replace(/^tel:/i, "")
    .replace(/\(0\)/g, "")
    .replace(/[^\d+]/g, "");

  if (cleaned.startsWith("00")) cleaned = "+" + cleaned.slice(2);

  try {
    const phone = parsePhoneNumberFromString(cleaned, countryHint);

    if (!phone || !phone.isValid()) return null;

    const number = phone.number; // +1234567890

    const digits = number.replace(/\D/g, "");

    // extra safety
    if (digits.length < 10 || digits.length > 12) return null;
    if (/^(\d)\1{6,}$/.test(digits)) return null;

    return number;
  } catch {
    return null;
  }
}

function toAbsoluteUrl(href, base) {
  try {
    if (!href || /^(javascript:|#)/i.test(href.trim())) return null;
    return new URL(href.trim(), base).toString();
  } catch { return null; }
}

// ── Pick best email ───────────────────────────────────────────
function pickBestEmail(emails) {
  if (!emails?.length) return "";
  const order = ["info", "contact", "hello", "sales", "support", "enquiry", "inquiry", "admin"];
  for (const prefix of order) {
    const hit = emails.find(e => e.toLowerCase().startsWith(prefix + "@"));
    if (hit) return hit;
  }
  return emails[0];
}

// ── Pick best phone ───────────────────────────────────────────
function pickBestPhone(phones) {
  if (!phones?.length) return "";

  // prefer valid intl numbers
  const intl = phones.find(p => p.startsWith("+"));
  if (intl) return intl;

  // prefer US format
  const us = phones.find(p => p.startsWith("+1"));
  if (us) return us;

  return phones[0];
}

// ── Extract contacts from HTML ────────────────────────────────
function extractContacts(html, baseUrl = "") {
  const d = decodeHTML(html);

  // Phones
  const telPhones  = [...d.matchAll(/href=["']tel:([^"'\s]+)["']/gi)].map(m => m[1]);
  const ldPhones   = [], ldEmails = [];
  for (const block of [...d.matchAll(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)].map(m => m[1])) {
    try {
      (function walk(n) {
        if (!n) return;
        if (Array.isArray(n)) { n.forEach(walk); return; }
        if (typeof n === "object") {
          for (const [k, v] of Object.entries(n)) {
            if (k.toLowerCase() === "telephone" && typeof v === "string") ldPhones.push(v);
            if (k.toLowerCase() === "email"     && typeof v === "string") ldEmails.push(v);
            if (typeof v === "object") walk(v);
          }
        }
      })(JSON.parse(block.trim()));
    } catch {}
  }
  const microPhones = [
    ...[...d.matchAll(/itemprop=["']telephone["'][^>]*content=["']([^"']+)["']/gi)].map(m => m[1]),
    ...[...d.matchAll(/itemprop=["']telephone["'][^>]*>\s*([^<]+)\s*</gi)].map(m => m[1]),
  ];
  const textPhones = [];
  const phoneRe = /(?<![a-zA-Z0-9@])(\+?\d[\d\s\-\.\(\)]{6,18}\d)(?![a-zA-Z0-9@])/g;
  let pm;
  while ((pm = phoneRe.exec(d)) !== null) textPhones.push(pm[1].trim());

  const phoneNumbers = uniqCase([...telPhones, ...ldPhones, ...microPhones, ...textPhones].map(p => normalizePhone(p, baseUrl.includes(".in") ? "IN" : "US")).filter(Boolean));

  // Emails
  const mailtoEmails = [...d.matchAll(/href=["']mailto:([^"'?\s]+)/gi)].map(m => m[1].split("?")[0].toLowerCase().trim());
  const metaEmails   = [
    ...[...d.matchAll(/itemprop=["']email["'][^>]*content=["']([^"']+)["']/gi)].map(m => m[1]),
    ...[...d.matchAll(/itemprop=["']email["'][^>]*>\s*([^<]+)\s*</gi)].map(m => m[1]),
  ];
  const regexEmails = (d.match(/\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,63}\b/g) || [])
    .map(e => e.toLowerCase().trim())
    .filter(e => {
      if (/\.(png|jpe?g|gif|svg|webp|ico|pdf|css|js|woff|ttf)$/i.test(e)) return false;
      if (/@(wixpress|sentry\.io|codefusion|example\.com|domain\.com|test\.com|yoursite|yourdomain)/.test(e)) return false;
      if (/^[a-f0-9]{10,}@/.test(e)) return false;
      return true;
    });

  const emails = uniqCase([...mailtoEmails, ...ldEmails, ...metaEmails, ...regexEmails]);

  // Contact page links
  const contactPageLinks = [];
  let m;
  const linkRe = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  while ((m = linkRe.exec(d)) !== null) {
    const href = (m[1] || "").trim();
    const text = (m[2] || "").replace(/<[^>]*>/g, " ").trim().toLowerCase();
    if (/contact/i.test(href) || /\/(contact|reach|support)(\/|$)/i.test(href) ||
        /contact\s*us/i.test(text) || /get\s*in\s*touch/i.test(text)) {
      const abs = toAbsoluteUrl(href, baseUrl);
      if (abs && !abs.includes("mailto:") && !abs.includes("tel:")) contactPageLinks.push(abs);
    }
  }

  return { emails, phoneNumbers, contactPageLinks: uniqCase(contactPageLinks) };
}

function createKey(lead) {
  const domain = (lead.company_web_url || "")
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];

  return domain || lead.company_name.toLowerCase();
}

function dedupeLeads(leads) {
  const map = new Map();

  for (const lead of leads) {
    const key = createKey(lead);

    if (!map.has(key)) {
      map.set(key, lead);
    } else {
      const existing = map.get(key);

      map.set(key, {
        ...existing,
        email: existing.email || lead.email,
        phone: existing.phone || lead.phone,
      });
    }
  }

  return Array.from(map.values());
}

function mergeContacts(a, b) {
  return {
    emails:           uniqCase([...a.emails,       ...b.emails]),
    phoneNumbers:     uniqCase([...a.phoneNumbers, ...b.phoneNumbers]),
    contactPageLinks: uniqCase([...a.contactPageLinks, ...b.contactPageLinks]),
  };
}

// ── Scrape one company ────────────────────────────────────────
async function scrapeCompany(url) {
  const fetched = await fetchHTML(url);
  if (!fetched) return null;
  let result = extractContacts(fetched.html, fetched.finalUrl);

  for (const contactUrl of result.contactPageLinks.slice(0, 2)) {
    if (contactUrl === fetched.finalUrl) continue;
    const cf = await fetchHTML(contactUrl);
    if (!cf) continue;
    result = mergeContacts(result, extractContacts(cf.html, cf.finalUrl));
  }

  let properUrl = url;
  try { properUrl = new URL(fetched.finalUrl).origin; } catch {}

  return { contacts: result, properUrl };
}

// ── Async pool ────────────────────────────────────────────────
async function asyncPool(limit, items, fn) {
  const results = new Array(items.length);
  const executing = new Set();
  for (let i = 0; i < items.length; i++) {
    const p = fn(items[i], i).then(r => { results[i] = r; executing.delete(p); });
    executing.add(p);
    if (executing.size >= limit) await Promise.race(executing);
  }
  await Promise.all(executing);
  return results;
}

// ── Progress bar ──────────────────────────────────────────────
function progress(done, total, label) {
  const pct = Math.round((done / total) * 100);
  const bar  = "█".repeat(Math.floor(pct / 5)) + "░".repeat(20 - Math.floor(pct / 5));
  process.stdout.write(`\r   [${bar}] ${pct}% (${done}/${total}) ${(label||"").slice(0,20).padEnd(20)}`);
}

// ── Write CSV ─────────────────────────────────────────────────
function writeCSV(filePath, rows, headers) {
  fs.writeFileSync(filePath, stringify(rows, { header: true, columns: headers }), "utf8");
}

// ── Write TSV (tab-separated for direct paste into sheets) ────
function writeTSV(filePath, rows, headers) {
  const lines = [
    headers.join("\t"),
    ...rows.map(r => headers.map(h => (r[h] || "").replace(/\t/g, " ")).join("\t")),
  ];
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

// ── MAIN ──────────────────────────────────────────────────────
(async () => {
  console.log("═══════════════════════════════════════");
  console.log("        Lead Scraper — starting        ");
  console.log("═══════════════════════════════════════\n");

  // Step 1 — fetch from Apify
  const rawItems = await fetchFromApify();
  const leads    = rawItems.map(mapApifyItem).filter(l => l.company_name || l.raw_url);
  console.log(`✔  Mapped ${leads.length} leads\n`);
  console.log(`   Scraping websites…\n`);

  let done = 0;
  const matched = [];
  const skipped = [];

  // Step 2 — scrape each company
  await asyncPool(CONFIG.concurrency, leads, async (lead) => {
    const name       = lead.company_name;
    const country    = lead.country;
    const industry   = lead.industry;
    const cleanedUrl = cleanUrl(lead.raw_url);

    if (!cleanedUrl) {
      skipped.push({ company_name: name, company_web_url: "", email: "", phone: "", country, industry, skip_reason: "No website URL" });
      done++; progress(done, leads.length, name);
      return;
    }

    const result = await scrapeCompany(cleanedUrl);

    if (!result) {
      skipped.push({ company_name: name, company_web_url: cleanedUrl, email: "", phone: "", country, industry, skip_reason: "Scrape failed / timeout" });
      done++; progress(done, leads.length, name);
      return;
    }

    const { contacts, properUrl } = result;
    const email = pickBestEmail(contacts.emails) || "";
    const phone = pickBestPhone(contacts.phoneNumbers);

    if (!email && !phone) {
      skipped.push({ company_name: name, company_web_url: properUrl, email: "", phone: "", country, industry, skip_reason: "No email or phone found" });
      done++; progress(done, leads.length, name);
      return;
    }

    // ── Exact output format ──────────────────────────────────
    matched.push({
      company_name:    name,
      company_web_url: properUrl,
      email:           email,
      phone:           phone,
      country:         country,
      industry:        industry,
    });

    done++; progress(done, leads.length, name);
  });

  console.log("\n");

  // Step 3 — write output files
  const finalLeads = dedupeLeads(matched);

writeCSV(CONFIG.outputFile,  finalLeads, OUTPUT_HEADERS);
writeTSV(CONFIG.tsvFile,     finalLeads, OUTPUT_HEADERS);
  writeTSV(CONFIG.tsvFile,     matched, OUTPUT_HEADERS);
  writeCSV(CONFIG.skippedFile, skipped, [...OUTPUT_HEADERS, "skip_reason"]);

  // Summary
  console.log("═══════════════════════════════════════");
  console.log("               RESULTS                 ");
  console.log("═══════════════════════════════════════");
  console.log(`  Fetched from Apify : ${leads.length}`);
  console.log(`  With contacts      : ${matched.length}`);
  console.log(`  Skipped            : ${skipped.length}`);
  console.log("═══════════════════════════════════════\n");
  console.log(`  📄  ${CONFIG.outputFile}`);
  console.log(`  📋  ${CONFIG.tsvFile}   ← paste into Excel / Google Sheets`);
  console.log(`  ⚠   ${CONFIG.skippedFile}\n`);

  if (matched.length > 0) {
    console.log("── Preview ──────────────────────────────────────────────────────────────");
    console.log(
      "  " +
      "company_name".padEnd(24) +
      "company_web_url".padEnd(28) +
      "email".padEnd(26) +
      "phone".padEnd(16) +
      "country".padEnd(8) +
      "industry"
    );
    console.log("  " + "─".repeat(110));
    matched.slice(0, 5).forEach(r => {
      console.log(
        "  " +
        (r.company_name    || "").slice(0, 22).padEnd(24) +
        (r.company_web_url || "").slice(0, 26).padEnd(28) +
        (r.email           || "—").slice(0, 24).padEnd(26) +
        (r.phone           || "—").slice(0, 14).padEnd(16) +
        (r.country         || "").padEnd(8) +
        (r.industry        || "—").slice(0, 30)
      );
    });
    console.log("  " + "─".repeat(110));
    console.log(`\n✅  Open output_leads.tsv → Ctrl+A → Ctrl+C → paste into your sheet.\n`);
  } else {
    console.log(`⚠  No contacts found. Check your Apify token and dataset ID.\n`);
  }
})();
