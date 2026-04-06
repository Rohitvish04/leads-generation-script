/**
 * lead_scraper.js
 * ─────────────────────────────────────────────────────────────
 * INPUT  : input_leads.csv  (comma OR tab separated)
 *          Columns: Company Name, Website URL, Country code
 *
 * OUTPUT : output_leads.tsv  ← paste directly into any sheet
 *          output_leads.csv  ← standard CSV
 *          skipped_leads.csv ← leads with no contacts
 *
 * Each row: company_name | proper_website_url | email | phone | country
 *
 * SETUP  : npm install axios csv-parse csv-stringify
 * RUN    : node lead_scraper.js
 * ─────────────────────────────────────────────────────────────
 */

"use strict";

const fs            = require("fs");
const axios         = require("axios");
const { parse }     = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");

// ─── CONFIG ──────────────────────────────────────────────────
const CONFIG = {
  inputFile:   "input_leads.csv",
  outputFile:  "output_leads.csv",
  tsvFile:     "output_leads.tsv",
  skippedFile: "skipped_leads.csv",
  concurrency: 3,
  timeoutMs:   20000,
};
// ─────────────────────────────────────────────────────────────

// ── Clean and normalize a URL ─────────────────────────────────
// Returns a proper URL like https://www.example.com
function cleanUrl(raw) {
  if (!raw) return "";
  let url = raw.trim()
    .replace(/\s+/g, "")          // remove all spaces
    .replace(/^_{1,}/g, "")       // remove leading underscores
    .replace(/_{1,}$/g, "")       // remove trailing underscores
    .replace(/^\.+/, "")          // remove leading dots
    .replace(/\.$/, "");          // remove trailing dot

  // Add protocol if missing
  if (url && !/^https?:\/\//i.test(url)) {
    url = "https://" + url;
  }

  // Parse and reconstruct to get a clean canonical URL
  try {
    const parsed = new URL(url);
    // Return clean origin + pathname (strip trailing slash if just homepage)
    let clean = parsed.origin;
    if (parsed.pathname && parsed.pathname !== "/") {
      clean += parsed.pathname.replace(/\/$/, ""); // remove trailing slash
    }
    return clean;
  } catch {
    return url; // return as-is if URL parsing fails
  }
}

// ── Get final URL after redirects ─────────────────────────────
// Follows redirects to get the real final URL of the site
async function getFinalUrl(url) {
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
    // axios gives us the final URL after all redirects
    const finalUrl = res.request?.res?.responseUrl || res.config?.url || url;
    return { html: typeof res.data === "string" ? res.data : JSON.stringify(res.data), finalUrl };
  } catch { return null; }
}

// ── Detect delimiter ──────────────────────────────────────────
function detectDelimiter(content) {
  const firstLine = content.split("\n")[0] || "";
  const tabs   = (firstLine.match(/\t/g) || []).length;
  const commas = (firstLine.match(/,/g)  || []).length;
  return tabs >= commas ? "\t" : ",";
}

// ── Read CSV ──────────────────────────────────────────────────
function readCSV(filePath) {
  if (!fs.existsSync(filePath)) {
    console.error(`\n❌  File not found: ${filePath}`);
    console.error(`   Columns needed: Company Name, Website URL, Country code\n`);
    process.exit(1);
  }
  const content   = fs.readFileSync(filePath, "utf8");
  const delimiter = detectDelimiter(content);
  console.log(`   Delimiter detected: ${delimiter === "\t" ? "TAB" : "COMMA"}`);
  return parse(content, {
    columns:            true,
    skip_empty_lines:   true,
    trim:               true,
    delimiter,
    relax_column_count: true,
  });
}

// ── Helpers ───────────────────────────────────────────────────
function decodeHTMLEntities(text) {
  return text
    .replace(/&amp;/gi,  "&")
    .replace(/&lt;/gi,   "<")
    .replace(/&gt;/gi,   ">")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(Number(d)));
}

function uniqCase(arr) {
  const seen = new Set();
  return arr.map(s => String(s).trim()).filter(v => {
    const k = v.toLowerCase();
    if (!k || seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function normalizePhone(raw) {
  if (!raw) return null;
  let s = String(raw)
    .replace(/^tel:/i, "")
    .replace(/\(0\)/g, "")
    .replace(/[^\d+]/g, "");
  if (s.startsWith("00")) s = "+" + s.slice(2);
  const digits = s.replace(/\D/g, "");
  if (digits.length < 7 || digits.length > 15) return null;
  if (/^(\d)\1{5,}$/.test(digits))     return null;
  if (/^(0{7,}|1234567)/.test(digits)) return null;
  return s;
}

function toAbsoluteUrl(href, base) {
  try {
    if (!href || /^(javascript:|#|\s)/i.test(href.trim())) return null;
    return new URL(href.trim(), base).toString();
  } catch { return null; }
}

function stripTags(html) {
  return html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

// ── Pick best email ───────────────────────────────────────────
function pickBestEmail(emails) {
  if (!emails || emails.length === 0) return "";
  const preferred = ["info", "contact", "hello", "sales", "support", "enquiry", "inquiry", "admin"];
  for (const prefix of preferred) {
    const match = emails.find(e => e.toLowerCase().startsWith(prefix + "@"));
    if (match) return match;
  }
  return emails[0];
}

// ── Pick best phone ───────────────────────────────────────────
function pickBestPhone(phones) {
  if (!phones || phones.length === 0) return "";
  const intl = phones.find(p => p.startsWith("+"));
  if (intl) return intl;
  return phones.sort((a, b) => b.length - a.length)[0];
}

// ── Extract contacts from HTML ────────────────────────────────
function extractContacts(html, baseUrl = "") {
  const decoded = decodeHTMLEntities(html);

  // PHONES
  const telPhones = [...decoded.matchAll(/href=["']tel:([^"'\s]+)["']/gi)].map(m => m[1]);

  const ldPhones = [], ldEmails = [];
  for (const block of [...decoded.matchAll(
    /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )].map(m => m[1])) {
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

  const microdataPhones = [
    ...[...decoded.matchAll(/itemprop=["']telephone["'][^>]*content=["']([^"']+)["']/gi)].map(m => m[1]),
    ...[...decoded.matchAll(/itemprop=["']telephone["'][^>]*>\s*([^<]+)\s*</gi)].map(m => m[1]),
  ];

  const textPhones = [];
  const phoneRe = /(?<![a-zA-Z0-9@])(\+?\d[\d\s\-\.\(\)]{6,18}\d)(?![a-zA-Z0-9@])/g;
  let pm;
  while ((pm = phoneRe.exec(decoded)) !== null) textPhones.push(pm[1].trim());

  const phoneNumbers = uniqCase(
    [...telPhones, ...ldPhones, ...microdataPhones, ...textPhones]
      .map(normalizePhone).filter(Boolean)
  );

  // EMAILS
  const mailtoEmails = [...decoded.matchAll(/href=["']mailto:([^"'?\s]+)/gi)]
    .map(m => m[1].split("?")[0].toLowerCase().trim());

  const metaEmails = [
    ...[...decoded.matchAll(/itemprop=["']email["'][^>]*content=["']([^"']+)["']/gi)].map(m => m[1]),
    ...[...decoded.matchAll(/itemprop=["']email["'][^>]*>\s*([^<]+)\s*</gi)].map(m => m[1]),
  ];

  const regexEmails = (decoded.match(/\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,63}\b/g) || [])
    .map(e => e.toLowerCase().trim())
    .filter(e => {
      if (/\.(png|jpe?g|gif|svg|webp|ico|pdf|css|js|woff|ttf)$/i.test(e)) return false;
      if (/@(wixpress|sentry\.io|codefusion|example\.com|domain\.com|test\.com|yoursite|yourdomain|company\.com)/.test(e)) return false;
      if (/^[a-f0-9]{10,}@/.test(e)) return false;
      if (/^(email|mail|name|user)@(example|test|domain|email)/.test(e)) return false;
      return true;
    });

  const emails = uniqCase([...mailtoEmails, ...ldEmails, ...metaEmails, ...regexEmails]);

  // CONTACT PAGE LINKS
  const contactPageLinks = [];
  let m;
  const linkRe = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  while ((m = linkRe.exec(decoded)) !== null) {
    const href = (m[1] || "").trim();
    const text = stripTags(m[2] || "").toLowerCase();
    if (
      /contact/i.test(href) ||
      /\/(contact|reach|support|get-in-touch)(\/|$)/i.test(href) ||
      /contact\s*us/i.test(text) ||
      /get\s*in\s*touch/i.test(text) ||
      /reach\s*us/i.test(text)
    ) {
      const abs = toAbsoluteUrl(href, baseUrl);
      if (abs && !abs.includes("mailto:") && !abs.includes("tel:")) contactPageLinks.push(abs);
    }
  }

  return { emails, phoneNumbers, contactPageLinks: uniqCase(contactPageLinks) };
}

function mergeContacts(a, b) {
  return {
    emails:           uniqCase([...a.emails,       ...b.emails]),
    phoneNumbers:     uniqCase([...a.phoneNumbers, ...b.phoneNumbers]),
    contactPageLinks: uniqCase([...a.contactPageLinks, ...b.contactPageLinks]),
  };
}

// ── Scrape company ────────────────────────────────────────────
async function scrapeCompany(url) {
  const fetched = await getFinalUrl(url);
  if (!fetched) return null;

  const { html, finalUrl } = fetched;
  let result = extractContacts(html, finalUrl);

  // Also scrape contact page
  for (const contactUrl of result.contactPageLinks.slice(0, 2)) {
    if (contactUrl === finalUrl) continue;
    const cf = await getFinalUrl(contactUrl);
    if (!cf) continue;
    result = mergeContacts(result, extractContacts(cf.html, cf.finalUrl));
  }

  return { contacts: result, finalUrl };
}

// ── Async pool ────────────────────────────────────────────────
async function asyncPool(limit, items, fn) {
  const results   = new Array(items.length);
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
  process.stdout.write(`\r   [${bar}] ${pct}% (${done}/${total}) ${(label||"").slice(0,22).padEnd(22)}`);
}

// ── Write helpers ─────────────────────────────────────────────
function writeCSV(filePath, rows, headers) {
  fs.writeFileSync(filePath, stringify(rows, { header: true, columns: headers }), "utf8");
}

function writeTSV(filePath, rows, headers) {
  const lines = [
    headers.join("\t"),
    ...rows.map(r => headers.map(h => (r[h] || "").replace(/\t/g, " ")).join("\t")),
  ];
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

function createSampleInput() {
  fs.writeFileSync(CONFIG.inputFile, [
    "Company Name,Website URL,Country code",
    "Hotel Golden Regency,http://goldenregency.in/,IN",
    "Ascent BPO Services Pvt. Ltd.,https://www.ascentbpo.com/,IN",
    "Du Telecom,https://www.du.ae/,AE",
  ].join("\n"), "utf8");
  console.log(`   ✔  Created sample ${CONFIG.inputFile} — add your leads and run again.\n`);
}

// ── MAIN ──────────────────────────────────────────────────────
(async () => {
  console.log("═══════════════════════════════════════");
  console.log("        Lead Scraper — starting        ");
  console.log("═══════════════════════════════════════\n");

  if (!fs.existsSync(CONFIG.inputFile)) {
    console.log(`⚠  ${CONFIG.inputFile} not found. Creating sample…`);
    createSampleInput();
    process.exit(0);
  }

  const leads = readCSV(CONFIG.inputFile);
  console.log(`✔  Loaded ${leads.length} leads\n`);
  console.log(`   Scraping + extracting proper URLs, one email, one phone…\n`);

  let done = 0;
  const matched = [];
  const skipped = [];

  await asyncPool(CONFIG.concurrency, leads, async (lead) => {
    const rawUrl  = (lead["Website URL"] || lead["Website  URL"] || "").trim();
    const name    = (lead["Company Name"] || "").trim();
    const country = (lead["Country code"] || lead["Country Code"] || "").trim();

    // Step 1: clean the raw URL from input
    const cleanedUrl = cleanUrl(rawUrl);

    if (!cleanedUrl) {
      skipped.push({ company_name: name, proper_website_url: "", country, skip_reason: "No website URL" });
      done++; progress(done, leads.length, name);
      return;
    }

    // Step 2: scrape and get the FINAL url after redirects
    const result = await scrapeCompany(cleanedUrl);

    if (!result) {
      skipped.push({ company_name: name, proper_website_url: cleanedUrl, country, skip_reason: "Scrape failed / timeout" });
      done++; progress(done, leads.length, name);
      return;
    }

    const { contacts, finalUrl } = result;

    // Step 3: build proper website URL from final URL (origin only)
    let properUrl = cleanedUrl;
    try {
      const parsed = new URL(finalUrl);
      properUrl = parsed.origin; // e.g. https://www.example.com
    } catch {}

    // Step 4: pick exactly ONE email and ONE phone
    const email = pickBestEmail(contacts.emails);
    const phone = pickBestPhone(contacts.phoneNumbers);

    if (!email && !phone) {
      skipped.push({ company_name: name, proper_website_url: properUrl, country, skip_reason: "No email or phone found" });
      done++; progress(done, leads.length, name);
      return;
    }

    matched.push({
      company_name:        name,
      proper_website_url:  properUrl,   // clean proper URL e.g. https://www.example.com
      email:               email,        // exactly ONE
      phone:               phone,        // exactly ONE
      country:             country,
    });

    done++; progress(done, leads.length, name);
  });

  console.log("\n");

  const headers = ["company_name", "proper_website_url", "email", "phone", "country"];
  writeCSV(CONFIG.outputFile,  matched,  headers);
  writeTSV(CONFIG.tsvFile,     matched,  headers);
  writeCSV(CONFIG.skippedFile, skipped,  ["company_name", "proper_website_url", "country", "skip_reason"]);

  console.log("═══════════════════════════════════════");
  console.log("               RESULTS                 ");
  console.log("═══════════════════════════════════════");
  console.log(`  Total processed : ${leads.length}`);
  console.log(`  With contacts   : ${matched.length}`);
  console.log(`  Skipped         : ${skipped.length}`);
  console.log("═══════════════════════════════════════\n");
  console.log(`  📄  ${CONFIG.outputFile}   ← standard CSV`);
  console.log(`  📋  ${CONFIG.tsvFile}   ← paste into Excel / Google Sheets`);
  console.log(`  ⚠   ${CONFIG.skippedFile}\n`);

  if (matched.length > 0) {
    console.log("── Preview ──────────────────────────────────────────────────────");
    console.log("  " + "Company".padEnd(24) + "Proper URL".padEnd(30) + "Phone".padEnd(16) + "Email");
    console.log("  " + "─".repeat(90));
    matched.slice(0, 5).forEach(r => {
      const n = (r.company_name       || "").slice(0, 22).padEnd(24);
      const u = (r.proper_website_url || "").slice(0, 28).padEnd(30);
      const p = (r.phone              || "—").slice(0, 14).padEnd(16);
      const e = (r.email              || "—").slice(0, 35);
      console.log(`  ${n}${u}${p}${e}`);
    });
    console.log("  " + "─".repeat(90));
    console.log(`\n✅  Open output_leads.tsv → Ctrl+A → Ctrl+C → paste into your sheet.\n`);
  } else {
    console.log(`⚠  No contacts found. Check that Website URL column has valid URLs.\n`);
  }
})();