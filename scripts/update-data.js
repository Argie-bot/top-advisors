#!/usr/bin/env node
/**
 * Advisor Recognition Intelligence — Data Update Script
 *
 * Fetches the latest advisor lists from Forbes/SHOOK, Barron's, AdvisorHub,
 * and InvestmentNews, then builds the compact advisor-data.js file.
 *
 * Usage:
 *   node scripts/update-data.js                 # fetch all publications
 *   node scripts/update-data.js --forbes        # fetch Forbes only
 *   node scripts/update-data.js --barrons       # fetch Barron's only
 *   node scripts/update-data.js --advisorhub    # fetch AdvisorHub only
 *   node scripts/update-data.js --investmentnews # fetch InvestmentNews only
 *   node scripts/update-data.js --dry-run       # fetch & parse but don't write
 *
 * Environment:
 *   DATA_YEAR  — override the target year (default: current year)
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const { execSync } = require('child_process');

// ============================================================
// CONFIGURATION
// ============================================================

const YEAR = process.env.DATA_YEAR || new Date().getFullYear().toString();
const PREV_YEAR = (parseInt(YEAR) - 1).toString();
const OUT_DIR = path.resolve(__dirname, '..');
const CACHE_DIR = path.resolve(__dirname, '..', '.cache');
const DRY_RUN = process.argv.includes('--dry-run');
const FETCH_ALL = !process.argv.some(a => ['--forbes','--barrons','--advisorhub','--investmentnews'].includes(a));
const FETCH_FORBES = FETCH_ALL || process.argv.includes('--forbes');
const FETCH_BARRONS = FETCH_ALL || process.argv.includes('--barrons');
const FETCH_ADVISORHUB = FETCH_ALL || process.argv.includes('--advisorhub');
const FETCH_INVESTMENTNEWS = FETCH_ALL || process.argv.includes('--investmentnews');

// Forbes API (paginated JSON)
const FORBES_API = `https://www.forbes.com/forbesapi/org/wealth-management-teams-best-in-state/${YEAR}/position/true.json`;
const FORBES_PAGE_SIZE = 200;

// Barron's URLs (HTML with embedded __STATE__ JSON)
const BARRONS_BASE = 'https://www.barrons.com/advisor/report/top-financial-advisors';
const BARRONS_LISTS = [
  { slug: '100',             listName: `Top 100 Advisors ${PREV_YEAR}`,      type: 'individual' },
  { slug: '1200',            listName: `Top 1200 Advisors ${PREV_YEAR}`,     type: 'individual' },
  { slug: 'independent/100', listName: `Top 100 Independent ${PREV_YEAR}`,   type: 'individual' },
  { slug: 'women/100',       listName: `Top 100 Women ${PREV_YEAR}`,         type: 'individual' },
  { slug: 'private-wealth',  listName: `Top 250 PW Teams ${PREV_YEAR}`,      type: 'teams' },
];

// AdvisorHub URLs (HTML with embedded wpDataTables)
const ADVISORHUB_LISTS = [
  { url: `/advisors-to-watch-solo-${PREV_YEAR}/`,     listName: `Advisors to Watch: Solo ${PREV_YEAR}` },
  { url: `/advisors-to-watch-next-gen-${PREV_YEAR}/`,  listName: `Advisors to Watch: Next Gen ${PREV_YEAR}` },
  { url: `/advisors-to-watch-over-1b-${PREV_YEAR}/`,   listName: `Advisors to Watch: Over $1B ${PREV_YEAR}` },
  { url: `/advisors-to-watch-under-1b-${PREV_YEAR}/`,  listName: `Advisors to Watch: Under $1B ${PREV_YEAR}` },
  { url: `/advisors-to-watch-ria-${PREV_YEAR}/`,       listName: `Advisors to Watch: RIA ${PREV_YEAR}` },
];

// InvestmentNews
const INVESTMENTNEWS_HNW_URL = `https://www.investmentnews.com/hnw-advisors-${PREV_YEAR}`;

// User-Agent for curl requests
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

// State abbreviation map (Barron's uses AP-style abbreviations)
const STATE_MAP = {
  'Ala.': 'Alabama', 'Alaska': 'Alaska', 'Ariz.': 'Arizona', 'Ark.': 'Arkansas',
  'Calif.': 'California', 'Colo.': 'Colorado', 'Conn.': 'Connecticut', 'Del.': 'Delaware',
  'D.C.': 'Washington D.C.', 'Fla.': 'Florida', 'Ga.': 'Georgia', 'Hawaii': 'Hawaii',
  'Idaho': 'Idaho', 'Ill.': 'Illinois', 'Ind.': 'Indiana', 'Iowa': 'Iowa',
  'Kan.': 'Kansas', 'Ky.': 'Kentucky', 'La.': 'Louisiana', 'Maine': 'Maine',
  'Md.': 'Maryland', 'Mass.': 'Massachusetts', 'Mich.': 'Michigan', 'Minn.': 'Minnesota',
  'Miss.': 'Mississippi', 'Mo.': 'Missouri', 'Mont.': 'Montana', 'Neb.': 'Nebraska',
  'Nev.': 'Nevada', 'N.H.': 'New Hampshire', 'N.J.': 'New Jersey', 'N.M.': 'New Mexico',
  'N.Y.': 'New York', 'N.C.': 'North Carolina', 'N.D.': 'North Dakota', 'Ohio': 'Ohio',
  'Okla.': 'Oklahoma', 'Ore.': 'Oregon', 'Pa.': 'Pennsylvania', 'R.I.': 'Rhode Island',
  'S.C.': 'South Carolina', 'S.D.': 'South Dakota', 'Tenn.': 'Tennessee', 'Texas': 'Texas',
  'Utah': 'Utah', 'Vt.': 'Vermont', 'Va.': 'Virginia', 'Wash.': 'Washington',
  'W.Va.': 'West Virginia', 'Wis.': 'Wisconsin', 'Wyo.': 'Wyoming'
};

function expandState(abbr) {
  return STATE_MAP[abbr] || abbr;
}

// ============================================================
// HTTP / CURL HELPERS
// ============================================================

function fetchJSON(url) {
  return new Promise(function(resolve, reject) {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      headers: { 'User-Agent': UA, 'Accept': 'application/json' }
    };
    https.get(options, function(res) {
      if (res.statusCode === 404) { resolve(null); return; }
      if (res.statusCode !== 200) { reject(new Error('HTTP ' + res.statusCode + ' for ' + url)); return; }
      let data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        try { resolve(JSON.parse(data)); } catch(e) { reject(e); }
      });
    }).on('error', reject);
  });
}

function curlFetch(url) {
  // Use curl for sites that block Node.js https (Barron's, AdvisorHub)
  try {
    const result = execSync(
      `curl -sL -A "${UA}" "${url}"`,
      { maxBuffer: 50 * 1024 * 1024, timeout: 60000, encoding: 'utf8' }
    );
    return result;
  } catch(e) {
    console.warn('  curl failed for ' + url + ': ' + e.message);
    return null;
  }
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function cacheWrite(filename, content) {
  ensureDir(CACHE_DIR);
  fs.writeFileSync(path.join(CACHE_DIR, filename), content);
}

function cacheRead(filename) {
  const fp = path.join(CACHE_DIR, filename);
  if (fs.existsSync(fp)) return fs.readFileSync(fp, 'utf8');
  return null;
}

// ============================================================
// 1. FORBES/SHOOK
// ============================================================

async function fetchForbes() {
  console.log('\n=== FORBES/SHOOK ===');
  console.log('API: ' + FORBES_API);

  let allTeams = [];
  let start = 0;
  let pageNum = 0;

  while (true) {
    const url = FORBES_API + '?limit=' + FORBES_PAGE_SIZE + '&start=' + start;
    process.stdout.write('  Fetching page ' + pageNum + ' (start=' + start + ')... ');

    const data = await fetchJSON(url);
    if (!data) {
      console.log('404 — list not yet published for ' + YEAR);
      return [];
    }

    const teams = data.organizationList ? data.organizationList.organizationsLists : [];
    console.log(teams.length + ' teams');

    if (teams.length === 0) break;
    allTeams = allTeams.concat(teams);

    // Cache raw page
    cacheWrite('forbes_page_' + pageNum + '.json', JSON.stringify(data));

    start += FORBES_PAGE_SIZE;
    pageNum++;

    // Safety: max 100 pages (20,000 teams)
    if (pageNum > 100) break;
  }

  // Deduplicate by naturalId
  const seen = new Set();
  allTeams = allTeams.filter(function(t) {
    if (seen.has(t.naturalId)) return false;
    seen.add(t.naturalId);
    return true;
  });
  console.log('  Unique teams: ' + allTeams.length);

  // Parse QA fields
  function getQA(team, question) {
    if (!team.qas) return '';
    const qa = team.qas.find(function(q) { return q.question === question; });
    return qa ? qa.answer : '';
  }

  // Build records (one per group member)
  let records = [];
  allTeams.forEach(function(team) {
    const teamName = team.organizationName || '';
    const firm = team.parentCompany ? team.parentCompany.name : '';
    const state = team.state || '';
    const city = team.city || '';
    const rank = team.industryRanks && team.industryRanks[0] ? team.industryRanks[0].rank : '';
    const category = team.industryRanks && team.industryRanks[0] ? team.industryRanks[0].industry : '';
    const teamAssets = getQA(team, 'Team Assets');
    const minAccount = getQA(team, 'Minimum account size for new business');
    const typicalNetWorth = getQA(team, 'Typical Net Worth of Relationships');
    const typicalHousehold = getQA(team, 'Typical size of Household accounts');

    const members = team.groupMembers || [];
    if (members.length > 0) {
      members.forEach(function(member) {
        records.push({
          name: member.name,
          teamName: teamName,
          firm: firm, state: state, city: city,
          rank: rank, category: category, teamAssets: teamAssets,
          minAccount: minAccount, typicalNetWorth: typicalNetWorth, typicalHousehold: typicalHousehold,
          publication: 'Forbes/SHOOK',
          list: 'Best-in-State Teams ' + YEAR,
          clientTypes: ''
        });
      });
    } else {
      records.push({
        name: teamName,
        teamName: teamName,
        firm: firm, state: state, city: city,
        rank: rank, category: category, teamAssets: teamAssets,
        minAccount: minAccount, typicalNetWorth: typicalNetWorth, typicalHousehold: typicalHousehold,
        publication: 'Forbes/SHOOK',
        list: 'Best-in-State Teams ' + YEAR,
        clientTypes: ''
      });
    }
  });

  console.log('  Individual records: ' + records.length);
  return records;
}

// ============================================================
// 2. BARRON'S
// ============================================================

function parseBarronsIndividual(html, listName) {
  const regex = /"data":\[(\{"202[456] Rank".*?\})\]/g;
  let records = [];
  let m;
  while ((m = regex.exec(html)) !== null) {
    try {
      const arr = JSON.parse('[' + m[1] + ']');
      arr.forEach(function(r) {
        // Advisor name is in HTML link: <a href="...">Name</a>
        const nameMatch = r.Advisor ? r.Advisor.match(/>([^<]+)</) : null;
        const rankKey = Object.keys(r).find(function(k) { return k.match(/^\d{4} Rank$/); });
        records.push({
          name: nameMatch ? nameMatch[1] : (r.Advisor || ''),
          teamName: '',
          firm: r.Firm || '',
          state: expandState(r.State || ''),
          city: r.City || '',
          rank: rankKey ? r[rankKey] : '',
          category: '',
          teamAssets: r['Team Assets\n($mil)'] ? '$' + r['Team Assets\n($mil)'] + 'M' : '',
          minAccount: r['Typical Account\n($mil)'] ? '$' + r['Typical Account\n($mil)'] + 'M' : '',
          typicalNetWorth: r['Typical Net Worth\n($mil)'] ? '$' + r['Typical Net Worth\n($mil)'] + 'M' : '',
          typicalHousehold: '',
          clientTypes: r['Client type(s)'] || '',
          publication: "Barron's",
          list: listName
        });
      });
    } catch(e) { /* skip parse errors */ }
  }
  return records;
}

function parseBarronsTeams(html, listName) {
  const regex = /"data":\[(\{"202[456] Rank".*?\})\]/g;
  let records = [];
  let m;
  while ((m = regex.exec(html)) !== null) {
    try {
      const arr = JSON.parse('[' + m[1] + ']');
      arr.forEach(function(r) {
        const teamName = r.Team || '';
        const location = r.Location || '';
        const locParts = location.split(', ');
        const city = locParts[0] || '';
        const state = expandState(locParts.slice(1).join(', '));
        const keyAdvisors = r['Key Advisor(s)'] || '';
        const teamAssetsBil = r['Team Assets ($bil)'] || '';
        const rankKey = Object.keys(r).find(function(k) { return k.match(/^\d{4} Rank$/); });
        const rank = rankKey ? r[rankKey] : '';

        const advisors = keyAdvisors ? keyAdvisors.split(',').map(function(n) { return n.trim(); }).filter(Boolean) : [teamName];
        advisors.forEach(function(advisor) {
          records.push({
            name: advisor,
            teamName: teamName,
            firm: r.Firm || '',
            state: state, city: city,
            rank: rank, category: '',
            teamAssets: teamAssetsBil ? '$' + teamAssetsBil + 'B' : '',
            minAccount: '', typicalNetWorth: '', typicalHousehold: '',
            clientTypes: '',
            publication: "Barron's",
            list: listName
          });
        });
      });
    } catch(e) { /* skip parse errors */ }
  }
  return records;
}

async function fetchBarrons() {
  console.log("\n=== BARRON'S ===");
  let allRecords = [];

  for (const list of BARRONS_LISTS) {
    const url = BARRONS_BASE + '/' + list.slug;
    const cacheFile = 'barrons_' + list.slug.replace(/\//g, '_') + '.html';
    process.stdout.write('  Fetching ' + list.listName + '... ');

    let html = curlFetch(url);
    if (!html || html.length < 1000) {
      console.log('FAILED or empty — skipping');
      // Try cache
      html = cacheRead(cacheFile);
      if (html) {
        console.log('  Using cached version');
      } else {
        continue;
      }
    } else {
      cacheWrite(cacheFile, html);
    }

    let records;
    if (list.type === 'teams') {
      records = parseBarronsTeams(html, list.listName);
    } else {
      records = parseBarronsIndividual(html, list.listName);
    }
    console.log(records.length + ' records');
    allRecords = allRecords.concat(records);
  }

  console.log("  Barron's total: " + allRecords.length);
  return allRecords;
}

// ============================================================
// 3. ADVISORHUB
// ============================================================

function parseAdvisorHubHTML(html, listName) {
  const tableMatch = html.match(/wpdatatable_id="\d+"[\s\S]*?<\/table>/);
  if (!tableMatch) return [];

  // Get headers
  const headerSection = tableMatch[0].match(/<thead[\s\S]*?<\/thead>/);
  let headers = [];
  if (headerSection) {
    const thPattern = /<th[^>]*>[\s\S]*?<\/th>/g;
    let m;
    while ((m = thPattern.exec(headerSection[0])) !== null) {
      headers.push(m[0].replace(/<[^>]+>/g, '').trim());
    }
  }

  // Get data rows
  const tbodyMatch = tableMatch[0].match(/<tbody[\s\S]*?<\/tbody>/);
  if (!tbodyMatch) return [];

  const rowPattern = /<tr[\s\S]*?<\/tr>/g;
  let records = [];
  let m;
  while ((m = rowPattern.exec(tbodyMatch[0])) !== null) {
    const cellPattern = /<td[^>]*>([\s\S]*?)<\/td>/g;
    let cells = [];
    let cm;
    while ((cm = cellPattern.exec(m[0])) !== null) {
      cells.push(cm[1].replace(/<[^>]+>/g, '').trim());
    }
    if (cells.length > 0) {
      let record = {};
      headers.forEach(function(h, i) { record[h] = cells[i] || ''; });

      const name = (record['Full Name 1'] || record['PREV NAME'] || record['Name'] || '').replace(/\*$/, '').trim();
      const cityState = record['City, State'] || '';
      const parts = cityState.split(', ');

      records.push({
        name: name,
        teamName: record['Team'] || '',
        firm: record['Firm'] || '',
        state: parts.slice(1).join(', ') || '',
        city: parts[0] || '',
        rank: record['Rank'] || record['Ranking'] || '',
        category: '', teamAssets: '', minAccount: '',
        typicalNetWorth: '', typicalHousehold: '', clientTypes: '',
        publication: 'AdvisorHub',
        list: listName
      });
    }
  }
  return records;
}

async function fetchAdvisorHub() {
  console.log('\n=== ADVISORHUB ===');
  let allRecords = [];

  for (const list of ADVISORHUB_LISTS) {
    const url = 'https://www.advisorhub.com' + list.url;
    const cacheFile = 'advisorhub_' + list.url.replace(/[^a-z0-9]/g, '_') + '.html';
    process.stdout.write('  Fetching ' + list.listName + '... ');

    let html = curlFetch(url);
    if (!html || html.length < 1000) {
      console.log('FAILED or empty — skipping');
      html = cacheRead(cacheFile);
      if (html) {
        console.log('  Using cached version');
      } else {
        continue;
      }
    } else {
      cacheWrite(cacheFile, html);
    }

    const records = parseAdvisorHubHTML(html, list.listName);
    console.log(records.length + ' records');
    allRecords = allRecords.concat(records);
  }

  console.log('  AdvisorHub total: ' + allRecords.length);
  return allRecords;
}

// ============================================================
// 4. INVESTMENTNEWS
// ============================================================

function parseInvestmentNewsHNW(html) {
  const detailsPattern = /<details[^>]*>[\s\S]*?<\/details>/g;
  let records = [];
  let dm;
  while ((dm = detailsPattern.exec(html)) !== null) {
    const section = dm[0];
    const summaryMatch = section.match(/<summary[^>]*>([\s\S]*?)<\/summary>/);
    const region = summaryMatch ? summaryMatch[1].replace(/<[^>]+>/g, '').trim() : '';
    const liPattern = /<li[^>]*>([\s\S]*?)<\/li>/g;
    let li;
    while ((li = liPattern.exec(section)) !== null) {
      let firm = li[1].replace(/<[^>]+>/g, '').replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&ndash;/g, '-').trim();
      if (firm) {
        records.push({
          name: firm, teamName: '', firm: firm,
          state: '', city: '', rank: '', category: region,
          teamAssets: '', minAccount: '', typicalNetWorth: '', typicalHousehold: '',
          clientTypes: '',
          publication: 'InvestmentNews',
          list: 'Top Independent HNW Advisors ' + PREV_YEAR
        });
      }
    }
  }
  return records;
}

async function fetchInvestmentNews() {
  console.log('\n=== INVESTMENTNEWS ===');
  let allRecords = [];

  // Try fetching HNW page
  const cacheFile = 'investmentnews_hnw.html';
  process.stdout.write('  Fetching HNW Advisors page... ');

  let html = curlFetch(INVESTMENTNEWS_HNW_URL);
  if (!html || html.length < 1000) {
    // Try alternate URL patterns
    const altUrls = [
      `https://www.investmentnews.com/hnw-advisors/`,
      `https://www.investmentnews.com/awards/hnw-advisors-${PREV_YEAR}/`,
      `https://www.investmentnews.com/best-practices/hnw-advisors-${PREV_YEAR}/`,
    ];
    for (const altUrl of altUrls) {
      html = curlFetch(altUrl);
      if (html && html.length > 1000) {
        console.log('found at ' + altUrl);
        break;
      }
    }
    if (!html || html.length < 1000) {
      console.log('FAILED — trying cache');
      html = cacheRead(cacheFile);
    }
  }

  if (html && html.length > 1000) {
    cacheWrite(cacheFile, html);
    const hnwRecords = parseInvestmentNewsHNW(html);
    console.log(hnwRecords.length + ' HNW firms');
    allRecords = allRecords.concat(hnwRecords);
  } else {
    console.log('No HNW data available');
  }

  // Try fetching 5-Star Independent page
  const fiveStarUrls = [
    `https://www.investmentnews.com/five-star-independent-advisors-${PREV_YEAR}/`,
    `https://www.investmentnews.com/awards/five-star-independent-advisors-${PREV_YEAR}/`,
    `https://www.investmentnews.com/five-star-independent-financial-advisors-${PREV_YEAR}/`,
  ];

  process.stdout.write('  Fetching 5-Star Independent page... ');
  let fiveStarHtml = null;
  for (const url of fiveStarUrls) {
    fiveStarHtml = curlFetch(url);
    if (fiveStarHtml && fiveStarHtml.length > 2000) break;
    fiveStarHtml = null;
  }

  if (fiveStarHtml) {
    // Try to extract names from structured content (varies by year)
    // Look for list items, table rows, or bold names in paragraphs
    const namePatterns = [
      // <li> tags with names
      /<li[^>]*>([A-Z][a-z]+ (?:[A-Z]\. )?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)<\/li>/g,
      // <strong> or <b> tags with names
      /<(?:strong|b)>([A-Z][a-z]+ (?:[A-Z]\. )?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)<\/(?:strong|b)>/g,
    ];
    let fiveStarNames = new Set();
    namePatterns.forEach(function(pattern) {
      let nm;
      while ((nm = pattern.exec(fiveStarHtml)) !== null) {
        const name = nm[1].trim();
        if (name.length > 5 && name.length < 50) fiveStarNames.add(name);
      }
    });

    console.log(fiveStarNames.size + ' names extracted');
    fiveStarNames.forEach(function(name) {
      allRecords.push({
        name: name, teamName: '', firm: '',
        state: '', city: '', rank: '', category: '',
        teamAssets: '', minAccount: '', typicalNetWorth: '', typicalHousehold: '',
        clientTypes: '',
        publication: 'InvestmentNews',
        list: '5-Star Independent Advisors ' + PREV_YEAR
      });
    });
    cacheWrite('investmentnews_fivestar.html', fiveStarHtml);
  } else {
    console.log('not available');
  }

  console.log('  InvestmentNews total: ' + allRecords.length);
  return allRecords;
}

// ============================================================
// COMBINE & OUTPUT
// ============================================================

function buildOutput(allRecords, previousNames) {
  console.log('\n=== YEAR-OVER-YEAR COMPARISON ===');

  // Flag new entrants: names in current data that weren't in previous data for the same publication
  let newCount = 0;
  allRecords.forEach(function(r) {
    const prevSet = previousNames[r.publication];
    if (!prevSet || prevSet.size === 0) {
      // No previous data for this publication — don't mark anyone as new
      r.isNew = 0;
    } else {
      const nameKey = r.name.toLowerCase().trim();
      r.isNew = prevSet.has(nameKey) ? 0 : 1;
      if (r.isNew) newCount++;
    }
  });
  console.log('  New entrants: ' + newCount + ' (across all publications)');
  Object.keys(previousNames).forEach(function(pub) {
    const prevCount = previousNames[pub].size;
    const curCount = allRecords.filter(function(r) { return r.publication === pub; }).length;
    const pubNew = allRecords.filter(function(r) { return r.publication === pub && r.isNew; }).length;
    console.log('    ' + pub + ': ' + curCount + ' current, ' + prevCount + ' previous, ' + pubNew + ' new');
  });

  console.log('\n=== BUILDING OUTPUT ===');

  // Build compact indexed format
  const publications = [...new Set(allRecords.map(function(r) { return r.publication; }))].sort();
  const lists = [...new Set(allRecords.map(function(r) { return r.list; }))].sort();
  const firms = [...new Set(allRecords.map(function(r) { return r.firm; }))].sort();
  const states = [...new Set(allRecords.map(function(r) { return r.state; }))].sort();
  const cities = [...new Set(allRecords.map(function(r) { return r.city; }))].sort();

  // Row format: [name, teamName, firmIdx, stateIdx, cityIdx, rank, category, teamAssets, minAccount, typicalNetWorth, typicalHousehold, pubIdx, listIdx, clientTypes, isNew]
  const rows = allRecords.map(function(r) {
    return [
      r.name,
      r.teamName || '',
      firms.indexOf(r.firm),
      states.indexOf(r.state),
      cities.indexOf(r.city),
      r.rank,
      r.category || '',
      r.teamAssets || '',
      r.minAccount || '',
      r.typicalNetWorth || '',
      r.typicalHousehold || '',
      publications.indexOf(r.publication),
      lists.indexOf(r.list),
      r.clientTypes || '',
      r.isNew || 0
    ];
  });

  const compactData = { P: publications, L: lists, F: firms, S: states, C: cities, R: rows };
  const jsContent = 'const RAW_DATA=' + JSON.stringify(compactData) + ';';

  console.log('  Publications: ' + publications.length + ' — ' + publications.join(', '));
  console.log('  Lists: ' + lists.length);
  console.log('  Firms: ' + firms.length);
  console.log('  States: ' + states.length);
  console.log('  Cities: ' + cities.length);
  console.log('  Total records: ' + rows.length);
  console.log('  New entrants: ' + newCount);
  console.log('  File size: ' + (jsContent.length / 1024 / 1024).toFixed(2) + ' MB');

  if (!DRY_RUN) {
    // Write main data file (always "advisor-data.js" for the current/latest)
    const outPath = path.join(OUT_DIR, 'advisor-data.js');
    fs.writeFileSync(outPath, jsContent);
    console.log('  Saved to: ' + outPath);

    // Archive a year-versioned copy in data/ directory
    const dataDir = path.join(OUT_DIR, 'data');
    ensureDir(dataDir);
    const archivePath = path.join(dataDir, YEAR + '.js');
    fs.writeFileSync(archivePath, jsContent);
    console.log('  Archived to: ' + archivePath);

    // Update years manifest (lists all available year files)
    const existingYears = fs.readdirSync(dataDir)
      .filter(function(f) { return f.match(/^\d{4}\.js$/); })
      .map(function(f) { return parseInt(f.replace('.js', '')); })
      .sort(function(a, b) { return b - a; }); // newest first
    const manifest = { years: existingYears, current: parseInt(YEAR) };
    fs.writeFileSync(path.join(dataDir, 'years.json'), JSON.stringify(manifest, null, 2));
    console.log('  Years manifest: ' + existingYears.join(', '));
  } else {
    console.log('  [DRY RUN] Would save to: ' + path.join(OUT_DIR, 'advisor-data.js'));
  }

  return { publications: publications.length, lists: lists.length, records: rows.length, newEntrants: newCount };
}

// ============================================================
// MAIN
// ============================================================

async function main() {
  console.log('Advisor Recognition Intelligence — Data Update');
  console.log('Target year: ' + YEAR + ' (data period: ' + PREV_YEAR + ')');
  console.log('Fetching: ' + [
    FETCH_FORBES && 'Forbes', FETCH_BARRONS && "Barron's",
    FETCH_ADVISORHUB && 'AdvisorHub', FETCH_INVESTMENTNEWS && 'InvestmentNews'
  ].filter(Boolean).join(', '));
  if (DRY_RUN) console.log('*** DRY RUN — will not write output ***');

  let allRecords = [];

  // Load existing data as fallback (used when a source is not being refreshed,
  // or when a fresh fetch returns 0 records to prevent data loss)
  // Also used for year-over-year comparison to identify new entrants
  const existingPath = path.join(OUT_DIR, 'advisor-data.js');
  let existingData = null;
  let previousNames = {}; // { publication: Set(lowercase names) }
  if (fs.existsSync(existingPath)) {
    try {
      const js = fs.readFileSync(existingPath, 'utf8');
      const jsonStr = js.replace(/^const RAW_DATA=/, '').replace(/;$/, '');
      existingData = JSON.parse(jsonStr);
      console.log('\nLoaded existing data: ' + existingData.R.length + ' records');

      // Extract names per publication for YoY comparison
      existingData.R.forEach(function(r) {
        const pub = existingData.P[r[11]] || '';
        if (!pub) return;
        if (!previousNames[pub]) previousNames[pub] = new Set();
        previousNames[pub].add((r[0] || '').toLowerCase().trim());
      });
      const prevPubs = Object.keys(previousNames);
      console.log('  Previous names loaded for: ' + prevPubs.map(function(p) { return p + ' (' + previousNames[p].size + ')'; }).join(', '));
    } catch(e) {
      console.warn('Could not load existing data: ' + e.message);
    }
  }

  // Helper: get existing records for a specific publication
  function getExistingRecords(pubName) {
    if (!existingData) return [];
    const pubIdx = existingData.P.indexOf(pubName);
    if (pubIdx === -1) return [];
    return existingData.R
      .filter(function(r) { return r[11] === pubIdx; })
      .map(function(r) {
        return {
          name: r[0], teamName: r[1],
          firm: existingData.F[r[2]] || '', state: existingData.S[r[3]] || '', city: existingData.C[r[4]] || '',
          rank: r[5], category: r[6], teamAssets: r[7], minAccount: r[8],
          typicalNetWorth: r[9], typicalHousehold: r[10],
          publication: pubName, list: existingData.L[r[12]] || '', clientTypes: r[13] || ''
        };
      });
  }

  // Fetch each publication (or use existing data as fallback)
  // Helper: fetch a publication, falling back to existing data on failure or 0 results
  async function fetchWithFallback(pubName, shouldFetch, fetchFn) {
    if (shouldFetch) {
      try {
        const records = await fetchFn();
        if (records.length > 0) {
          allRecords = allRecords.concat(records);
          return;
        }
        // 0 records — fall back to existing data to prevent data loss
        const existing = getExistingRecords(pubName);
        if (existing.length > 0) {
          console.log('  ' + pubName + ': fetch returned 0 records, using ' + existing.length + ' existing records');
          allRecords = allRecords.concat(existing);
        }
      } catch(e) {
        console.error('  ' + pubName + ' fetch failed: ' + e.message);
        allRecords = allRecords.concat(getExistingRecords(pubName));
      }
    } else {
      allRecords = allRecords.concat(getExistingRecords(pubName));
    }
  }

  await fetchWithFallback('Forbes/SHOOK', FETCH_FORBES, fetchForbes);
  await fetchWithFallback("Barron's", FETCH_BARRONS, fetchBarrons);
  await fetchWithFallback('AdvisorHub', FETCH_ADVISORHUB, fetchAdvisorHub);
  await fetchWithFallback('InvestmentNews', FETCH_INVESTMENTNEWS, fetchInvestmentNews);

  if (allRecords.length === 0) {
    console.log('\nNo records found. Check if the target year URLs are published yet.');
    process.exit(1);
  }

  const result = buildOutput(allRecords, previousNames);

  console.log('\n=== SUMMARY ===');
  console.log('Publications: ' + result.publications);
  console.log('Lists: ' + result.lists);
  console.log('Total records: ' + result.records);
  console.log('New entrants: ' + result.newEntrants);
  console.log('Done!');
}

main().catch(function(e) {
  console.error('\nFATAL: ' + e.message);
  console.error(e.stack);
  process.exit(1);
});
