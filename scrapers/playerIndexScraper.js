/**
 * Scrape Basketball Reference player index to collect all player profile URLs.
 * Index: https://www.basketball-reference.com/players/
 * Then each letter: /players/a/, /players/b/, ... and /players/--/ for "other" names.
 * Follows pagination on each letter page when present (Next link).
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import { withRateLimit } from '../utils/rateLimiter.js';
import { retry } from '../utils/retry.js';

const BASE = 'https://www.basketball-reference.com';

/** Segment path for each letter (a-z). Note: /players/--/ returns 404 so we only use letters. */
const INDEX_SEGMENTS = 'abcdefghijklmnopqrstuvwxyz'.split('');

function extractPlayerUrlsFromHtml(html, base = BASE) {
  const $ = cheerio.load(html);
  const urls = new Set();
  // Target the main players table by id; fallback to any table with player links
  const $table = $('#players');
  const $links = ($table.length ? $table : $('table')).find('a[href*="/players/"][href$=".html"]');
  $links.each((_, el) => {
    const href = $(el).attr('href');
    if (href && href.includes('/players/') && href.endsWith('.html')) {
      const full = href.startsWith('http') ? href : `${base}${href.startsWith('/') ? '' : '/'}${href}`;
      urls.add(full);
    }
  });
  return urls;
}

/** Get next page URL from pagination .prevnext, or null if none */
function getNextPageUrl(html, currentUrl) {
  const $ = cheerio.load(html);
  const base = BASE;
  const $next = $('.prevnext a.button2.next');
  if (!$next.length) return null;
  const href = $next.attr('href');
  if (!href) return null;
  return href.startsWith('http') ? href : `${base}${href.startsWith('/') ? '' : '/'}${href}`;
}

export async function fetchPlayerUrlsFromIndex() {
  const allUrls = new Set();

  for (const segment of INDEX_SEGMENTS) {
    let url = `${BASE}/players/${segment}/`;

    while (url) {
      let nextUrl = null;
      await withRateLimit(async () => {
        const html = await retry(async () => {
          const res = await axios.get(url, {
            timeout: 20000,
            headers: {
              'User-Agent': 'Mozilla/5.0 (compatible; NBA-Scraper/1.0)',
              'Accept': 'text/html',
            },
            validateStatus: (s) => s === 200 || s === 429,
          });
          if (res.status === 429) {
            const err = new Error('Rate limited (429)');
            err.response = res;
            throw err;
          }
          return res.data;
        });
        const pageUrls = extractPlayerUrlsFromHtml(html);
        pageUrls.forEach((u) => allUrls.add(u));
        nextUrl = getNextPageUrl(html, url);
      });
      url = nextUrl;
    }
  }

  return Array.from(allUrls);
}

/**
 * Extract sr_player_id from URL like .../players/j/jamesle01.html or .../players/--/xyz01.html
 */
export function srPlayerIdFromUrl(url) {
  const match = url.match(/\/players\/[^/]+\/([a-z0-9]+)\.html$/i);
  return match ? match[1].toLowerCase() : null;
}

export default { fetchPlayerUrlsFromIndex, srPlayerIdFromUrl };
