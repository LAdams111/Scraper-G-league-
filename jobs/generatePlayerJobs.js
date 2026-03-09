/**
 * Generate player scrape jobs: fetch NBA (and optionally G League) player URLs from Basketball Reference
 * and insert into player_scrape_jobs with status 'pending' and league.
 * This scraper is for NBA/G League only; WNBA uses the same DB but a separate Scraper-WNBA service.
 * Set SCRAPER_LEAGUE=nba (default) or SCRAPER_LEAGUE=gleague.
 * For a full reset for one league, use: npm run regenerate-jobs
 */

import 'dotenv/config';
import { pool } from '../db/db.js';
import { fetchPlayerUrlsFromIndex } from '../scrapers/playerIndexScraper.js';

const BATCH_SIZE = 300;
const VALID_LEAGUES = ['nba', 'gleague'];

function getScraperLeague() {
  const raw = (process.env.SCRAPER_LEAGUE || 'nba').toLowerCase().trim();
  if (VALID_LEAGUES.includes(raw)) return raw;
  console.error(`Invalid SCRAPER_LEAGUE "${process.env.SCRAPER_LEAGUE}". Must be one of: ${VALID_LEAGUES.join(', ')}. (WNBA is handled by a separate scraper.)`);
  process.exit(1);
}

async function generateJobs() {
  const league = getScraperLeague();
  console.log(`Fetching player index from Basketball Reference (league=${league})...`);
  const urls = await fetchPlayerUrlsFromIndex();
  console.log(`Found ${urls.length} player URLs.`);

  let urlColumn = null;
  let hasLeagueColumn = false;
  try {
    await pool.query('SELECT id, player_url, league FROM player_scrape_jobs LIMIT 1');
    urlColumn = 'player_url';
    hasLeagueColumn = true;
    console.log('Using column: player_url, league');
  } catch (err) {
    if (err.code === '42703') {
      try {
        await pool.query('SELECT id, url FROM player_scrape_jobs LIMIT 1');
        urlColumn = 'url';
        console.log('Using column: url (no league column)');
      } catch (e) {
        if (e.code === '42703') {
          try {
            await pool.query('SELECT id, url, league FROM player_scrape_jobs LIMIT 1');
            urlColumn = 'url';
            hasLeagueColumn = true;
            console.log('Using column: url, league');
          } catch (e2) {
            await pool.end();
            throw new Error('player_scrape_jobs must have a player_url or url column.');
          }
        } else {
          await pool.end();
          throw e;
        }
      }
    } else {
      throw err;
    }
  }

  let inserted = 0;
  for (let i = 0; i < urls.length; i += BATCH_SIZE) {
    const batch = urls.slice(i, i + BATCH_SIZE);
    try {
      if (hasLeagueColumn) {
        const values = batch.map((_, j) => `($${j + 1}, $${batch.length + 1}, 'pending')`).join(', ');
        const params = [...batch, league];
        const res = await pool.query(
          `INSERT INTO player_scrape_jobs (${urlColumn}, league, status) VALUES ${values}
           ON CONFLICT (${urlColumn}) DO NOTHING`,
          params
        );
        inserted += res.rowCount ?? 0;
      } else {
        const values = batch.map((_, j) => `($${j + 1}, 'pending')`).join(', ');
        const res = await pool.query(
          `INSERT INTO player_scrape_jobs (${urlColumn}, status) VALUES ${values}
           ON CONFLICT (${urlColumn}) DO NOTHING`,
          batch
        );
        inserted += res.rowCount ?? batch.length;
      }
    } catch (err) {
      if (err.code === '42703') {
        await pool.end();
        throw new Error(`player_scrape_jobs table must have ${urlColumn}${hasLeagueColumn ? ' and league' : ''}.`);
      }
      throw err;
    }
  }
  const skipped = urls.length - inserted;
  console.log(`Jobs: ${inserted} new, ${skipped} already existed (league=${league}).`);
  await pool.end();
}

generateJobs().catch((err) => {
  console.error(err);
  process.exit(1);
});
