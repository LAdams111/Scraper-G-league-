/**
 * Clear this league's job queue and regenerate player scrape jobs from Basketball Reference.
 * Only affects the league set by SCRAPER_LEAGUE (nba or gleague). WNBA jobs are never touched.
 * Use after deploy or when rosters are incomplete for NBA/G League only.
 *
 * Usage: SCRAPER_LEAGUE=nba node jobs/regeneratePlayerJobs.js
 */

import 'dotenv/config';
import { pool } from '../db/db.js';
import { fetchPlayerUrlsFromIndex } from '../scrapers/playerIndexScraper.js';

const BATCH_SIZE = 300;
const VALID_LEAGUES = ['nba', 'gleague'];

function getScraperLeague() {
  const raw = (process.env.SCRAPER_LEAGUE || 'nba').toLowerCase().trim();
  if (VALID_LEAGUES.includes(raw)) return raw;
  console.error(`Invalid SCRAPER_LEAGUE. Must be one of: ${VALID_LEAGUES.join(', ')}. (WNBA is a separate scraper.)`);
  process.exit(1);
}

async function regenerateJobs() {
  const league = getScraperLeague();
  console.log(`Regenerating player scrape jobs for league=${league} only (WNBA jobs are not touched)...`);

  let urlColumn = 'url';
  let hasLeagueColumn = false;
  try {
    await pool.query('SELECT id, player_url, league FROM player_scrape_jobs LIMIT 1');
    urlColumn = 'player_url';
    hasLeagueColumn = true;
  } catch (e) {
    if (e.code === '42703') {
      try {
        await pool.query('SELECT id, url, league FROM player_scrape_jobs LIMIT 1');
        hasLeagueColumn = true;
      } catch (e2) {
        if (e2.code === '42703') {
          await pool.query('SELECT id, url FROM player_scrape_jobs LIMIT 1');
        } else if (e2.code === '42P01') {
          await pool.end();
          throw new Error('player_scrape_jobs table does not exist. Run migrate first.');
        } else {
          throw e2;
        }
      }
    } else if (e.code === '42P01') {
      await pool.end();
      throw new Error('player_scrape_jobs table does not exist. Run migrate first.');
    } else {
      throw e;
    }
  }

  if (hasLeagueColumn) {
    const del = await pool.query('DELETE FROM player_scrape_jobs WHERE league = $1', [league]);
    console.log(`Cleared ${del.rowCount} existing jobs for league=${league}.`);
  } else {
    console.log('Clearing all jobs (no league column)...');
    await pool.query('TRUNCATE TABLE player_scrape_jobs RESTART IDENTITY CASCADE');
  }

  console.log('Fetching full player index from Basketball Reference (~26 letter pages)...');
  const urls = await fetchPlayerUrlsFromIndex();
  console.log(`Found ${urls.length} player URLs. Inserting in batches of ${BATCH_SIZE}...`);

  let inserted = 0;
  for (let i = 0; i < urls.length; i += BATCH_SIZE) {
    const batch = urls.slice(i, i + BATCH_SIZE);
    if (hasLeagueColumn) {
      const values = batch.map((_, j) => `($${j + 1}, $${batch.length + 1}, 'pending')`).join(', ');
      const params = [...batch, league];
      await pool.query(
        `INSERT INTO player_scrape_jobs (${urlColumn}, league, status) VALUES ${values}`,
        params
      );
    } else {
      const values = batch.map((_, j) => `($${j + 1}, 'pending')`).join(', ');
      await pool.query(
        `INSERT INTO player_scrape_jobs (${urlColumn}, status) VALUES ${values}`,
        batch
      );
    }
    inserted += batch.length;
    if ((i + BATCH_SIZE) % 1500 === 0 || i + BATCH_SIZE >= urls.length) {
      console.log(`  inserted ${Math.min(i + BATCH_SIZE, urls.length)} / ${urls.length}`);
    }
  }

  console.log(`Done. ${inserted} jobs enqueued for league=${league}. Start workers to scrape (npm start or npm run workers).`);
  await pool.end();
}

regenerateJobs().catch((err) => {
  console.error(err);
  process.exit(1);
});
