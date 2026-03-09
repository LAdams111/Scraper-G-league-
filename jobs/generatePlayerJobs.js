/**
 * Generate player scrape jobs: fetch all player URLs from Basketball Reference index
 * and insert into player_scrape_jobs with status 'pending'.
 * Uses player_url or url column depending on existing schema.
 * For a full reset (clear + refill ~5.4k jobs), use: npm run regenerate-jobs
 */

import 'dotenv/config';
import { pool } from '../db/db.js';
import { fetchPlayerUrlsFromIndex } from '../scrapers/playerIndexScraper.js';

const BATCH_SIZE = 300;

async function generateJobs() {
  console.log('Fetching player index from Basketball Reference...');
  const urls = await fetchPlayerUrlsFromIndex();
  console.log(`Found ${urls.length} player URLs.`);

  let urlColumn = null;
  try {
    await pool.query('SELECT id, player_url FROM player_scrape_jobs LIMIT 1');
    urlColumn = 'player_url';
    console.log('Using column: player_url');
  } catch (err) {
    if (err.code === '42703') {
      try {
        await pool.query('SELECT id, url FROM player_scrape_jobs LIMIT 1');
        urlColumn = 'url';
        console.log('Using column: url');
      } catch (e) {
        await pool.end();
        throw new Error('player_scrape_jobs must have a player_url or url column.');
      }
    } else {
      throw err;
    }
  }

  let inserted = 0;
  let skipped = 0;
  for (let i = 0; i < urls.length; i += BATCH_SIZE) {
    const batch = urls.slice(i, i + BATCH_SIZE);
    const values = batch.map((_, j) => `($${j + 1}, 'pending')`).join(', ');
    try {
      const res = await pool.query(
        `INSERT INTO player_scrape_jobs (${urlColumn}, status) VALUES ${values}
         ON CONFLICT (${urlColumn}) DO NOTHING`,
        batch
      );
      inserted += res.rowCount ?? batch.length;
    } catch (err) {
      if (err.code === '42703') {
        await pool.end();
        throw new Error(`player_scrape_jobs table must have a ${urlColumn} column.`);
      }
      throw err;
    }
  }
  skipped = urls.length - inserted;

  console.log(`Jobs: ${inserted} new, ${skipped} already existed.`);
  await pool.end();
}

generateJobs().catch((err) => {
  console.error(err);
  process.exit(1);
});
