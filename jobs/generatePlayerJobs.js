/**
 * Generate player scrape jobs: fetch all player URLs from Basketball Reference index
 * and insert into player_scrape_jobs with status 'pending'.
 * Uses player_url or url column depending on existing schema.
 */

import 'dotenv/config';
import { pool } from '../db/db.js';
import { fetchPlayerUrlsFromIndex } from '../scrapers/playerIndexScraper.js';

async function generateJobs() {
  console.log('Fetching G League player index from Basketball Reference...');
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
  for (const url of urls) {
    try {
      await pool.query(
        `INSERT INTO player_scrape_jobs (${urlColumn}, status) VALUES ($1, 'pending')`,
        [url]
      );
      inserted++;
    } catch (err) {
      if (err.code === '23505') {
        skipped++;
      } else if (err.code === '42703') {
        await pool.end();
        throw new Error(`player_scrape_jobs table must have a ${urlColumn} column.`);
      } else {
        throw err;
      }
    }
  }

  console.log(`Jobs: ${inserted} new, ${skipped} already existed.`);
  await pool.end();
}

generateJobs().catch((err) => {
  console.error(err);
  process.exit(1);
});
