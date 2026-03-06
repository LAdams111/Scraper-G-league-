/**
 * Generate player scrape jobs: fetch all player URLs from Basketball Reference index
 * and insert into player_scrape_jobs with status 'pending'.
 * Uses existing schema: player_url, status, etc.
 * If player_url has a UNIQUE constraint, duplicate URLs will be skipped (23505).
 */

import 'dotenv/config';
import { pool } from '../db/db.js';
import { fetchPlayerUrlsFromIndex } from '../scrapers/playerIndexScraper.js';

async function generateJobs() {
  console.log('Fetching player index from Basketball Reference...');
  const urls = await fetchPlayerUrlsFromIndex();
  console.log(`Found ${urls.length} player URLs.`);

  let inserted = 0;
  let skipped = 0;
  for (const url of urls) {
    try {
      await pool.query(
        `INSERT INTO player_scrape_jobs (player_url, status) VALUES ($1, 'pending')`,
        [url]
      );
      inserted++;
    } catch (err) {
      if (err.code === '23505') {
        skipped++;
      } else if (err.code === '42703') {
        await pool.end();
        throw new Error('player_scrape_jobs table must have a player_url column.');
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
