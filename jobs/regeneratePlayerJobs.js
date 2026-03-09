/**
 * Clear the job queue and regenerate all player scrape jobs from Basketball Reference.
 * Use this after deploying the index fix to enqueue ~5,400 players (full NBA/ABA history).
 *
 * Usage: node jobs/regeneratePlayerJobs.js
 *   (requires DATABASE_URL; run once after deploy or when rosters are incomplete)
 */

import 'dotenv/config';
import { pool } from '../db/db.js';
import { fetchPlayerUrlsFromIndex } from '../scrapers/playerIndexScraper.js';

const BATCH_SIZE = 300;

async function regenerateJobs() {
  console.log('Regenerating player scrape jobs (clear + full index)...');

  let urlColumn = 'url';
  try {
    await pool.query('SELECT id, player_url FROM player_scrape_jobs LIMIT 1');
    urlColumn = 'player_url';
  } catch (e) {
    if (e.code === '42703') {
      await pool.query('SELECT id, url FROM player_scrape_jobs LIMIT 1');
    } else if (e.code === '42P01') {
      await pool.end();
      throw new Error('player_scrape_jobs table does not exist. Run migrate first.');
    } else {
      throw e;
    }
  }

  console.log('Clearing existing jobs...');
  await pool.query('TRUNCATE TABLE player_scrape_jobs RESTART IDENTITY CASCADE');
  console.log('Fetching full player index from Basketball Reference (~26 letter pages)...');
  const urls = await fetchPlayerUrlsFromIndex();
  console.log(`Found ${urls.length} player URLs. Inserting in batches of ${BATCH_SIZE}...`);

  let inserted = 0;
  for (let i = 0; i < urls.length; i += BATCH_SIZE) {
    const batch = urls.slice(i, i + BATCH_SIZE);
    const values = batch.map((_, j) => `($${j + 1}, 'pending')`).join(', ');
    await pool.query(
      `INSERT INTO player_scrape_jobs (${urlColumn}, status) VALUES ${values}`,
      batch
    );
    inserted += batch.length;
    if ((i + BATCH_SIZE) % 1500 === 0 || i + BATCH_SIZE >= urls.length) {
      console.log(`  inserted ${Math.min(i + BATCH_SIZE, urls.length)} / ${urls.length}`);
    }
  }

  console.log(`Done. ${inserted} jobs enqueued. Start workers to scrape (npm start or npm run workers).`);
  await pool.end();
}

regenerateJobs().catch((err) => {
  console.error(err);
  process.exit(1);
});
