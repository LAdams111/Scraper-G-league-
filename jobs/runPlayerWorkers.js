/**
 * Worker: claim a pending job, scrape player, persist, mark complete/failed.
 * Run with: npm start (Railway) or npm run workers.
 * Uses existing schema: player_scrape_jobs (id, player_url, status, attempts, last_error, created_at, updated_at).
 */

import 'dotenv/config';
import { pool, testConnection } from '../db/db.js';
import { scrapeAndPersistPlayer } from '../scrapers/playerSeasonScraper.js';

const MAX_RETRIES = 3;
const POLL_MS = 3000;

let jobsTableValid = true;

async function checkJobsTableSchema() {
  try {
    await pool.query('SELECT id, player_url FROM player_scrape_jobs LIMIT 1');
    return true;
  } catch (err) {
    if (err.code === '42703') {
      console.error(
        "Database table player_scrape_jobs is missing the 'player_url' column. " +
        "Ensure the table has: id, player_url, status, attempts, last_error, created_at, updated_at."
      );
      return false;
    }
    if (err.code === '42P01') {
      console.error(
        "Database table player_scrape_jobs does not exist."
      );
      return false;
    }
    throw err;
  }
}

async function claimJob() {
  if (!jobsTableValid) return null;
  const client = await pool.connect();
  try {
    const selectRes = await client.query(
      `SELECT id, player_url
       FROM player_scrape_jobs
       WHERE status = 'pending'
       ORDER BY id
       LIMIT 1
       FOR UPDATE SKIP LOCKED`
    );
    const row = selectRes.rows[0];
    if (!row) return null;
    await client.query(
      `UPDATE player_scrape_jobs
       SET status = 'processing', updated_at = NOW()
       WHERE id = $1`,
      [row.id]
    );
    return row;
  } finally {
    client.release();
  }
}

async function markComplete(jobId) {
  await pool.query(
    `UPDATE player_scrape_jobs SET status = 'complete', updated_at = NOW() WHERE id = $1`,
    [jobId]
  );
}

async function markFailed(jobId, errorMessage) {
  await pool.query(
    `UPDATE player_scrape_jobs
     SET status = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END,
         attempts = attempts + 1,
         last_error = $3,
         updated_at = NOW()
     WHERE id = $1`,
    [jobId, MAX_RETRIES, errorMessage]
  );
}

async function processOneJob() {
  const job = await claimJob();
  if (!job) return false;

  const { id: jobId, player_url } = job;
  console.log(`[Worker ${process.pid}] Job ${jobId}: ${player_url}`);

  try {
    const result = await scrapeAndPersistPlayer(player_url);
    if (result.ok) {
      console.log(`[Worker ${process.pid}] Job ${jobId} complete: ${result.sr_player_id} (${result.seasons_count} seasons)`);
      await markComplete(jobId);
    } else if (result.reason === 'not_found') {
      console.log(`[Worker ${process.pid}] Job ${jobId} skipped (missing page): ${player_url}`);
      await markComplete(jobId);
    } else {
      await markFailed(jobId, result.reason || 'scrape returned not ok');
    }
  } catch (err) {
    console.error(`[Worker ${process.pid}] Job ${jobId} error:`, err.message);
    if (err.response && err.response.status === 404) {
      console.log(`[Worker ${process.pid}] Job ${jobId} skipped (404): ${player_url}`);
      await markComplete(jobId);
    } else {
      await markFailed(jobId, err.message || String(err));
    }
  }
  return true;
}

async function runWorker() {
  console.log('Starting NBA scraper workers...');
  await testConnection();
  console.log('Worker pool initialized');
  jobsTableValid = await checkJobsTableSchema();
  if (!jobsTableValid) {
    console.log('Worker will stay up. Fix the schema and restart to process jobs.');
    while (true) {
      await new Promise((r) => setTimeout(r, 60000));
      console.log('Waiting for player_scrape_jobs table (player_url column).');
    }
  }
  console.log(`[Worker ${process.pid}] Started.`);
  while (true) {
    const hadJob = await processOneJob();
    if (!hadJob) {
      await new Promise((r) => setTimeout(r, POLL_MS));
    }
  }
}

runWorker().catch((err) => {
  console.error(err);
  process.exit(1);
});
