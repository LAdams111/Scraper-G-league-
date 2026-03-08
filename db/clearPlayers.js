/**
 * Clear all player-related data for a clean slate.
 * Deletes in order: player_season_stats → player_seasons → player_external_ids → players.
 * Optionally clears player_scrape_jobs so you can re-run generate-jobs.
 *
 * Usage: node db/clearPlayers.js [--keep-jobs]
 *   --keep-jobs  Do not clear player_scrape_jobs (default: clear jobs too)
 */

import 'dotenv/config';
import { pool, testConnection } from './db.js';

async function clearPlayers(keepJobs = false) {
  await testConnection();

  const client = await pool.connect();
  try {
    console.log('Truncating player data (player_season_stats, player_seasons, player_external_ids, players)...');
    await client.query(`
      TRUNCATE TABLE
        player_season_stats,
        player_seasons,
        player_external_ids,
        players
      RESTART IDENTITY CASCADE
    `);
    console.log('Player data cleared.');

    if (!keepJobs) {
      console.log('Truncating player_scrape_jobs...');
      await client.query('TRUNCATE TABLE player_scrape_jobs RESTART IDENTITY CASCADE');
      console.log('Job queue cleared. Run generate-jobs to enqueue players again.');
    } else {
      console.log('Kept player_scrape_jobs (--keep-jobs).');
      const reset = await client.query(
        `UPDATE player_scrape_jobs SET status = 'pending' WHERE status IN ('complete', 'processing') RETURNING id`
      );
      console.log(`Reset ${reset.rowCount} jobs to pending (ready to rescrape).`);
    }
  } finally {
    client.release();
    await pool.end();
  }
}

const keepJobs = process.argv.includes('--keep-jobs');
clearPlayers(keepJobs).catch((err) => {
  console.error(err);
  process.exit(1);
});
