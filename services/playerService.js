import { query } from '../db/db.js';

const SOURCE_BR = 'basketball_reference';
const SOURCE_BR_GLEAGUE = 'basketball_reference_gleague';

/**
 * Get player id by Basketball Reference sr_player_id (players.sr_player_id). Returns null if not found.
 */
export async function getPlayerBySrId(srPlayerId) {
  try {
    const r = await query(
      'SELECT id FROM players WHERE sr_player_id = $1',
      [srPlayerId]
    );
    return r.rows.length > 0 ? r.rows[0].id : null;
  } catch (err) {
    console.error('[playerService] getPlayerBySrId error:', err.message);
    throw err;
  }
}

/**
 * Get player id by external id and source (e.g. basketball_reference_gleague, jamesle01d).
 */
export async function getPlayerIdByExternalId(source, externalId) {
  if (!source || !externalId) return null;
  try {
    const r = await query(
      'SELECT player_id FROM player_external_ids WHERE source = $1 AND external_id = $2',
      [source, externalId]
    );
    return r.rows.length > 0 ? r.rows[0].player_id : null;
  } catch (err) {
    console.error('[playerService] getPlayerIdByExternalId error:', err.message);
    throw err;
  }
}

/**
 * Find a single player by full_name and birth_date (for matching G League to existing NBA player).
 * Returns player_id if exactly one match; null if none or multiple (ambiguous).
 */
export async function findPlayerByNameAndBirthDate(fullName, birthDate) {
  if (!fullName || !birthDate) return null;
  try {
    const r = await query(
      `SELECT id FROM players
       WHERE TRIM(full_name) = TRIM($1) AND birth_date = $2`,
      [fullName, birthDate]
    );
    return r.rows.length === 1 ? r.rows[0].id : null;
  } catch (err) {
    console.error('[playerService] findPlayerByNameAndBirthDate error:', err.message);
    throw err;
  }
}

/**
 * Try to find an existing player that represents the same person as this G League profile.
 * 1) Already have this G League id in external_ids
 * 2) This G League id is the main sr_player_id (G-League-only player already in DB)
 * 3) G League id ends with 'd' and stripping it gives an existing NBA sr_player_id
 * 4) Match by full_name + birth_date (exactly one)
 */
export async function findExistingPlayerForGLeague(gleagueSrId, profile) {
  if (!gleagueSrId) return null;

  const byGLeagueExternal = await getPlayerIdByExternalId(SOURCE_BR_GLEAGUE, gleagueSrId);
  if (byGLeagueExternal) return byGLeagueExternal;

  const bySrId = await getPlayerBySrId(gleagueSrId);
  if (bySrId) return bySrId;

  const nbaId = gleagueSrId.endsWith('d') ? gleagueSrId.slice(0, -1) : null;
  if (nbaId) {
    const byNbaId = await getPlayerBySrId(nbaId);
    if (byNbaId) return byNbaId;
  }

  if (profile && profile.full_name && profile.birth_date) {
    const byNameDob = await findPlayerByNameAndBirthDate(profile.full_name, profile.birth_date);
    if (byNameDob) return byNameDob;
  }

  return null;
}

/**
 * Insert player into players table. Does not use ON CONFLICT so it works
 * whether or not there is a UNIQUE constraint on sr_player_id.
 * Checks for existing player first to avoid duplicates.
 */
export async function insertPlayer(data) {
  const {
    full_name,
    first_name,
    last_name,
    birth_date,
    birth_place,
    height_cm,
    weight_kg,
    position,
    nationality,
    sr_player_id,
  } = data;

  if (!sr_player_id) {
    console.error('[playerService] insertPlayer: sr_player_id is required');
    return null;
  }

  try {
    const existing = await query('SELECT id FROM players WHERE sr_player_id = $1', [sr_player_id]);
    if (existing.rows.length > 0) {
      const playerId = existing.rows[0].id;
      await query(
        `UPDATE players SET
          full_name = COALESCE($2, full_name), first_name = COALESCE($3, first_name), last_name = COALESCE($4, last_name),
          birth_date = COALESCE($5, birth_date), birth_place = COALESCE($6, birth_place),
          height_cm = COALESCE($7, height_cm), weight_kg = COALESCE($8, weight_kg),
          position = COALESCE($9, position), nationality = COALESCE($10, nationality)
         WHERE id = $1`,
        [
          playerId,
          full_name ?? null,
          first_name ?? null,
          last_name ?? null,
          birth_date ?? null,
          birth_place ?? null,
          height_cm ?? null,
          weight_kg ?? null,
          position ?? null,
          nationality ?? null,
        ]
      );
      await ensureExternalId(playerId, sr_player_id);
      return playerId;
    }

    const ins = await query(
      `INSERT INTO players (
        full_name, first_name, last_name, birth_date, birth_place,
        height_cm, weight_kg, position, nationality, sr_player_id
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING id`,
      [
        full_name ?? null,
        first_name ?? null,
        last_name ?? null,
        birth_date ?? null,
        birth_place ?? null,
        height_cm ?? null,
        weight_kg ?? null,
        position ?? null,
        nationality ?? null,
        sr_player_id,
      ]
    );
    const playerId = ins.rows[0].id;
    await ensureExternalId(playerId, sr_player_id);
    return playerId;
  } catch (err) {
    console.error('[playerService] insertPlayer error:', err.message);
    throw err;
  }
}

/**
 * Ensure player_external_ids row exists for a given source.
 * Uses check-then-insert/update so it works with or without a UNIQUE constraint on (player_id, source).
 */
async function ensureExternalIdForSource(playerId, externalId, source) {
  const r = await query(
    'SELECT id FROM player_external_ids WHERE player_id = $1 AND source = $2',
    [playerId, source]
  );
  if (r.rows.length > 0) {
    await query(
      'UPDATE player_external_ids SET external_id = $3 WHERE player_id = $1 AND source = $2',
      [playerId, source, externalId]
    );
  } else {
    await query(
      `INSERT INTO player_external_ids (player_id, source, external_id)
       VALUES ($1, $2, $3)`,
      [playerId, source, externalId]
    );
  }
}

/**
 * Ensure player_external_ids row exists. Uses SOURCE_BR (NBA).
 */
async function ensureExternalId(playerId, srPlayerId) {
  await ensureExternalIdForSource(playerId, srPlayerId, SOURCE_BR);
}

/**
 * Ensure external_id row exists for existing player (NBA source).
 */
export async function upsertExternalId(playerId, srPlayerId) {
  try {
    await ensureExternalId(playerId, srPlayerId);
  } catch (err) {
    console.error('[playerService] upsertExternalId error:', err.message);
    throw err;
  }
}

/**
 * Insert or merge a G League player. If we find an existing player (same person via
 * G League external id, NBA id from gleague id minus 'd', or name+birth_date), we
 * add the G League external id and return its id; we do not overwrite any existing
 * profile data (only add G League external id and later seasons/stats). Otherwise we
 * create a new player with sr_player_id = gleagueSrId and add G League external id.
 */
export async function insertOrMergeGLeaguePlayer(profile, gleagueSrId) {
  if (!gleagueSrId) {
    console.error('[playerService] insertOrMergeGLeaguePlayer: gleagueSrId is required');
    return null;
  }

  const existingId = await findExistingPlayerForGLeague(gleagueSrId, profile);
  if (existingId) {
    // Add G League external id only; do not update any existing profile fields
    await ensureExternalIdForSource(existingId, gleagueSrId, SOURCE_BR_GLEAGUE);
    return existingId;
  }

  const {
    full_name,
    first_name,
    last_name,
    birth_date,
    birth_place,
    height_cm,
    weight_kg,
    position,
    nationality,
  } = profile;

  const ins = await query(
    `INSERT INTO players (
      full_name, first_name, last_name, birth_date, birth_place,
      height_cm, weight_kg, position, nationality, sr_player_id
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    RETURNING id`,
    [
      full_name ?? null,
      first_name ?? null,
      last_name ?? null,
      birth_date ?? null,
      birth_place ?? null,
      height_cm ?? null,
      weight_kg ?? null,
      position ?? null,
      nationality ?? null,
      gleagueSrId,
    ]
  );
  const playerId = ins.rows[0].id;
  await ensureExternalIdForSource(playerId, gleagueSrId, SOURCE_BR_GLEAGUE);
  return playerId;
}

export default {
  getPlayerBySrId,
  getPlayerIdByExternalId,
  findPlayerByNameAndBirthDate,
  findExistingPlayerForGLeague,
  insertPlayer,
  insertOrMergeGLeaguePlayer,
  upsertExternalId,
};
