/**
 * G League team abbreviation <-> name map. No DB dependency so scrapers can use it without loading db.
 * Includes active and defunct franchises so historical jersey/team data resolves.
 */

export const TEAM_MAP = {
  ACC: { name: 'San Diego Clippers', city: 'San Diego' },
  AUS: { name: 'Austin Spurs', city: 'Austin' },
  BAK: { name: 'Motor City Cruise', city: 'Detroit' },
  BIR: { name: 'Birmingham Squadron', city: 'Birmingham' },
  CAN: { name: 'Cleveland Charge', city: 'Cleveland' },
  CCG: { name: 'Capital City Go-Go', city: 'Washington' },
  CPS: { name: 'College Park Skyhawks', city: 'College Park' },
  DEL: { name: 'Delaware Blue Coats', city: 'Delaware' },
  ERI: { name: 'Osceola Magic', city: 'Kissimmee' },
  FWN: { name: 'Noblesville Boom', city: 'Noblesville' },
  GBO: { name: 'Greensboro Swarm', city: 'Greensboro' },
  IDA: { name: 'Salt Lake City Stars', city: 'Salt Lake City' },
  SLC: { name: 'Salt Lake City Stars', city: 'Salt Lake City' },
  IWA: { name: 'Iowa Wolves', city: 'Des Moines' },
  LIN: { name: 'Long Island Nets', city: 'Long Island' },
  LOS: { name: 'South Bay Lakers', city: 'Los Angeles' },
  MAI: { name: 'Maine Celtics', city: 'Portland' },
  MHU: { name: 'Memphis Hustle', city: 'Memphis' },
  MXC: { name: 'Capitanes de Ciudad de México', city: 'Mexico City' },
  RAP: { name: 'Raptors 905', city: 'Mississauga' },
  RGV: { name: 'Rio Grande Valley Vipers', city: 'Rio Grande Valley' },
  REN: { name: 'Stockton Kings', city: 'Stockton' },
  RIP: { name: 'Rip City Remix', city: 'Portland' },
  SCW: { name: 'Santa Cruz Warriors', city: 'Santa Cruz' },
  SPR: { name: 'Grand Rapids Gold', city: 'Grand Rapids' },
  SXF: { name: 'Sioux Falls Skyforce', city: 'Sioux Falls' },
  TEX: { name: 'Texas Legends', city: 'Frisco' },
  TUL: { name: 'Oklahoma City Blue', city: 'Oklahoma City' },
  VAL: { name: 'Valley Suns', city: 'Phoenix' },
  WES: { name: 'Westchester Knicks', city: 'Westchester' },
  WCB: { name: 'Windy City Bulls', city: 'Chicago' },
  WIS: { name: 'Wisconsin Herd', city: 'Oshkosh' },
  ARK: { name: 'Arkansas RimRockers', city: 'Little Rock' },
  FAY: { name: 'Fayetteville Patriots', city: 'Fayetteville' },
  FLO: { name: 'Florida Flame', city: 'Fort Myers' },
  FTW: { name: 'Fort Worth Flyers', city: 'Fort Worth' },
  GLI: { name: 'G League Ignite', city: 'Henderson' },
  GRE: { name: 'Greenville Groove', city: 'Greenville' },
  MOB: { name: 'Mobile Revelers', city: 'Mobile' },
  ROA: { name: 'Roanoke Dazzle', city: 'Roanoke' },
};

/** Return G League abbreviation for a team name (e.g. "Salt Lake City Stars" -> "SLC"). */
export function getAbbrevByTeamName(teamName) {
  if (!teamName || typeof teamName !== 'string') return null;
  const normalized = teamName.trim();
  for (const [abbrev, info] of Object.entries(TEAM_MAP)) {
    if (info.name === normalized || info.name.includes(normalized) || normalized.includes(info.name)) {
      return abbrev;
    }
  }
  return null;
}
