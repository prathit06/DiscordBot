CREATE TABLE IF NOT EXISTS war_attacks(
    id INTEGER primary key AUTOINCREMENT,
    season TEXT,
    teamSize INTEGER,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    clan_tag TEXT,
    clan_name TEXT,
    player_tag TEXT,
    player_name TEXT,
    player_townhalllevel INTEGER,
    stars INTEGER,
    destruction DECIMAL,
    insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp DATETIME,
    unique(season,startTime,player_tag)
);