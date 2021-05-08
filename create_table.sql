CREATE TABLE IF NOT EXISTS cwl_war_attacks(
    id INTEGER primary key AUTOINCREMENT,
    season TEXT,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    player_name TEXT,
    player_townhalllevel INTEGER,
    stars INTEGER,
    destruction DECIMAL,
    insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp DATETIME,
    unique(season,startTime,player_name)
);

CREATE TABLE IF NOT EXISTS normal_war_attacks(
    id INTEGER primary key AUTOINCREMENT,
    season TEXT,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    player_name TEXT,
    player_townhalllevel INTEGER,
    stars INTEGER,
    destruction DECIMAL,
    insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp DATETIME,
    unique(season,startTime,player_name)
);