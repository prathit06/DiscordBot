CREATE TABLE IF NOT EXISTS cwl_war_attacks(
    id serial,
    season varchar,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    player_name varchar,
    player_townhalllevel INTEGER,
    stars INTEGER,
    destruction DECIMAL,
    insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_timestamp TIMESTAMP,
    UNIQUE (season,startTime,player_name,stars,destruction)
);

CREATE TABLE IF NOT EXISTS normal_war_attacks(
    id serial,
    season varchar,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    player_name varchar,
    player_townhalllevel INTEGER,
    stars INTEGER,
    destruction DECIMAL,
    insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_timestamp TIMESTAMP,
    UNIQUE (season,startTime,player_name,stars,destruction)
);
