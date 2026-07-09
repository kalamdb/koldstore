CREATE TABLE game.player_events (
  game_id text NOT NULL,
  player_id text NOT NULL,
  match_id text NOT NULL,
  id bigint PRIMARY KEY,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL
);
