CREATE TABLE ai.session_events (
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  session_id text NOT NULL,
  id bigint PRIMARY KEY,
  event_type text NOT NULL,
  prompt text NOT NULL,
  response text NOT NULL,
  tool_name text,
  token_count integer NOT NULL,
  created_at timestamptz NOT NULL
);
