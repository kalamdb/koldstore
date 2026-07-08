CREATE TABLE chat.messages (
  tenant_id text NOT NULL,
  conversation_id text NOT NULL,
  id bigint PRIMARY KEY,
  sender_id text NOT NULL,
  role text NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL,
  edited_at timestamptz,
  deleted_at timestamptz
);

CREATE INDEX chat_messages_tenant_conv_created_idx
  ON chat.messages (tenant_id, conversation_id, created_at DESC);
