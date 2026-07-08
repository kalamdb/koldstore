SET koldstore.user_id = 'tenant-0001';

SELECT *
FROM chat.messages
WHERE tenant_id = current_setting('koldstore.user_id')
  AND conversation_id = 'conv-0001'
  AND created_at < timestamptz '2024-06-01'
ORDER BY created_at DESC
LIMIT 100;
