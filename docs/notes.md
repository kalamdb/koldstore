Tasks:
- Add cold policy with a filter and limit as well
- Check the bg worker which does the WAL reading, we can make it also flush cold rows or make this as a shared resource for both of these workers
- We need to add lock while we delete/prune hot/mirror rows: docs/cases/async-flush-prune-race.md

