---
"@iterativeflow/dynamodb": patch
---

DynamoDB: the unfiltered autoscaling-backlog counts (`Timer.dueCount`, `Store.dueCronCount`) now use `Select: COUNT`, so DynamoDB returns the count server-side and transfers no item attributes over the wire.
