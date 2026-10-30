# USDC On-Chain Reconciliation Pipeline

A real-time pipeline that streams USDC transfers from the Base Sepolia testnet through Kafka and PySpark into Snowflake, then uses dbt to reconcile them against payment approvals.

Inspired by how AI agent payment platforms work: an agent gets approval before paying, the payment settles on-chain, and finance needs to know whether the two match.

## Architecture

Base Sepolia RPC → Python producer → Kafka (3 partitions) → PySpark Structured Streaming → Snowflake (raw) → dbt (staging, marts)

## How it works

• **Producer** (`producer.py`): polls the Base Sepolia node with `eth_getLogs` for USDC Transfer events, decodes sender, receiver and amount (6 decimals), and sends each transfer to Kafka keyed by transaction hash. It stays 5 blocks behind the chain tip so reorgs cannot change data already read.
• **Spark consumer** (`spark_consumer.py`): reads from Kafka in 10 second micro-batches, casts types, lowercases wallets, validates hashes, wallets and amounts, and writes good rows to `usdc_transfers_clean` and bad rows (with a reason) to `usdc_transfers_rejected`. Checkpoints make restarts safe.
• **Approvals** (`make_approvals.py`): generates test approvals from real transfers, with planted problems: about 10% wrong amounts, about 10% missing tx hashes, and 30 approvals with no payment.
• **dbt** (`crypto_recon/`):
  • `stg_transfers` dedupes on tx hash plus log index
  • `stg_approvals` normalises wallets
  • `reconciliation` matches approvals by reported tx hash, falls back to payer, payee and exact amount, and labels each one matched, amount_mismatch or unmatched
  • `recon_summary` gives counts and USDC totals per status
  • `reconciliation_history` snapshot tracks status changes over time
  • Tests: unique and not null approval IDs, accepted status values

## Results

• Over 100,000 real USDC transfers streamed, 0 rejected
• 530 approvals reconciled: 443 matched, 57 amount mismatches, 30 unmatched, catching every planted problem
• All 7 dbt checks passing

## Problems I hit

• **Spark 4 connector mismatch**: the Kafka connector built for Spark 3.5 (Scala 2.12) crashed on PySpark 4.2.1. Fixed by using the Scala 2.13 connector and matching it to the installed PySpark version.
• **Wrong database**: `.env` still pointed at an old database, so Snowflake said the table did not exist. Fixed by setting the database explicitly in `write_pandas`.
• **Duplicate matches**: the unique test failed with 436 duplicates, because one transaction can contain several USDC transfers sharing a tx hash. Fixed by keeping one match per approval, the closest amount.

## Tech stack

Python, Kafka, PySpark, Snowflake, dbt, JSON-RPC (Base Sepolia)

## Run it

1. `docker start kafka`, then create the `usdc_transfers` topic with 3 partitions
2. `uv run producer.py`
3. `uv run spark_consumer.py`
4. `uv run make_approvals.py`
5. `cd crypto_recon && uv run dbt build`
