import os
import random
import uuid
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    private_key_file=os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="CRYPTO",
    schema="RAW",
    role=os.getenv("SNOWFLAKE_ROLE"),
)

transfers = conn.cursor().execute("""
    select tx_hash, from_wallet, to_wallet, amount_usdc, loaded_at
    from crypto.raw.usdc_transfers_clean
    sample (500 rows)
""").fetch_pandas_all()

agents = ["agent_alpha", "agent_beta", "agent_gamma", "agent_delta"]
rows = []

for _, t in transfers.iterrows():
    roll = random.random()
    amount = float(t["AMOUNT_USDC"])
    tx_hash = t["TX_HASH"]
    if roll < 0.10:
        amount = round(amount * 0.5, 6)
    elif roll < 0.20:
        tx_hash = None
    rows.append({
        "APPROVAL_ID": str(uuid.uuid4()),
        "AGENT_ID": random.choice(agents),
        "MANDATE_ID": "mandate_" + str(random.randint(1, 5)),
        "PAYER_WALLET": t["FROM_WALLET"],
        "PAYEE_WALLET": t["TO_WALLET"],
        "APPROVED_AMOUNT_USDC": amount,
        "REPORTED_TX_HASH": tx_hash,
        "APPROVED_AT": t["LOADED_AT"],
    })

for _ in range(30):
    rows.append({
        "APPROVAL_ID": str(uuid.uuid4()),
        "AGENT_ID": random.choice(agents),
        "MANDATE_ID": "mandate_" + str(random.randint(1, 5)),
        "PAYER_WALLET": "0x" + uuid.uuid4().hex + "00000000",
        "PAYEE_WALLET": "0x" + uuid.uuid4().hex + "00000000",
        "APPROVED_AMOUNT_USDC": round(random.uniform(1, 100), 2),
        "REPORTED_TX_HASH": None,
        "APPROVED_AT": transfers["LOADED_AT"].max(),
    })

import pandas as pd
df = pd.DataFrame(rows)
write_pandas(conn, df, "APPROVALS", database="CRYPTO", schema="RAW",
             auto_create_table=True, overwrite=True, use_logical_type=True)
print(f"Wrote {len(df)} approvals")
conn.close()