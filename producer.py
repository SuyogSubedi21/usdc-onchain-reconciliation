import requests
import json
import time
from confluent_kafka import Producer


producer = Producer({
"bootstrap.servers": "localhost:9092",
"acks": "all",
"enable.idempotence": True,
})

CONFIRMATIONS = 5

RPC_URL = "https://sepolia.base.org"


def rpc_call(method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    response = requests.post(RPC_URL, json=body, timeout=10)
    response.raise_for_status()
    return response.json()["result"]

USDC_ADDRESS = "0x036CbD53842c5426634e7929541eC2318f3dCF7e"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def get_transfers(from_block, to_block):
    params = [{
    "address": USDC_ADDRESS,
    "topics": [TRANSFER_TOPIC],
    "fromBlock": hex(from_block),
    "toBlock": hex(to_block),   
    }]
    return rpc_call("eth_getLogs", params)


def decode_transfer(log):
    return {
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "block_number": int(log["blockNumber"], 16),
        "block_hash": log["blockHash"],
        "from_wallet": "0x" + log["topics"][1][-40:],
        "to_wallet": "0x" + log["topics"][2][-40:],
        "amount_usdc": int(log["data"], 16) / 1_000_000,
    }

if __name__ == "__main__":
    latest = int(rpc_call("eth_blockNumber", []), 16)
    next_block = latest - 100

    while True:
        safe_block = int(rpc_call("eth_blockNumber", []), 16) - CONFIRMATIONS
        if safe_block >= next_block:
            logs = get_transfers(next_block, safe_block)
            for log in logs:
                transfer = decode_transfer(log)
                producer.produce(
                    "usdc_transfers",
                    key=transfer["tx_hash"],
                    value=json.dumps(transfer),
                )
            producer.flush()
            print(f"Blocks {next_block} to {safe_block}: sent {len(logs)} transfers")
            next_block = safe_block + 1
        time.sleep(5)