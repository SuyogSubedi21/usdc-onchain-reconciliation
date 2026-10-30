select
    approval_id,
    agent_id,
    mandate_id,
    lower(payer_wallet) as payer_wallet,
    lower(payee_wallet) as payee_wallet,
    approved_amount_usdc,
    reported_tx_hash,
    approved_at
from {{ source('raw', 'approvals') }}