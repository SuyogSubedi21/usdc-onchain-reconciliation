select
    tx_hash,
    log_index,
    block_number,
    block_hash,
    from_wallet,
    to_wallet,
    amount_usdc,
    loaded_at
from {{ source('raw', 'usdc_transfers_clean') }}
qualify row_number() over (
    partition by tx_hash, log_index
    order by loaded_at
) = 1   