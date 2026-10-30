with approvals as (
    select * from {{ ref('stg_approvals') }}
),

transfers as (
    select * from {{ ref('stg_transfers') }}
),

by_hash as (
    select a.approval_id, t.tx_hash, t.amount_usdc, 'tx_hash' as match_method
    from approvals a
    join transfers t on a.reported_tx_hash = t.tx_hash
    qualify row_number() over (
        partition by a.approval_id
        order by abs(t.amount_usdc - a.approved_amount_usdc)
    ) = 1
),

by_wallets as (
    select a.approval_id, t.tx_hash, t.amount_usdc, 'wallets' as match_method
    from approvals a
    join transfers t
        on a.payer_wallet = t.from_wallet
        and a.payee_wallet = t.to_wallet
        and a.approved_amount_usdc = t.amount_usdc
    where a.reported_tx_hash is null
    qualify row_number() over (partition by a.approval_id order by t.loaded_at) = 1
),

matches as (
    select * from by_hash
    union all
    select * from by_wallets
)

select
    a.approval_id,
    a.agent_id,
    a.mandate_id,
    a.approved_amount_usdc,
    m.tx_hash,
    m.amount_usdc as paid_amount_usdc,
    m.match_method,
    case
        when m.tx_hash is null then 'unmatched'
        when m.amount_usdc != a.approved_amount_usdc then 'amount_mismatch'
        else 'matched'
    end as status
from approvals a
left join matches m on a.approval_id = m.approval_id