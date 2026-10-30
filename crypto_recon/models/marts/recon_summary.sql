select
    status,
    count(*) as approvals,
    sum(approved_amount_usdc) as approved_usdc,
    sum(coalesce(paid_amount_usdc, 0)) as paid_usdc
from {{ ref('reconciliation') }}
group by status