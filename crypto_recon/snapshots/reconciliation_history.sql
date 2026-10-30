{% snapshot reconciliation_history %}

{{
    config(
        target_schema='DEV_SUYOG',
        unique_key='approval_id',
        strategy='check',
        check_cols=['status', 'tx_hash'],
    )
}}

select approval_id, agent_id, status, tx_hash
from {{ ref('reconciliation') }}

{% endsnapshot %}