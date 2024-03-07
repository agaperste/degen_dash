-- part of a query repo
-- query name: $DEGEN Individual Holder Profit and Loss To Date
-- query link: https://dune.com/queries/3490939


with all_transfers as (
    select "from" as txn_from
        , to as txn_to
        , value / 1e18 as degen_amount
        , date_trunc('minute', evt_block_time) as minute
        , evt_block_number
        , evt_tx_hash
    from erc20_base.evt_Transfer
    where contract_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed -- DEGEN
)

, degen_price as (
    select minute
        , price
    from prices.usd 
    where contract_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed 
        and blockchain = 'base'
        and minute >= timestamp '2024-01-15 16:45' -- no DEGEN price until 2024-01-15 16:45
)
 
, dex_price as (
    select 
        minute
        , avg(degen_price) as price -- there can be multiple trades in one minute, so we take the average
        from (
            select date_trunc('minute', block_time) as minute
                , amount_usd
                , case when token_bought_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed then amount_usd / (token_bought_amount_raw / 1e18)
                else amount_usd / (token_sold_amount_raw / 1e18)
                end as degen_price
            from dex.trades
            where (token_bought_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed or token_sold_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed)
                and block_time < timestamp '2024-01-15 16:45' -- no DEGEN price in prices.usd until 2024-01-15 16:45
                and block_date >= date('2024-01-07')
        )
        group by 1
)

, together as (
    select t.*
        , t.degen_amount * coalesce(p.price, p2.price, 9.70382579323361e-7) as degen_usd 
    from all_transfers t 
    left join degen_price p on p.minute = t.minute
    left join dex_price p2 on p2.minute = t.minute
)

, degen_in as (
    select txn_to as address
        , date_trunc('day', minute) as day
        , sum(degen_amount) as sum_degen
        , sum(degen_usd) as sum_degen_usd
    from together
    group by 1,2
)

, degen_out as (
    select txn_from as address
        , date_trunc('day', minute) as day
        , -1 * sum(degen_amount) as sum_degen
        , -1 * sum(degen_usd) as sum_degen_usd
    from together
    group by 1,2
)

, base as (
    select *
    from degen_in
    
    union all 
    
    select *
    from degen_out
)

,  result as (
    select address
        , sum(sum_degen) as net_degen
        , sum(sum_degen_usd) as net_degen_usd
    from base
    group by 1
)

select address
    -- formatting extremely small numbers left due to calculation rounding 
    , case when net_degen < 0.0001 then 0 else net_degen end as net_degen 
    , case when abs(net_degen) < 0.001 then 0 else net_degen_usd end as net_degen_usd 
from result
where net_degen >= case 
        when '{{current_holder}}' = 'yes' then cast('{{current_holder_threshold}}' as double)
         when '{{current_holder}}' = 'no' then -1
    end