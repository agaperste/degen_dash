-- part of a query repo
-- query name: $DEGEN daily realized cap
-- query link: https://dune.com/queries/3499954


with bal_per_txn as (
    select address, cast("timestamp" as timestamp) as time, cast(realized_balance as double) as realized_balance
    from dune.dune.dataset_degen_realized_balanace
)

, ranked_bal AS (
  SELECT
    address,
    time,
    realized_balance,
    ROW_NUMBER() OVER (PARTITION BY address, DATE_TRUNC('day', time) ORDER BY time DESC) AS rank_order
  FROM
    bal_per_txn
)

, daily_bal as (
    select address, date_trunc('day', time) as day, realized_balance from ranked_bal
    where rank_order = 1
)

, time_series as (
    with time_seq as (
                    select sequence(
                        (select date_trunc('day', min(time)) from bal_per_txn)
                        , date_trunc('day', cast(now() as timestamp)) 
                        , interval '1' day
                    ) as time 
                )
    select time.time
    from time_seq
    cross join unnest(time) as time(time)
)

, filled_balances AS (
    SELECT
    t.time AS time
    , a.address AS address
    , COALESCE(b.realized_balance
          , lag(b.realized_balance) IGNORE NULLS OVER (PARTITION BY a.address ORDER BY t.time)
          , 0
    ) AS realized_balance
    FROM time_series t
    CROSS JOIN (SELECT DISTINCT address FROM daily_bal) a
    LEFT JOIN daily_bal b 
        ON a.address = b.address AND t.time = b.day
)


, degen_price as (
    select date_trunc('day', minute) as day
        , avg(price) as price
    from prices.usd 
    where contract_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed 
        and blockchain = 'base'
        and minute >= timestamp '2024-01-15 16:45' -- no DEGEN price until 2024-01-15 16:45
    group by 1

    union all
    
    select day
        , avg(degen_price) as price -- there can be multiple trades in one minute, so we take the average
    from (
        select date_trunc('day', block_time) as day
            , amount_usd
            , case when token_bought_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed then amount_usd / (token_bought_amount_raw / 1e18)
            else amount_usd / (token_sold_amount_raw / 1e18)
            end as degen_price
        from dex.trades
        where (token_bought_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed or token_sold_address = 0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed)
            and block_time < date('2024-01-15') -- no DEGEN price in prices.usd until 2024-01-15 16:45
            and block_date >= date('2024-01-07')
    )
    group by 1
)


SELECT time
    , sum(realized_balance) as realized_cap
    , avg(price) as price 
FROM filled_balances
left join degen_price on day = time
group by 1
-- order by address, time