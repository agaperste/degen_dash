-- part of a query repo
-- query name: $DEGEN virtual UTXO base table
-- query link: https://dune.com/queries/3490743


/*
Realized capitalization for $degen (https://warpcast.com/ilemi/0xdfad1f0e)

- what is realized cap for Bitcoin UTXO model: https://academy.glassnode.com/market/realized-capitalization#
    - measure the value of a cryptocurrency's supply in a manner that accounts for the actual price at which each unit last moved
    1. Identify the Last Transaction Price for Each Unit
    2. Multiply Each Unit by Its Last Transaction Price
    3. Sum Up the Realized Values
- however, $degen is an ERC20 fungible token, so we can't track each individual units like we do for Bitcoin
- instead, as Julian proposed, we will ** track and calculate the realized cap on a per user, i.e., wallet address level **
    - doing it the ** virtual UTXO ** way proposed here by CoinMetric https://coinmetrics.io/realized-capitalization/
    - each incoming payment creates a new coin attached to the account, the coin is valued at the price of the movement
    - each outgoing payment triggers a coin selection on the coins attached to the account, the change is valued at the current market price
    - the coin selection we’ll use is largest coins first
    
Others
- https://basescan.org/address/0x88d42b6dbc10d2494a0c6c189cefc7573a6dce62 --> Season 1 (airdrop 2?) airdrop aka claim address, txn from when claiming DEGEN from points
- https://basescan.org/token/0x4ed4e862860bed51a9570b96d89af5e1b0efefed --> $DEGEN token contract address
- how you can get DEGEN
    - Claim
    - Buy it with Uniswap's pool
    - Provide LP and get it?
*/

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
        , coalesce(p.price, p2.price, 9.70382579323361e-7) as price_at_time 
    from all_transfers t 
    left join degen_price p on p.minute = t.minute
    left join dex_price p2 on p2.minute = t.minute
)

, degen_in as (
    select txn_to as address
        , minute
        , degen_amount
        , price_at_time
        , evt_tx_hash
    from together
)

, degen_out as (
    select txn_from as address
        , minute
        , -1 * degen_amount
        , price_at_time
        , evt_tx_hash
    from together
)

, base as (
    select *
    from degen_in
    
    union all 
    
    select *
    from degen_out
)

select * from base
