

--filled query 
    with something as (select root_id, security, LEFT(CAST(create_time as text), 19) as create_time, ROW_NUMBER() OVER(PARTITION BY root_id order by execution_time asc) as rk  
    from events.fills_201611 
    where security like 'O:%' and order_state = 'FILLED')
    select s.*
    from something s
    where s.rk = 1
limit 10 

--cancelled query 
with cancels as (select root_id, (cast('2016-11-01' as text) + substring(CAST(create_time as text), 12, 17)) as create_time, ROW_NUMBER() OVER (PARTITION BY root_id order by create_time asc) as rnk
from events.xi_201611
where actor_name = 'OptionXI' and end_state = 'CANCELED')
select c.*
from cancels c
where c.rnk = 1
order by create_time desc
limit 100


select * 
from events.fills_201611
where root_id = '00005aee-911c-4ba3-aa6b-fc583a05357d'
limit 100 

select * 
from events.fills_201611
where security like 'O:%' and order_state = 'FILLED'
order by root_id
limit 20

