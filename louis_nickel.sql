-- 
-- 
-- ----Query by Total 
-- with k as (select *, CASE WHEN underlying in (select ticker from public.nickle_stock) then .05 else .01 END as stockTicksAs
-- from public.orders_201611 a
-- join public.option_ticks x on a.underlying = x.ticker 
-- where a.underlying != 'SPY' and a.underlying != 'IWM' and a.underlying!= 'QQQ'),
-- 
-- kl as (select account, count(CASE WHEN (price >= 3 and stockTicksAs = .01 and (cast(price as numeric) % .05 != 0)) then account else null END) as invalidLargenormal,
-- 	count(CASE WHEN (stockTicksAs = .05 and (cast(price as numeric) % .05 != 0)) then account else null END) as invalidnickle
-- from k
-- group by account)
-- 
-- select * 
-- from kl 
-- 
-- 


-- ----Query by Symbol
-- with k as (select *, CASE WHEN underlying in (select ticker from public.nickle_stock) then .05 else .01 END as stockTicksAs
-- from public.orders_201611 a
-- join public.option_ticks x on a.underlying = x.ticker 
-- where a.underlying != 'SPY' and a.underlying != 'IWM' and a.underlying != 'QQQ'),
-- 
-- 
--  kl as (select count(CASE WHEN (price >= 3 and stockTicksAs = .01 and (cast(price as numeric) % .05 != 0)) then account else null END) as invalidLargenormal, 
-- 	count(CASE WHEN (stockTicksAs = .05 and ticks = 5 and (cast(price as numeric) % .05 != 0)) then account else null END) as invalidnickle, underlying
-- from k
-- group by underlying)
-- 
-- select * from kl 
-- where (invalidLargeNormal !=0 or invalidnickle != 0)
-- order by invalidLargeNormal desc





----Query by Account and Symbol 
-- with k as (select *, CASE WHEN underlying in (select ticker from public.nickle_stock) then .05 else .01 END as stockTicksAs
-- from public.orders_201611 a
-- join public.option_ticks x on a.underlying = x.ticker 
-- where a.underlying != 'SPY' and a.underlying != 'IWM' and a.underlying != 'QQQ') ,
-- 
-- kl as (select account, count(CASE WHEN (price >= 3 and stockTicksAs = .01 and (cast(price as numeric) % .05 != 0)) then account else null END) as invalidLargenormal, 
-- 	count(CASE WHEN (stockTicksAs = .05 and ticks = 5 and (cast(price as numeric) % .05 != 0)) then account else null END) as invalidnickle,underlying
-- from k
-- group by account, underlying)
-- select * from kl 
-- where (invalidLargeNormal !=0 or invalidnickle != 0)
-- order by invalidLargeNormal desc