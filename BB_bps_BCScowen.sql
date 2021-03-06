
with bc as (select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, date, mid from [CMSandbox].[dbo].[JG_Bclays16] 
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val,  '2016-01-01' as date, mid  from [CMSandbox].[dbo].[JG_BclaysJan16] 
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, '2016-03-01' as date, mid  from [CMSandbox].[dbo].[JG_BclaysMar16]
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, '2016-05-01' date, mid  from [CMSandbox].[dbo].[JG_BclaysMay16]
			union select left(ticker, CHARINDEX('.',ticker)-1) ticker, executed_val, '2016-04-01' date, mid  from [CMSandbox].[dbo].[JG_BclaysApril16]
			union select ticker, [principal_val] as executed_val, date, -[arvl_shortfall_bps] as mid  from [CMSandbox].[dbo].[JG_cowen16])
select  case when Liq_Rank<=125 then '01.[1-125]'
            when Liq_Rank<=250 then '02.[126-250]'
              when Liq_Rank<=500 then '03.[251-500]'
              when Liq_Rank<=750 then '04.[501-750]'
              when Liq_Rank<=1000 then '05.[751-1000]'
              when Liq_Rank<=1500 then '06.[1001-1500]'
              when Liq_Rank<=2000 then '07.[1501-2000]'
              else '08.[2000+]'
       end liq_rank,
       sum(executed_val) ntnl, -sum(mid*executed_val)*0.0001 impact, -sum(mid*executed_val)/sum(executed_val) bps, date
from bc jg with (nolock)
join CMSandbox..tblLiquidityRankings tl with (nolock)
on jg.date between tl.RowIn and tl.RowOut and jg.ticker =tl.Symbol
group by case when Liq_Rank<=125 then '01.[1-125]'
            when Liq_Rank<=250 then '02.[126-250]'
              when Liq_Rank<=500 then '03.[251-500]'
              when Liq_Rank<=750 then '04.[501-750]'
              when Liq_Rank<=1000 then '05.[751-1000]'
              when Liq_Rank<=1500 then '06.[1001-1500]'
              when Liq_Rank<=2000 then '07.[1501-2000]'
              else '08.[2000+]'
       end ,date
order by DATE, liq_rank 


--select jg.date, 
--       sum(executed_val) ntnl, -sum(mid*executed_val)*0.0001 impact, -sum(mid*executed_val)/sum(executed_val) bps
--from bc jg with (nolock)
--join CMSandbox..tblLiquidityRankings tl with (nolock)
--on jg.date between tl.RowIn and tl.RowOut and jg.ticker=tl.Symbol
--group by jg.date
--order by jg.DATE
