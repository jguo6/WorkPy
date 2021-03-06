/****** Script for SelectTopNRows command from SSMS  ******/
WITH x as (SELECT *
  FROM [RTPosition].[dbo].[tblactivities] a
  Where CPS = 'S' and TradeDate > '2016-12-01'  and (OperatorID = '218')
	and TradeSource != 'BASKET-PRD' 
  )
  
  select * from x a
  where TransactionID not in (select TransactionID from x b where b.TradeDate = a.TradeDate group by TransactionID having COUNT(*) > 1) 
  order by ExecutionTime asc 
  