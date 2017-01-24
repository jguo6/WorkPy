--USE [CMSandbox]
--GO

--/****** Object:  Table [dbo].[FuturePrice]    Script Date: 12/02/2016 09:28:44 ******/
--SET ANSI_NULLS ON
--GO

--SET QUOTED_IDENTIFIER ON
--GO

--SET ANSI_PADDING ON
--GO

--CREATE TABLE [dbo].[tradeDays](
--	[date] [smalldatetime] NULL,
--	[num] [int] NULL,
--) ON [PRIMARY]

--GO

--SET ANSI_PADDING OFF
--GO

--INSERT INTO [CMSandbox].[dbo].[tradeDays] 
--SELECT [TradeDate], RANK() over (order by tradedate desc) as num 
--  FROM[igtdev].[dbo].[tblStockHistory] 
--  where TradeDate > '2015-01-01' and Symbol = 'AAPL' 
--  group by TradeDate
--  order by TradeDate desc
  
select * from tradedays 

--truncate table [CMSandbox].[dbo].[tradeDays]


