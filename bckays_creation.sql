USE [CMSandbox]
GO

/****** Object:  Table [dbo].[FuturePrice]    Script Date: 12/02/2016 09:28:44 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[cow_jeff16](
	[date] [smalldatetime] NULL,
	[ntnl] [float] NULL,
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

BULK INSERT [CMSandbox].[dbo].[JG_cowen16]
	FROM 'h:\\importfiles\\jguo\\cowen_janapr_aug.csv'
WITH (FIELDTERMINATOR = ',')
GO 
