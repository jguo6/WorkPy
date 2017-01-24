with stockVega as (
SELECT lastupdated, (CASE WHEN CHARINDEX('/', underlyingSymbol) = 0 
      then underlyingSymbol else LEFT(underlyingSymbol, CHARINDEX('.', underlyingSymbol) - 1) END) as underlyingSymbol, (CASE WHEN CHARINDEX('.', Account) = 0 
      then Account else LEFT(Account, CHARINDEX('.', Account) - 1) END) as account, sum(blendedVolTimeWeightedVega) as vega, sum(impliedGammaC) as gamma, SUM(ThetaCNew) as theta
FROM [positions].[dbo].[tblPnLPositionHistory_NG]
where thetaCNew is not null and account not like '%F%' and account not like '%JAB%' and account not like '%SVN%'
group by lastUpdated, underlyingSymbol, account
)
select top 50 s.lastUpdated, s.account, SUM(abs(s.vega)) * 100 as AbsGroupVega, SUM(s.gamma + s.theta) as fr
from stockVega s
where s.account not like '%F%' and account not like '%LCL%'
group by s.lastUpdated, s.account 
order by s.lastUpdated desc
