-- Databricks notebook source
-- MAGIC %md
-- MAGIC accounting_tax_ledger_ADM
-- MAGIC   

-- COMMAND ----------


DROP TABLE IF EXISTS aws_rds_colossalbet.analytics.accounting_tax_ledger_ADM_scd;

create table aws_rds_colossalbet.analytics.accounting_tax_ledger_ADM_scd as 
select 
      a.*
      ,p.client_rating as client_rating_scd
      ,c.ClientRating as client_rating_current
      ,cast(af.affiliate_name as varchar(50)) as affiliate_name_scd
      ,cast(c.AffiliateName as varchar(50)) as affiliate_name_current
      ,c.DateOpened
      ,c.HeardAbout
      ,case 
        when LEFT(lower(c.HeardAbout),3) = 'ph_' then 'PuntHub' 
       else 'Colossalbet' end as brand
      ,DATE_TRUNC('week',a.FinalisedDate) as finalised_week 


from 
(

(
 select 
      tl.WTLS_ID
      ,tl.TransID
      ,tl.TransactionID
      ,tl.TransactionDate
      ,tl.OutcomeDate
      ,tl.FinalDate
      ,tl.Finalised  
      ,case when tl.Finalised > '2000-01-01' then tl.Finalised else null end as FinalisedDate
      ,tl.AccountID
      ,tl.UserID
      ,tl.EventID
      ,case when tl.ToteDerived = 1 then 'Parimutuel' else 'Fixed-Odds' end as ProductType
      ,case when tl.SportID in (1,37,39) then 'Racing' 
            when tl.SportID = 0 then 'Others' 
            else 'Sports' end as SportType
      ,tl.SportID
      ,spd.Sport
      ,tl.RaceDate as EventDate
      ,tl.Meeting as EventVenue
      ,case when tl.RaceType = 'G' then 'Greyhounds'
            when tl.RaceType = 'H' then 'Harness'
            when tl.RaceType = 'T' then 'Thoroughbreds'
            end as RaceType
      ,tl.RaceNum
      ,tl.RaceState as EventState
      ,tl.OptionID
      ,tl.TeamA
      ,tl.TeamB
      ,tl.Description
      ,NULL as WalletEventDescription
      ,substr(tl.Outcome, 1, 256) as Outcome 
      ,tl.BetTypeID
      ,btd.BetType
      ,tl.Special as PriceType
      ,tl.ToteDerived
      ,case when tl.FreeBetBalance <>0 then 1 else 0 end as IsFreeBet
      ,case when tl.FreeBetBalance <>0 then -1 * tl.TotalAmount else 0 end as FreeBetBalance
      ,case when tl.IsSGM = 1 then 1
            when tl.Special = 'SameRaceMulti' then 1
            when btd.BetType = 'Multi' or tl.MultiLegs> 0 then 1 
            else 0 end as IsMulti
      ,case when tl.IsSGM then 1 else 0 end as IsSGM
      ,case when tl.Special = 'SameRaceMulti' then 1 else 0 end as IsSRM
      ,tl.MultiLegs
      ,case when tl.MultiLegs > 0 then 1/(cast(tl.MultiLegs as FLOAT)) else cast(1 as FLOAT) end as WeightInBet
      ,ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY MultiLegID) as MultiLegNo
      ,tl.MultiLegID
      ,tl.BetAmount
      ,tl.TotalAmount
      ,tl.Dividend
      ,tl.BetReturn
      ,tl.BetMethodID
      ,bmd.BetMethod
      ,tl.Referrer
      ,tl.MatchCompID
      ,tl.ParentDescription
      ,tl.ParentEvent
      ,tl.MarketTypeID
      ,tl.Country
      ,tl.Premium
      ,tl.LicenseID
      ,case when tl.FreeBetBalance <>0 then 0 else tl.TotalAmount end as Turnover
      ,(tl.TotalAmount - tl.BetReturn) as Revenue
      ,case when tl.FreeBetBalance <>0 then - tl.BetReturn else tl.TotalAmount - tl.BetReturn end as RevenueGross
      ,tl.TransactionTime
      ,tl.Handicap
      ,tl.UserIP
      ,tl.QLDCode
      ,tl.MeetingType
      ,tl.POCState
      ,tl.AddrHomeState
      ,tl.InterfaceTag
      ,0 as TransferAmount

    from aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT tl 
    left join aws_rds_colossalbet.dbo.Sport_DIM spd
    on tl.SportID = spd.SportID

    left join aws_rds_colossalbet.dbo.BetMethod_DIM bmd
    on tl. BetMethodID = bmd.BetMethodID

    left join aws_rds_colossalbet.dbo.BetType_DIM btd
    on tl.BetTypeID = btd.BetTypeID
)

UNION ALL 

(

     select 
      br.BetAndReturnsID
      ,0 as TransID
      ,TransactionID
      ,TransactionDate
      ,OutcomeDate
      ,TransactionDate  as FinalDate
      ,TransactionDate as Finalised
      ,case when TransactionDate > '2000-01-01' then TransactionDate else null end as FinalisedDate
      ,AccountID
      ,UserID
      ,EventID
      ,'Transfer' as ProductType
      ,'Transfer' as SportType
      ,br.SportID
      ,spd.Sport
      ,NULL as EventDate
      ,NULL as EventVenue
      ,NULL as RaceType
      ,NULL as RaceNum
      ,NULL as EventState
      ,NULL as OptionID
      ,NULL as TeamA
      ,NULL as TeamB
      ,EventDescription as Description
      ,case 
        when EventDescription = 'EFT Deposit' and Outcome like '%: BS :%' then 'Deposit - BS Cash-In'
        when EventDescription = 'EFT Deposit' and Outcome like '%: EML :%' then 'Deposit - EML'
        when EventDescription = 'EFT Deposit' and Outcome like '%: FZ :%' then 'Deposit - Credit Card FZ'
        when EventDescription = 'EFT Deposit' and Outcome like '%: ZP :%' then 'Deposit - PayID'
        when EventDescription = 'EFT Deposit' then 'Deposit - EFT'
        when EventDescription = 'POLI Deposit' then 'Deposit - POLI'
        when EventDescription = 'BPAY Deposit' then 'Deposit - BPay'
        when EventDescription = 'Bank Deposit' then 'Deposit - Bank'
        when EventDescription = 'Credit Card Deposit' then 'Deposit - Credit Card'

        when EventDescription = 'Cash Withdrawal'  then 'Withdrawal - EML'
        when EventDescription = 'EFT Withdrawal' then 'Withdrawal - EFT'
        when EventDescription = 'Credit Card Withdrawal' then 'Withdrawal - Credit Card'
        when EventDescription like 'Adjustment' then 'Adjustment' 
        else EventDescription 
       END as WalletEventDescription
      ,substr(Outcome, 1, 256) as Outcome
      ,br.BetTypeID
      ,btd.BetType
      ,Special as PriceType
      ,0 as ToteDerived
      ,0 as IsFreeBet
      ,0 as FreeBetBalance
      ,0 as IsMulti
      ,0 as IsSGM
      ,0 as IsSRM
      ,0 as MultiLegs
      ,0 as WeightInBet
      ,0 as MultiLegNo
      ,NULL as MultiLegID
      ,0 as BetAmount
      ,0 as TotalAmount
      ,0 as Dividend
      ,0 as BetReturn
      ,br.BetMethodID
      ,bmd.BetMethod
      ,NULL as Referrer
      ,NULL as MatchCompID
      ,NULL as ParentDescription
      ,NULL as ParentEvent
      ,NULL as MarketTypeID
      ,NULL as Country
      ,NULL as Premium
      ,LicenseID
      ,0 as Turnover
      ,0 as Revenue
      ,0 as RevenueGross
      ,NULL as TransactionTime
      ,NULL as Handicap
      ,NULL as UserIP
      ,NULL as QLDCode
      ,NULL as MeetingType
      ,NULL as POCState
      ,NULL as AddrHomeState
      ,InterfaceTag
      ,BalanceEffect as TransferAmount

    from aws_rds_colossalbet.dbo.BetsAndReturns_FACT br 
    left join aws_rds_colossalbet.dbo.Sport_DIM spd
    on br.SportID = spd.SportID

    left join aws_rds_colossalbet.dbo.Status_DIM std
    on br. BetStatusID = std.StatusID

    left join aws_rds_colossalbet.dbo.BetMethod_DIM bmd
    on br. BetMethodID = bmd.BetMethodID

    left join aws_rds_colossalbet.dbo.BetType_DIM btd
    on br.BetTypeID = btd.BetTypeID
    and btd.BetType IN ('Transfer') 

where AccountID >=1000 and br.BetTypeID =11

)

) a

left join aws_rds_colossalbet.dbo.Client_DIM c 
on a.AccountID = c.AccountID

left join 
(
select 
account_id
,case when scd_version = 1 then account_opened_date else scd_start end as scd_start
,dateadd(day,-1,COALESCE(scd_end,'3000-01-01')) as scd_end
,client_rating
from aws_rds_colossalbet.analytics.account_client_rating_scd
)p
on a.AccountID = p.account_id
and cast(a.TransactionDate as date) between p.scd_start and p.scd_end

left join aws_rds_colossalbet.analytics.account_affiliate_scd_adjusted af
on a.AccountID = af.account_id
and cast(a.TransactionDate as date) between af.adjusted_scd_start and af.adjusted_scd_end


;


-- COMMAND ----------


