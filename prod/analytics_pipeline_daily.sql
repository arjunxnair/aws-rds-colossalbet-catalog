-- Databricks notebook source
-- MAGIC %md
-- MAGIC aws_rds_colossalbet.analytics.account_transactions_daywise_summary

-- COMMAND ----------


DROP TABLE IF EXISTS aws_rds_colossalbet.analytics.account_transactions_daywise_summary;

CREATE TABLE aws_rds_colossalbet.analytics.account_transactions_daywise_summary AS
select wrapper.* 

FROM
(
select 
ds.*
,ngr.revenue_ngr as revenue_ngr_amount,
ngr.product_fees,
ngr.gst,
ngr.poct_fees,
ngr.bank_fees,
ngr.adjustments as ngr_adjustments

from
(
select 
AccountID as account_id,
case when DATEDIFF(DAY,TransactionDate, Finalised) >0 then cast(Finalised as date) else cast(TransactionDate as date)  END as transaction_date,
sum(case when BetTypeID =11 and EventDescription LIKE '%Deposit%' then BalanceEffect else 0 end) as deposits,
sum(case when BetTypeID =11 and EventDescription LIKE '%Deposit%' and BalanceEffect <> 0 then 1 else 0 end) as deposits_count,
max(case when BetTypeID =11 and EventDescription LIKE '%Deposit%' then BalanceEffect end) as max_deposit_on_day,
min(case when BetTypeID =11 and EventDescription LIKE '%Deposit%' then BalanceEffect end) as min_deposit_on_day,

sum(case when BetTypeID =11 and EventDescription LIKE '%Withdrawal%' then -1* BalanceEffect else 0 end) as withdrawals,
sum(case when BetTypeID =11 and EventDescription LIKE '%Withdrawal%' and -1* BalanceEffect <> 0 then 1 else 0 end) as withdrawals_count,
max(case when BetTypeID =11 and EventDescription LIKE '%Withdrawal%' then -1* BalanceEffect end) as max_withdrawal_on_day,
min(case when BetTypeID =11 and EventDescription LIKE '%Withdrawal%' then -1* BalanceEffect end) as min_withdrawal_on_day,

sum(case when BetTypeID =11 and EventDescription LIKE '%Adjustment%' then BalanceEffect else 0 end) as adjustments,
sum(case when BetTypeID =11 and EventDescription LIKE '%Adjustment%' and BalanceEffect <> 0 then 1 else 0 end) as adjustments_count,

sum(case when betstatusID = 13 or BetTypeID =1 then 0 else TotalAmount end) as bet_amount,
sum(case when betstatusID = 13 or BetTypeID =1 then 0  
         when TotalAmount <> 0 then 1 
         else 0 end) as bet_count,

sum(case when betstatusID = 13 or BetTypeID =1 then ReturnAmount else 0 end) as bet_cancelled_amount,
sum(case when betstatusID = 13 or BetTypeID =1 then ReturnAmount else 0 end) as bet_cancelled_count,

sum(case when BetTypeID =5 and TotalAmount <>0 THEN BetAmount else 0 end) as exotic_bet_amount,
sum(case when BetTypeID =5 and TotalAmount <>0 THEN 1 else 0 end) as exotic_bet_count,

sum(case when BetTypeID =8 and TotalAmount <>0 THEN BetAmount else 0 end) as multi_bet_amount,
sum(case when BetTypeID =8 and TotalAmount <>0 THEN 1 else 0 end) as multi_bet_count,

sum(case when TotalAmount <>0 AND FreeBetBalance <> 0 then TotalAmount else 0 end) as freebet_amount,
sum(case when TotalAmount <>0 AND FreeBetBalance <> 0 then 1 else 0 end) as freebet_count,

sum(case when betstatusID = 13 or BetTypeID =1 then 0 
         when TotalAmount <>0 AND FreeBetBalance <> 0 then 0 
         else TotalAmount end) as turnover_amount,
sum(case when betstatusID = 13 or BetTypeID =1 then 0  
         when TotalAmount <>0 AND FreeBetBalance <> 0 then 0 
         when TotalAmount <> 0 then 1 
         else 0 end) as turnover_bet_count,

sum(case when betstatusID = 13 or BetTypeID =1 then 0 else ReturnAmount end) as dividend_amount,
sum(case when (betstatusID = 13 or BetTypeID =1) then 0  
         when ReturnAmount > 0 then 1 
         else 0 end) as dividend_bet_count,

sum(case when TotalAmount <>0 AND FreeBetBalance <> 0 then ReturnAmount else 0 end) as freebet_dividend_amount,
sum(case when TotalAmount <>0 AND FreeBetBalance <> 0 and ReturnAmount > 0 then 1 
         else 0 end) as freebet_dividend_count,       

sum(case when (betstatusID = 13 or BetTypeID =1) then 0 
         when CHARINDEX('Boosted',Outcome)>0 then TotalAmount 
         else 0 end) as boosted_bet_amount,
sum(case when betstatusID = 13 or BetTypeID =1 then 0  
         when CHARINDEX('Boosted',Outcome)>0 then 1 
         else 0 end) as boosted_bet_count,
sum(case when (betstatusID = 13 or BetTypeID =1) then 0 
         when CHARINDEX('Boosted',Outcome)>0 then ReturnAmount 
         else 0 end) as boosted_bet_dividend_amount,

sum(case when (betstatusID = 13 or BetTypeID =1) then 0 
         when FreeBetBalance <> 0 then - ReturnAmount 
    else TotalAmount - ReturnAmount end) as revenue_gross_amount,
sum(case when (betstatusID = 13 or BetTypeID =1) then 0 
         when FreeBetBalance <> 0 then 0 
         else TotalAmount - ReturnAmount end) as revenue_non_bb_amount,

sum(case when BalanceEffect <>0 then BalanceEffect else 0 end) as consolidated_balance_effect,
sum(1) as transaction_count,

from_utc_timestamp(current_timestamp, 'Australia/Sydney') as last_updated_timestamp
from aws_rds_colossalbet.dbo.BetsAndReturns_FACT 
where BetStatusID NOT IN ('14')
group by AccountID, 
(case when DATEDIFF(DAY,TransactionDate, Finalised) >0 then cast(Finalised as date) else cast(TransactionDate as date) end)
)ds
left join 
(
select
a.*,
RevenueGross-(product_fees*1.1)-gst-poct_fees-(bank_fees*1.1)-adjustments as revenue_ngr

from (
select
AccountID,
cast(Finalised as date) as reporting_date,
sum(turnover) as Turnover, 
sum(RevenueGross) as RevenueGross, 
sum(case 
when Description like '%Deposit%' then TransferAmount else 0 end ) as deposits,
sum(case 
when Description like '%Adjustment%' then TransferAmount else 0 end ) as adjustments,
sum(
case 
when (SportID in ('1','37','39') and ToteDerived= 1) then 0.0325 * TotalAmount
when (SportID in ('1','37','39') and ToteDerived= 0) then 0.0250 * TotalAmount
when (SportID in ('2')) then 0.0085 * TotalAmount
when (SportID in ('32','7')) then 0.02 * TotalAmount
when (SportID in ('3')) then 0.025 * TotalAmount
else 0 
END) as product_fees,
sum(0.0909*RevenueGross) as gst,
sum(
case 
when (POCState in ('NSW')) then 0.15 * Revenue
when (POCState in ('VIC')) then 0.1 * Revenue
when (POCState in ('QLD','SA','WA','TAS','NT')) then 0.15 * Revenue
when (POCState in ('ACT')) then 0.2 * RevenueGross
else 0 
END) as poct_fees,
sum(
case 
when WalletEventDescription = 'Deposit - BS Cash-In' then (0.04*TransferAmount)
when WalletEventDescription = 'Deposit - Credit Card' then (0.0143*TransferAmount)
when WalletEventDescription = 'Deposit - Credit Card FZ' then (0.0125*TransferAmount)
when WalletEventDescription = 'Deposit - POLI' then (case when (0.01*TransferAmount) > 3 then 3 else (0.01*TransferAmount)end)
when WalletEventDescription = 'Deposit - BPay' then (0.99)
when WalletEventDescription = 'Deposit - PayID' then (0.48)
when WalletEventDescription = 'Withdrawal - EML' then (0.008*ABS(TransferAmount))
when WalletEventDescription = 'Withdrawal - EFT' then (0.275)
else 0
END) as bank_fees
from aws_rds_colossalbet.analytics.accounting_tax_ledger_ADM_scd 
group by AccountID,cast(Finalised as date)
)a
)ngr
on ds.account_id = ngr.AccountID
and ds.transaction_date = ngr.reporting_date

)wrapper
;



-- COMMAND ----------



-- COMMAND ----------


DECLARE @last_month_first_day DATE = dateadd(mm,-1,dateadd(m, datediff(m, 0, getdate()), 0));   
DECLARE @last_month_last_day DATE = dateadd(dd,-1,dateadd(m, datediff(m, 0, getdate()), 0)); 
DECLARE @this_month_first_day DATE = dateadd(m, datediff(m, 0, getdate()), 0); 
DECLARE @last_week_first_day DATE = dateadd(wk,-1,dateadd(wk, datediff(wk, 0, getdate()), 0));  
DECLARE @last_week_last_day DATE = dateadd(dd,-1,dateadd(wk, datediff(wk, 0, getdate()), 0));  
DECLARE @this_week_first_day DATE = dateadd(wk,0,dateadd(wk, datediff(wk, 0, getdate()), 0));   
DECLARE @yesterday DATE = cast(GETDATE()-1 AT TIME ZONE 'UTC' AT time zone 'AUS Eastern Standard Time' as date);
DECLARE @intraday DATE = cast(GETDATE() AT TIME ZONE 'UTC' AT time zone 'AUS Eastern Standard Time' as date);
DECLARE @default_deposit_limit INT = 10000;


DROP TABLE IF EXISTS colossalbet.analytics.account_metrics_summary;

SELECT 
    case 
        when LEFT(lower(HeardAbout),3) = 'ph_' then 'PuntHub' 
        when AccountID in ('29286', '29285','27511','27101') then 'PuntHub' 
    else 'Colossalbet' end as brand, 
    case 
        when LEFT(lower(HeardAbout),3) = 'ph_' then 'PuntHub' 
        when AccountID in ('29286', '29285') then 'PuntHub' 
        when lower(HeardAbout) = 'punthub' then 'PuntHub' 
        when LEFT(lower(HeardAbout),4) = 'tbd_' then 'TBD' 
        when lower(HeardAbout) = 'wolfden' then 'Wolfden' 
        when LEFT(lower(HeardAbout),2) = 'g_' then 'Paid Search' 
        when lower(HeardAbout) = 'ios' then 'Paid Search' 
        when lower(HeardAbout) = 'android' then 'Paid Search' 
    else 'Organic' end as acquisition_source_category,  
    wrapper.*
    ,(
        SELECT MIN(C)
        FROM (VALUES (deposit_limit_daily),(deposit_limit_remaining_weekly), (deposit_limit_remaining_fortnightly),(deposit_limit_remaining_monthly)) v (C)
     ) AS MaxDepositOffer
    , case when bdt_profile in ('Restricted','WISE', 'Corporate','SportRest','SportWISE') or rg_current_status in ('Self_Excluded', 'Closed', 'On a Spell') or affiliate_name in ('Wolfden','WD_Talent') or SelfExclude > GETDATE()-30 then 0 else 1 end as recommendation_eligible
    , case 
      when PIN in ('2239','2258') then 1 
      when HomeState in ('SA','WA') then 0
      when (account_opened_date > GETDATE()-28 and bet_date_last is null) then 1
      when (bdt_profile in ('Restricted','WISE', 'Corporate','SportRest','SportWISE') or trading_profile in ('Hard', 'Watch','Fraud', 'Bonus Hunter') or rg_current_status in ('Self_Excluded', 'Closed', 'On a Spell') or affiliate_name in ('Wolfden','WD_Talent') or bb_abuse_level >= 3) then 0 
      else 1 end as mbs_eligible
    , case 
        when PIN in ('2239','2258') then 20
        when HomeState in ('SA','WA') then 0
        when (account_opened_date > GETDATE()-28 and bet_date_last is null) then 50
        when (bdt_profile in ('Restricted','WISE', 'Corporate','SportRest','SportWISE') or trading_profile in ('Hard', 'Watch','Fraud', 'Bonus Hunter') or rg_current_status in ('Self_Excluded', 'Closed', 'On a Spell') or affiliate_name in ('Wolfden','WD_Talent') or bb_abuse_level >= 4) then 0 
        --when bet_size_avg > 250 then 250
        --when bet_size_avg > 150 then 200
        --when bet_size_avg > 50 then 100
        when bet_size_avg > 20 then 50
        else 20
       end as mbs_cap
INTO colossalbet.analytics.account_metrics_summary
FROM
(
select 
c.accountID
,c.accountID+1000 as PIN
,c.UserID
,c.DateOpened as account_opened_date
,case when c.verified = 1 then 'true' else 'false' end as ID_verified
,case when s.[Status] = 'Closed' then c.ClosedDate else null end as account_closed_date
,s.[Status]
,c.HomeState
,c.HeardAbout as acquisition_source
,c.HeardAbout
,c.RunningBalance
,c.RunningFreeBetBalance
,case when cast(c.SelfExclude as date) > '2100-01-01' then c.SelfExcludeSetOn end as permanent_self_excluded_on_date
,case 
    when s.[Status] = 'Closed' and cast(c.ClosedDate as date) > '2000-01-01' then DATEDIFF(day,c.dateOpened,c.ClosedDate) 
    when cast(c.SelfExclude as date) > '2100-01-01' then DATEDIFF(day,c.dateOpened,c.SelfExcludeSetOn) 
 else DATEDIFF(day,c.dateOpened,@intraday) 
 end as lifetime_days
,c.ClientRating as client_rating
,case when LEFT(UPPER(AffiliateName),2) = 'OB' then NULL ELSE c.AffiliateName END as affiliate_name
,case when LEFT(UPPER(AffiliateName),2) = 'OB' then 'Organic' 
      when c.AffiliateName is null then 'Organic' 
      when c.AffiliateName like 'Restricted' then 'Restricted'
      else 'Managed' END as Managed
,case 
    when ClientRating = -2 then 'Duplicate'
    when ClientRating = -1 then 'Test Account'
    when ClientRating = 0 then 'Not Rated'
    when ClientRating = 1 then 'Hard'
    when ClientRating = 2 then 'Small'
    when ClientRating = 3 then 'VIP'
    when ClientRating = 4 then 'Watch'
    when ClientRating = 5 then 'Normal'
    when ClientRating = 6 then 'Bonus Hunter'
    when ClientRating = 7 then 'Fraud'
    when ClientRating = 8 then 'Rebate'
    else 'NA' end 
    as trading_profile
,case when cast(c.SelfExclude as date) > '2100-01-01' then 'Self_Excluded' 
      when s.[Status] = 'Closed' then 'Closed'
      when cast(c.SelfExclude as date) > '2000-01-01' and cast(c.SelfExclude as date) > getdate() then 'On a Spell'
      when c.DepositLimitDaily > 999999999 and c.DepositLimitWeekly > 999999999 and c.DepositLimitFortnightly > 999999999 and c.DepositLimitMonthly > 999999999 then 'Deposit Limit Not Set'
      when c.DepositLimitDaily is null and c.DepositLimitWeekly is null and c.DepositLimitFortnightly is null and c.DepositLimitMonthly is null then 'Deposit Limit Not Set'
      else 'Deposit Limit Set' end as rg_current_status
,case when cast(c.SelfExclude as date) > '2100-01-01' then '2100-01-01' end as SelfExclude
,c.DepositLimitDaily as deposit_limit_daily
,c.DepositLimitWeekly as deposit_limit_weekly
,c.DepositLimitFortnightly as deposit_limit_fortnightly
,c.DepositLimitMonthly as deposit_limit_monthly
,pr.profiling_num_bets
,isnull(pr.bdt_profile,'Normal') as bdt_profile
,pr.profiling_sim_yield
,pr.profiling_target_generosity
,pr.profiling_bdt_function
,am.*
,bb.bonus_bet_latest_issued_date
,bb.bonus_bet_issued_lifetime
,bb.bonus_bet_issued_last_91_days
,bonus_bet_used_lifetime
,bb.Last_Month_Bonus_bet_Issued
,bb.Last_Month_Bonus_bet_Issued/NULLIF(Last_Month_Revenue_Gross,0) as Last_Month_Bonusbet_issued_by_Revenue_Gross
,bb.This_Month_Bonus_bet_Issued
,bb.This_Month_Bonus_bet_Issued/NULLIF(This_Month_Revenue_Gross,0) as This_Month_Bonusbet_issued_by_Revenue_Gross
,bb.offers_sent
,bb.offers_redeemed
,bb.offers_not_redeemed
,case when deposit_date_first is not null and first_bb_issued_date <= deposit_date_first then 'Deposit Offer'  
      when deposit_date_first is null and first_bb_issued_date > '2022-07-01' then 'Deposit Offer'  
      else 'No Deposit Offer' end as first_deposit_offer
,cast((case when bb.offers_sent> 0 then bb.offers_redeemed/cast(bb.offers_sent as float) else 0 end) as NUMERIC(3,2)) as offer_conversion_percentage

,COALESCE(bb_unclaimed.live_unclaimed_bb_offer,0) as live_unclaimed_bb_offer

,(case when (COALESCE(c.DepositLimitDaily,@default_deposit_limit) - COALESCE(am.deposits_last_1_days,0))>0 then (COALESCE(c.DepositLimitDaily,@default_deposit_limit) - COALESCE(am.deposits_last_1_days,0)) else 0 end) as deposit_limit_remaining_daily
,(case when (COALESCE(c.DepositLimitWeekly,@default_deposit_limit)- COALESCE(am.deposits_last_7_days,0))>0 then (COALESCE(c.DepositLimitWeekly,@default_deposit_limit)- COALESCE(am.deposits_last_7_days,0)) else 0 end) as deposit_limit_remaining_weekly
,(case when (COALESCE(c.DepositLimitFortnightly,@default_deposit_limit) - COALESCE(am.deposits_last_14_days,0)) > 0 then (COALESCE(c.DepositLimitFortnightly,@default_deposit_limit) - COALESCE(am.deposits_last_14_days,0)) else 0 end) as deposit_limit_remaining_fortnightly
,(case when (COALESCE(c.DepositLimitMonthly,@default_deposit_limit) - COALESCE(am.deposits_last_28_days,0)) > 0 then (COALESCE(c.DepositLimitMonthly,@default_deposit_limit) - COALESCE(am.deposits_last_28_days,0)) else 0 end) as deposit_limit_remaining_monthly


,bb_behaviour.bb_turnover_roi
,bb_behaviour.bb_revenue_roi
,bb_behaviour.bb_deposit_roi
,bb_behaviour.bb_withdrawal_roi
,bb_behaviour.bb_deposit_count
,bb_behaviour.bb_withdrawal_count
,deposit_offer.avg_deposits_day_latest_five_active_days
,deposit_offer.avg_deposits_transaction_latest_five_active_days
,case when bb_abuser.bb_abuse_level = 5 then 'true' else 'false' end as bb_abuser_flag 
,case when bb_abuser.bb_abuse_level is NULL then 0 else bb_abuse_level end as bb_abuse_level 
,case when managed_in_past.managed_in_past > 0 then 'managed_in_past' else 'unmanaged_in_past' end as managed_in_past
,cp.fav_day_1
,cp.bets_perc_fav_day_1
,cp.fav_day_2
,cp.bets_perc_fav_day_2
,cp.fav_racing_1
,cp.bets_perc_fav_racetype_1
,cp.fav_racing_2
,cp.bets_perc_fav_racetype_2
,cp.fav_racing_3
,cp.bets_perc_fav_racetype_3
,cp.fav_sport_1
,bets_perc_fav_sport_1
,cp.fav_sport_2
,bets_perc_fav_sport_2
,cp.fav_sport_3
,bets_perc_fav_sport_3

,cp.fav_team
,cp.fav_thoroughbreds_venue
,cp.fav_harness_venue
,cp.fav_greyhound_venue
,cp.fav_racing_venue
,cp.top_fav_racing_venues
,cp.fav_betting_channel_1
,COALESCE(bb_adj.adjusted_lifetime_turnover,0)  as adjusted_lifetime_turnover
,COALESCE(bb_adj.adjusted_freebet_percentage,0) as adjusted_freebet_percentage
,sfmc.sfmc_status
,c.comment as trader_comment

from colossalbet.dbo.client_dim c WITH (NOLOCK)
left join colossalbet.dbo.Status_DIM s 
  on c.StatusID = s.StatusID


left join
(
select 
account_id
,count(distinct transaction_date) as lifetime_active_days
,sum(deposits) as deposits_lifetime
,sum(deposits_count) as deposits_count_lifetime
,(case when sum(deposits_count) > 0 then sum(deposits)/sum(deposits_count) else NULL end)as deposit_avg_size
,min(case when deposits <>0 then transaction_date end) as deposit_date_first
,max(case when deposits <>0 then transaction_date end) as deposit_date_last
,max(max_deposit_on_day) as deposit_amount_max
,min(min_deposit_on_day) as deposit_amount_min
,sum(case when transaction_date = @intraday then deposits else 0 end) as deposits_last_1_days
,sum(case when transaction_date = @intraday then deposits else 0 end) as deposits_today
,sum(case when transaction_date between dateadd(day,-6,@yesterday) AND @yesterday then deposits else 0 end) as deposits_last_7_days
,sum(case when transaction_date between dateadd(day,-13,@yesterday) AND @yesterday then deposits else 0 end) as deposits_last_14_days
,sum(case when transaction_date between dateadd(day,-27,@yesterday) AND @yesterday then deposits else 0 end) as deposits_last_28_days
,sum(case when transaction_date between dateadd(day,-90,@yesterday) AND @yesterday then deposits else 0 end) as deposits_last_91_days
,sum(case when transaction_date between dateadd(day,-181,@yesterday) AND @yesterday then deposits else 0 end) as deposits_last_182_days
,sum(case when transaction_date between dateadd(day,-363,@yesterday) AND @yesterday then deposits else 0 end) as deposits_last_364_days

,sum(adjustments) as adjustments_lifetime
,sum(adjustments_count) as adjustments_count_lifetime

,sum(withdrawals) as withdrawals_lifetime
,sum(withdrawals_count) as withdrawals_count_lifetime
,(case when sum(withdrawals_count)>0 then sum(withdrawals)/sum(withdrawals_count) else NULL end )as withdrawal_avg_size
,min(case when withdrawals <>0 then transaction_date end) as withdrawal_date_first
,max(case when withdrawals <>0 then transaction_date end) as withdrawal_date_last
,max(max_withdrawal_on_day) as withdrawal_amount_max
,min(min_withdrawal_on_day) as withdrawal_amount_min

,sum(bet_amount) as bet_amount_lifetime
,sum(bet_count) as bet_count_lifetime

,sum(freebet_amount) as freebet_amount_lifetime
,sum(freebet_dividend_amount) as freebet_dividend_amount_lifetime
,max(case when freebet_amount <>0 then transaction_date end) as freebet_date_last
,sum(case when transaction_date = @intraday then freebet_amount else 0 end) as freebet_amount_today
,sum(case when transaction_date between  dateadd(day,-6,@yesterday) AND @yesterday then freebet_amount else 0 end) as freebet_amount_last_7_days
,sum(case when transaction_date between  dateadd(day,-13,@yesterday) AND @yesterday then freebet_amount else 0 end) as freebet_amount_last_14_days
,sum(case when transaction_date between  dateadd(day,-27,@yesterday) AND @yesterday then freebet_amount else 0 end) as freebet_amount_last_28_days
,sum(case when transaction_date between  dateadd(day,-90,@yesterday) AND @yesterday then freebet_amount else 0 end) as freebet_amount_last_91_days
,sum(case when transaction_date between  dateadd(day,-181,@yesterday) AND @yesterday then freebet_amount else 0 end) as freebet_amount_last_182_days
,sum(case when transaction_date between  dateadd(day,-363,@yesterday) AND @yesterday then freebet_amount else 0 end) as freebet_amount_last_364_days
,sum(freebet_count) as freebet_count_lifetime

,sum(boosted_bet_amount) as boosted_bet_amount_lifetime
,sum(boosted_bet_count) as boosted_bet_count_lifetime
,sum(boosted_bet_dividend_amount) as boosted_bet_dividend_amount_lifetime

,sum(turnover_amount) as turnover_amount_lifetime
,sum(case when transaction_date = @intraday then turnover_amount else 0 end) as turnover_amount_today
,sum(case when transaction_date between dateadd(day,-6,@yesterday) AND @yesterday then turnover_amount else 0 end) as turnover_amount_last_7_days
,sum(case when transaction_date between dateadd(day,-13,@yesterday) AND @yesterday then turnover_amount else 0 end) as turnover_amount_last_14_days
,sum(case when transaction_date between dateadd(day,-27,@yesterday) AND @yesterday then turnover_amount else 0 end) as turnover_amount_last_28_days
,sum(case when transaction_date between dateadd(day,-90,@yesterday)AND @yesterday then turnover_amount else 0 end) as turnover_amount_last_91_days
,sum(case when transaction_date between dateadd(day,-181,@yesterday) AND @yesterday then turnover_amount else 0 end) as turnover_amount_last_182_days
,sum(case when transaction_date between dateadd(day,-363,@yesterday) AND @yesterday then turnover_amount else 0 end) as turnover_amount_last_364_days
,sum(case when transaction_date between @last_week_first_day AND @last_week_last_day then turnover_amount else 0 end) as turnover_amount_last_week
,sum(case when transaction_date between @this_week_first_day AND @intraday then turnover_amount else 0 end) as turnover_amount_this_week


,sum(turnover_bet_count) as turnover_bet_count_lifetime

,sum(dividend_amount) as dividend_amount_lifetime
,sum(dividend_bet_count) as dividend_bet_count_lifetime

,sum(revenue_gross_amount) as revenue_gross_amount_lifetime
,sum(case when transaction_date = @intraday then revenue_gross_amount else 0 end) as revenue_gross_amount_today
,sum(case when transaction_date between  dateadd(day,-6,@yesterday) AND @yesterday then revenue_gross_amount else 0 end) as revenue_gross_amount_last_7_days
,sum(case when transaction_date between  dateadd(day,-13,@yesterday) AND @yesterday then revenue_gross_amount else 0 end) as revenue_gross_amount_last_14_days
,sum(case when transaction_date between  dateadd(day,-27,@yesterday) AND @yesterday then revenue_gross_amount else 0 end) as revenue_gross_amount_last_28_days
,sum(case when transaction_date between  dateadd(day,-90,@yesterday) AND @yesterday then revenue_gross_amount else 0 end) as revenue_gross_amount_last_91_days
,sum(case when transaction_date between  dateadd(day,-181,@yesterday) AND @yesterday then revenue_gross_amount else 0 end) as revenue_gross_amount_last_182_days
,sum(case when transaction_date between  dateadd(day,-363,@yesterday) AND @yesterday then revenue_gross_amount else 0 end) as revenue_gross_amount_last_364_days

,sum(revenue_ngr_amount) as revenue_ngr_amount_lifetime
,sum(case when transaction_date = @intraday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_today
,sum(case when transaction_date between dateadd(day,-6,@yesterday) AND @yesterday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_7_days
,sum(case when transaction_date between dateadd(day,-13,@yesterday) AND @yesterday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_14_days
,sum(case when transaction_date between dateadd(day,-27,@yesterday) AND @yesterday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_28_days
,sum(case when transaction_date between dateadd(day,-90,@yesterday) AND @yesterday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_91_days
,sum(case when transaction_date between dateadd(day,-181,@yesterday) AND @yesterday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_182_days
,sum(case when transaction_date between dateadd(day,-363,@yesterday) AND @yesterday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_364_days

,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then revenue_ngr_amount else 0 end) as revenue_ngr_amount_last_month
,sum(case when transaction_date between @this_month_first_day AND @intraday then revenue_ngr_amount else 0 end) as revenue_ngr_amount_this_month


,(case when sum(bet_count) > 0 then sum(bet_amount)/sum(bet_count) else null end )as bet_size_avg
,min(case when bet_amount <>0 then transaction_date end) as bet_date_first
,max(case when bet_amount <>0 then transaction_date end) as bet_date_last
,max(case when bet_amount <>0 and dividend_amount > 0 then transaction_date end) as bet_date_last_winning
,max(case when bet_amount <>0 and dividend_amount <= 0 then transaction_date end) as bet_date_last_losing

,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 1 then transaction_date end) as bet_on_sunday_last
,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 2 then transaction_date end) as bet_on_monday_last
,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 3 then transaction_date end) as bet_on_tuesday_last
,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 4 then transaction_date end) as bet_on_wednesday_last
,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 5 then transaction_date end) as bet_on_thursday_last
,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 6 then transaction_date end) as bet_on_friday_last
,max(case when bet_amount <>0 and DATEPART(DW, transaction_date) = 7 then transaction_date end) as bet_on_saturday_last



,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then bet_amount else 0 end)/ NULLIF(sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then bet_count else 0 end),0) as Last_Month_Average_Bet_Size
,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then deposits else 0 end) as Last_Month_Deposit_Value
,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then withdrawals else 0 end) as Last_Month_Withdrawal_Value

,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then revenue_gross_amount else 0 end)/NULLIF(sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then turnover_amount else 0 end),0) as Last_Month_Yield_Gross
,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then revenue_gross_amount else 0 end) as Last_Month_Revenue_Gross
,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then turnover_amount else 0 end) as Last_Month_Turnover
,sum(case when transaction_date between @last_month_first_day AND @last_month_last_day then freebet_amount else 0 end) as Last_Month_Freebet

,sum(case when transaction_date between @this_month_first_day AND @intraday then bet_amount else 0 end)/NULLIF(sum(case when transaction_date between @this_month_first_day AND GETDATE() then bet_count else 0 end),0) as This_Month_Average_Bet_Size
,sum(case when transaction_date between @this_month_first_day AND @intraday then deposits else 0 end) as This_Month_Deposit_Value
,sum(case when transaction_date between @this_month_first_day AND @intraday then withdrawals else 0 end) as This_Month_Withdrawal_Value

,sum(case when transaction_date between @this_month_first_day AND @intraday then revenue_gross_amount else 0 end)/NULLIF(sum(case when transaction_date between @this_month_first_day AND GETDATE() then turnover_amount else 0 end),0) as This_Month_Yield_Gross
,sum(case when transaction_date between @this_month_first_day AND @intraday then revenue_gross_amount else 0 end) as This_Month_Revenue_Gross
,sum(case when transaction_date between @this_month_first_day AND @intraday then turnover_amount else 0 end) as This_Month_Turnover
,sum(case when transaction_date between @this_month_first_day AND @intraday then freebet_amount else 0 end) as This_Month_Freebet


,CURRENT_TIMESTAMP AT TIME ZONE 'AUS Eastern Standard Time' as last_updated_timestamp

from colossalbet.analytics.account_transactions_daywise_summary 
group by account_id
)am 
on c.accountID = am.account_id


left join
    (
    select 
    AccountID
    ,max(case when CreatedEstimate > ClaimedAt then ClaimedAt else CreatedEstimate end) as bonus_bet_latest_issued_date 
    ,cast(min(CreatedEstimate) as date) as first_bb_issued_date
    ,sum(BonusBetValue) as bonus_bet_issued_lifetime
    ,sum(BonusBetValue - RemainingValue) as bonus_bet_used_lifetime
    ,sum(case when ClaimedAt between dateadd(day,-90,@yesterday) AND @yesterday then BonusBetValue else 0 end) as bonus_bet_issued_last_91_days
    ,sum(case when ClaimedAt between @last_month_first_day AND @last_month_last_day then BonusBetValue else 0 end) as Last_Month_Bonus_bet_Issued
    ,sum(case when ClaimedAt between @this_month_first_day AND @intraday then BonusBetValue else 0 end) as This_Month_Bonus_bet_Issued
    ,COUNT(DISTINCT case when BonusBetPromotion like '%CRM%' then BonusBetID end) as offers_sent
    ,COUNT(DISTINCT case when BonusBetPromotion like '%CRM%' and ClaimedAt is not null then BonusBetID end) as offers_redeemed
    ,COUNT(DISTINCT case when BonusBetPromotion like '%CRM%' and ClaimedAt is null then BonusBetID end) as offers_not_redeemed
    from colossalbet.analytics.BonusBets_FACT WITH (NOLOCK)
    group by AccountID
    )bb
on c.accountID = bb.accountID

left join
    (
    select 
    AccountID
    ,count(distinct case when ClaimedAt IS NULL then BonusBetID end) as live_unclaimed_bb_offer
    from colossalbet.dbo.BonusBets_FACT WITH (NOLOCK)
    group by AccountID
    )bb_unclaimed
on c.accountID = bb_unclaimed.accountID

left join 
(
SELECT 
    [AccountID]
    ,UserID
    ,num_bets as profiling_num_bets
    ,bdt_profile
    ,mean_sim_yield as profiling_sim_yield
    ,IIF(ROUND(POWER(IIF(mean_sim_yield>0, mean_sim_yield, 0.005),2)*0.94+(IIF(mean_sim_yield>0, mean_sim_yield, 0.005)*0.18)-0.0082,2)>0,
        ROUND(POWER(IIF(mean_sim_yield>0, mean_sim_yield, 0.005),2)*0.94+(IIF(mean_sim_yield>0, mean_sim_yield, 0.005)*0.18)-0.0082,2),
        0) as profiling_target_generosity
    ,bdt_function as profiling_bdt_function
FROM [colossalbet].[trading].[customer_profile]
)pr 
on c.accountID = pr.AccountID  

left join colossalbet.analytics.customer_bonus_bet_behaviour bb_behaviour 
on c.accountID = bb_behaviour.AccountID  

left join 
(
SELECT 
 [AccountID],
 max(case when reason_description = 'Trader Input' then 5
      when reason_description = 'Multiple Props on Same Event' then 5
      when reason_description = 'BB odds > 5x avg cash odds' then 5
      when reason_description = 'MBS promotions turnover abuse'  then 4
      when reason_description = 'MBS promotions is last bet date' then 3
     else 2 end) as bb_abuse_level
FROM [colossalbet].[analytics].[bonus_bet_abusers]
group by AccountID
)bb_abuser
on c.accountID = bb_abuser.AccountID


left join 
(
SELECT 
a.account_id,
case when a.email_subscription_status not in ('active') or a.sms_subscription_status not in ('active') then 'Unsubscribed' else 'Active' end as sfmc_status
FROM [colossalbet].[salesforce].[contacts_subcriptions_status] a
)sfmc
on c.accountID = sfmc.account_id

left join 
(
SELECT 
[account_id]
,sum(case when affiliate_name IS NOT NULL AND affiliate_name not like '%ORG%' then 1 else 0 end) as managed_in_past
FROM [colossalbet].[analytics].[account_affiliate_scd]
group by account_id
)managed_in_past
on c.accountID = managed_in_past.account_id


left join 
(
select 
account_id, 
avg(deposits) as avg_deposits_day_latest_five_active_days, 
avg(deposits/deposits_count) as avg_deposits_transaction_latest_five_active_days
from
(
    select 
    account_id, 
    deposits, 
    deposits_count,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER by transaction_date desc) as row_number
from colossalbet.analytics.account_transactions_daywise_summary where deposits >0
)a
where row_number <=5
group by account_id
)deposit_offer
on c.accountID = deposit_offer.account_id

left join colossalbet.analytics.customer_preferences cp 
on c.accountID = cp.account_id


left join colossalbet.analytics.turnover_adjusted_for_generosity bb_adj
on c.accountID = bb_adj.AccountID

where c.AccountID > 1000
)wrapper
;

CREATE INDEX ix_account
ON colossalbet.analytics.account_metrics_summary(AccountID);

END;
