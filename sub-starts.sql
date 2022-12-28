-- SELECT * FROM `nyt-bigquery-beta-workspace.jose_data.sub_data` ORDER BY 1 DESC
INSERT `nyt-bigquery-beta-workspace.jose_data.sub_data` 

-- PARTITION BY snapshot_date

-- AS
WITH agg AS (
  SELECT
    *
  FROM
    `nyt-bizint-prd.enterprise_sensitive.SA_starts_stop_sub_type` 
),

weekly AS (
  SELECT
   -- DATE_TRUNC((cast (snapshot_date AS date)), WEEK(MONDAY)) + 6 AS week_end,
  	  cast (snapshot_date AS date) as snapshot_date, 
    case 
        when event in ('new start','complete stop') then 'New Subscriber/Complete Stop'
        when event in ('upsell', 'downsell') then 'Upgrade/Downgrade'
        when event like '%share%' then 'Share'
        when event like '%gift%' then 'Gift'
        when event like '%switch%' then 'Switch'
        when event like '%n/a%' then 'Other' 
    END AS  event,
    case 
        when event like '%start%' or event= 'upsell' then 'start' 
        when  event like '%stop%' or event = 'downsell' then 'stop'
    END AS event_type,
    *EXCEPT(snapshot_date,region,event)
  FROM
    agg
  WHERE
        snapshot_date BETWEEN '2021-09-13' AND '2022-12-18'
  	-- snapshot_date BETWEEN {{8 days ago|day|str}} and {{2 days ago|day|str}}
    -- AND extract(dayofweek from date({{yesterday|day|str}})) = 2
    AND financial_entitlement = 'Wirecutter'

)
SELECT
  *except(subscriber_type), 
  case 
    when subscriber_type like '%Only%' and subscriber_type not like '%Wirecutter%' then 'Other' 
    when subscriber_type ='n/a' then 'Other' else subscriber_type 
  end as subscriber_type
  --, ----case when event like '%stop%' then 'stop' else 'start' end as event_type
FROM
weekly  
WHERE event_type is not null
  AND event is not null
-- ORDER BY
--   1 DESC,
--   2,
--   3

