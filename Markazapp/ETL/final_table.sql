create or replace table dw.creditbook.final_table as

select u.user_id , android_os, app_version, device_language , city_geoip , year(t.creation_timestamp) year , month(t.creation_timestamp) month ,

cast(sum(case when lower(t.transaction_type) = 'credit' then amount
     else 0
     end ) as int) amount_of_credits ,
     
cast(sum(case when lower(t.transaction_type) = 'debit' then amount 
     else 0
     end ) as int) amount_of_debits ,


cast(sum(case when lower(t.transaction_type) = 'credit' then amount*-1
     when lower(t.transaction_type) = 'debit' then amount else 0 end) as int) amount_of_total_transactions ,

sum(case when lower(coalesce(t.transaction_type,'null')) in ('credit') then 1
    else 0 end) no_of_credits ,
    
sum(case when lower(coalesce(t.transaction_type,'null')) in ('debit') then 1
    else 0 end) no_of_debits ,   

sum(case when lower(coalesce(t.transaction_type,'null')) in ('credit','debit') then 1
    else 0 end) no_of_transactions ,
    
avg(rating_stars) ratings ,
max(user_last_activity) user_last_activity ,

current_date - max(user_last_activity) days_since_last_activity ,
min(creation_timestamp)  creation_timestamp
     


from load.creditbook.users_LD u
join load.creditbook.transactions_LD t 
on u.user_id = t.user_id and u.timestamp = t.timestamp
join load.creditbook.analytics_LD a
on u.user_id = a.user_id and cast(u.timestamp as date) = a.event_date
where u.user_id is not NULL
group by 1,2,3,4,5,6,7
order by 1