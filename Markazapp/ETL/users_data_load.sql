truncate table load.creditbook.users_raw;

copy into load.creditbook.users_raw
from @load.creditbook.s3_stage
  PATTERN ='.*users.csv'
  FILE_FORMAT = 'load.creditbook.creditbook_fileformat';

truncate table load.creditbook.users_LD;

insert into load.creditbook.users_LD
select id, timestamp, event_id, document_name, operation, document_id,

data:id user_id ,
data:business_card:name name,
data:business_card:mobile_no mobile_no,
data:business_card:alternate_mobile_no alternate_mobile_no,
data:business_card:location location,
data:business_card:coordinates:lat coordinates_lat,
data:business_card:coordinates:lng coordinates_lng,
data:business_card:business_name business_name,
data:businesss_type businesss_type,
data:cashbook_current_balance cashbook_current_balance,
data:contextID contextID,
data:current_location:latitude location_lat,
data:current_location:longitude location_lng,
data:fcm_token::string fcm_token,
data:fromNewAPP fromNewAPP,
data:img_base_64 img_base_64,
data:img_url img_url,
data:is_active is_active,
data:location_our_logs location_our_logs,
data:location_past_logs location_past_logs,
data:rating:feedback rating_feedback,
to_date(round(data:rating:rated_timestamp:_seconds + data:rating:rated_timestamp:_nanoseconds/1000000000)::varchar(255)) rating_timestamp,
data:rating:stars rating_stars,
data:referral_code referral_code,
to_date(round(data:user_last_activity:_seconds + data:user_last_activity:_nanoseconds/1000000000)::varchar(255)) user_last_activity,
to_date(round(data:user_signup_date:_seconds + data:user_signup_date:_nanoseconds/1000000000)::varchar(255)) user_signup_date

from load.creditbook.users_raw;