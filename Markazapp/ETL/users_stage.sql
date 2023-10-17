MERGE INTO stage.creditbook.users
  USING (
    SELECT *
    FROM load.creditbook.users_LD 
  ) AS src
  ON  users.id = src.id
  WHEN MATCHED THEN UPDATE
  SET users.timestamp = src.timestamp
    , users.event_id = src.event_id
    , users.document_name = src.document_name
    , users.operation = src.operation
    , users.document_id = src.document_id
	, users.user_id = src.user_id
	, users.name = src.name
	, users.mobile_no = src.mobile_no
	, users.alternate_mobile_no = src.alternate_mobile_no
	, users.location = src.location
	, users.coordinates_lat = src.coordinates_lat
	, users.coordinates_lng = src.coordinates_lng
	, users.business_name = src.business_name
	, users.businesss_type = src.businesss_type
	, users.cashbook_current_balance = src.cashbook_current_balance
	, users.contextID = src.contextID
	, users.location_lat = src.location_lat
	, users.location_lgn = src.location_lgn
	, users.fcm_token = src.fcm_token
	, users.fromNewAPP = src.fromNewAPP
	, users.img_base_64 = src.img_base_64
	, users.img_url = src.img_url
	, users.is_active = src.is_active
	, users.location_our_logs = src.location_our_logs
	, users.location_past_logs = src.location_past_logs
	, users.rating_feedback = src.rating_feedback
	, users.rating_timestamp = src.rating_timestamp
	, users.rating_stars = src.rating_stars
	, users.referral_code = src.referral_code
	, users.user_last_activity = src.user_last_activity
	, users.user_signup_date = src.user_signup_date
	
  WHEN NOT MATCHED THEN INSERT
  ( id
  , timestamp
  , event_id
  , document_name
  , operation
  , document_id
  , user_id
  , name
  , mobile_no
  , alternate_mobile_no
  , location
  , coordinates_lat
  , coordinates_lng
  , business_name
  , businesss_type
  , cashbook_current_balance
  , contextID
  , location_lat
  , location_lgn
  , fcm_token
  , fromNewAPP
  , img_base_64
  , img_url
  , is_active
  , location_our_logs
  , location_past_logs
  , rating_feedback
  , rating_timestamp
  , rating_stars
  , referral_code
  , user_last_activity
  , user_signup_date
  
  )
  VALUES
  ( src.id
  , src.timestamp
  , src.event_id
  , src.document_name
  , src.operation
  , src.document_id
  , src.user_id
  , src.name
  , src.mobile_no
  , src.alternate_mobile_no
  , src.location
  , src.coordinates_lat
  , src.coordinates_lng
  , src.business_name
  , src.businesss_type
  , src.cashbook_current_balance
  , src.contextID
  , src.location_lat
  , src.location_lgn
  , src.fcm_token
  , src.fromNewAPP
  , src.img_base_64
  , src.img_url
  , src.is_active
  , src.location_our_logs
  , src.location_past_logs
  , src.rating_feedback
  , src.rating_timestamp
  , src.rating_stars
  , src.referral_code
  , src.user_last_activity
  , src.user_signup_date
  )
  ; 