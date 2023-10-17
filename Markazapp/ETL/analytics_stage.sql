MERGE INTO stage.creditbook.analytics
  USING (
    SELECT *
    FROM load.creditbook.analytics_LD 
  ) AS src
  ON  analytics.id = src.id
  WHEN MATCHED THEN UPDATE
  SET analytics.event_date = src.event_date
    , analytics.event_timestamp = src.event_timestamp
    , analytics.event_name = src.event_name
    , analytics.user_id = src.user_id
    , analytics.user_pseudo_id = src.user_pseudo_id
	, analytics.device_model = src.device_model
	, analytics.android_os = src.android_os
	, analytics.device_language = src.device_language
	, analytics.city_geoIp = src.city_geoIp
	, analytics.app_version = src.app_version
	
  WHEN NOT MATCHED THEN INSERT
  ( id
  , event_date
  , event_timestamp
  , event_name
  , user_id
  , user_pseudo_id
  , device_model
  , android_os
  , device_language
  , city_geoIp
  , app_version
  )
  VALUES
  ( src.id
  , src.event_date
  , src.event_timestamp
  , src.event_name
  , src.user_id
  , src.user_pseudo_id
  , src.device_model
  , src.android_os
  , src.device_language
  , src.city_geoIp
  , src.app_version
  )
  ; 