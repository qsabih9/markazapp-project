MERGE INTO stage.markazapp.transactions
  USING (
    SELECT distinct *
    FROM load.markazapp.transactions
    WHERE product_id is not null
  ) AS src
  ON  transactions.customer_id = src.customer_id
  WHEN MATCHED THEN UPDATE
  SET transactions.product_id = src.product_id
    , transactions.purchase_date = TO_DATE(src.purchase_date, 'MM-DD-YYYY')
    , transactions.quantity = src.quantity
    , transactions.product_price = src.product_price
    , transactions.total_price = src.total_price
    , transactions.rating = src.rating
    , transactions.review = src.review
    , transactions.updated_at = current_timestamp
	
  WHEN NOT MATCHED THEN INSERT
  ( customer_id
  , product_id
  , purchase_date
  , quantity
  , product_price
  , total_price
  , rating
  , review
  , created_at
  , updated_at
  )
  VALUES
  ( src.customer_id
  , src.product_id
  , TO_DATE(purchase_date, 'MM-DD-YYYY')
  , src.quantity
  , src.product_price
  , src.total_price
  , src.rating
  , src.review
  , current_timestamp
  , current_timestamp
  )