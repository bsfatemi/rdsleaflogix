DROP TABLE IF EXISTS population;
CREATE TABLE population AS (
	SELECT
		ol.org,
		ol.source_system,
		
		c.customer_id,
		c.created_at_utc   as customer_created_at_utc,
		c.last_updated_utc as customer_last_updated_utc,
		c.phone,
		c.first_name,
		c.last_name,
		c.gender,
		c.birthday,
		c.age,
		c.email,
		c.user_type,
		c.geocode_type     as customer_geocode_type,
		c.long_address     as customer_long_address,
		c.address_street1  as customer_address_street1,
		c.address_street2  as customer_address_street2,
		c.city             as customer_city,
		c.state            as customer_state,
		c.zipcode          as customer_zipcode,
		c.latitude         as customer_latitude,
		c.longitude        as customer_longitude,
		c.pos_is_subscribed,

		o.order_id,
		o.order_facility,
		o.order_type,
		o.sold_by,
		o.sold_by_id,
		o.order_time_local,
		o.order_time_utc,
		o.delivery_order_received_local,
		o.delivery_order_started_local,
		o.delivery_order_complete_local,
		o.order_subtotal,
		o.order_tax,
		o.order_total,
		o.order_discount,
		o.delivery_order_address,
		o.delivery_order_street1,
		o.delivery_order_street2,
		o.delivery_order_city,
		o.delivery_order_zipcode,
		o.delivery_order_state,
		o.delivery_lon,
		o.delivery_lat,
		
		
		ol.order_line_id,
		ol.product_id,
		ol.product_name,
		ol.brand_id,
		ol.brand_name,
		ol.product_uom,
		ol.product_unit_count,
		ol.product_qty,
		ol.product_category_name,
		ol.product_class,
		ol.order_line_total,
		ol.order_line_discount,
		ol.order_line_tax,
		ol.order_line_subtotal,
		ol.order_line_list_price
		
	FROM order_lines as ol
	LEFT JOIN orders as o
		ON  ol.order_id      = o.order_id 
		AND ol.customer_id   = o.customer_id
		AND ol.org           = o.org
	LEFT JOIN customers as c
		ON  ol.customer_id   = c.customer_id
		AND ol.org           = c.org
	WHERE  
			ol.source_system IS NOT NULL
		AND ol.org IS NOT NULL
		AND ol.order_id IS NOT NULL
		AND ol.order_line_id IS NOT NULL
);