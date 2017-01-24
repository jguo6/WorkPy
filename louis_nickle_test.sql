SELECT x.session_id, y.root_id, x.order_id, y.security_id,
       width, drs_price, x.stock_cancel_price, 
       x.create_time,  x.actor_name, y.actor_name, ocancel_stock_price,  
       begin_state_fill_quantity, begin_state_order_quantity
  FROM events.drs_201611 x
  left join events.legs y on x.order_id = y.order_id and x.root_id = y.root_id 
  left join events.new z on z.order_id= x.order_id 
   where begin_state = 'NEW' and x.actor_name = 'DRSActor'-- and x.root_id = x.order_id
limit 5


(select a.event_id, a.root_id, a.order_id, account, trader_name, x.security_id
from events.new_201611 a
join events.legs x on a.root_id = x.root_id and a.order_id = x.order_id
where a.from_pid = 0 and a.root_id = a.order_id)

select * 
from events.drs_201611 
where actor_name = 'DRSActor' and begin_state = 'NEW'
limit 100

