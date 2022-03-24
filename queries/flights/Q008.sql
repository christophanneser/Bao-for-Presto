select
  count(*)
from
  flights
where
  origin_name = 'Lambert-St Louis International'
  and dest_name = 'Lincoln Municipal'
