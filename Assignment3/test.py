given_council_name = input("Enter council name")
given_troop_number = int(input("Enter troop Number"))

q = f"select a.cookie_name, qty + qty2 tqty from (select is2.cookie_name, sum(is2.quantity) qty  from troop t, belongs_to bt, individual_sales is2 where t.troop_number = bt.troop_number  and t.service_number = bt.troop_service_number and t.council_name = bt.troop_council_name  and is2.girl_name = bt.girl_name and is2.girl_address = bt.girl_address and t.troop_number =  {given_troop_number} group by is2.cookie_name) a full outer join  ( select ss.cookie_name , sum(ss.quantity) qty2 from troop t2, shop_sales ss where t2.troop_number = ss.troop_number and t2.troop_number = {given_troop_number} group by ss.cookie_name) b on a.cookie_name = b.cookie_name"
print(q)