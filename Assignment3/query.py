import psycopg2

hostname = 'localhost'
database = 'testingforpython'
username = 'bhanukedhar'
password = 'bhanukedhar'
port_id = 5432

given_council_name = input("Enter council name")
given_troop_number = int(input("Enter troop Number"))

conn = None
cur = None
try : 
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = password,
        port = port_id
    )


    cur = conn.cursor()

    # q = f"select a.cookie_name, qty + qty2 tqty from (select is2.cookie_name, sum(is2.quantity) qty  from Z1974769.troop t, Z1974769.belongs_to bt, Z1974769.individual_sales is2 where t.troop_number = bt.troop_number  and t.service_number = bt.troop_service_number and t.council_name = bt.troop_council_name  and is2.girl_name = bt.girl_name and is2.girl_address = bt.girl_address and t.troop_number =  {given_troop_number} group by is2.cookie_name) a full outer join  ( select ss.cookie_name , sum(ss.quantity) qty2 from Z1974769.troop t2, Z1974769.shop_sales ss where t2.troop_number = ss.troop_number and t2.troop_number = {given_troop_number} group by ss.cookie_name) b on a.cookie_name = b.cookie_name"
    cur.execute(f"select a.cookie_name, qty + qty2 tqty from (select is2.cookie_name, sum(is2.quantity) qty  from Z1974769.troop t, Z1974769.belongs_to bt, Z1974769.individual_sales is2 where t.troop_number = bt.troop_number  and t.service_number = bt.troop_service_number and t.council_name = bt.troop_council_name  and is2.girl_name = bt.girl_name and is2.girl_address = bt.girl_address and t.troop_number =  {given_troop_number} group by is2.cookie_name) a full outer join  ( select ss.cookie_name , sum(ss.quantity) qty2 from Z1974769.troop t2, Z1974769.shop_sales ss where t2.troop_number = ss.troop_number and t2.troop_number = {given_troop_number} group by ss.cookie_name) b on a.cookie_name = b.cookie_name")
    res = cur.fetchall()
    print(res)






    conn.commit()
except Exception as err:
    print(err)
finally:
    if conn is not None:
        conn.close()
    if cur is not None:
        cur.close()
