import psycopg2
import sys

hostname = 'localhost'
database = 'testingforpython'
username = 'bhanukedhar'
password = 'bhanukedhar'
port_id = 5432

for filename in sys.argv[1:]:

    # all_data = open("cookie.dat", "r")
    lines =[]
    # for data in all_data:
    #     lines.append(list(data.strip().split("|")))
    print(filename)
    with open(filename, "r") as all_data:
        next(all_data)
        for data in all_data:
            lines.append(list(data.strip().split("|")))

    # for i in lines:
    #     print(i[11])
    # for i in range(len(all_data_with_sepetared)):
    # print(all_data)

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = password,
            port = port_id
        )


        
        cur = conn.cursor()

        #inserting unique values into cookie
        #11 positions
        cur.execute('DELETE FROM Z1974769.cookie')
        insert_cookie = 'INSERT INTO Z1974769.cookie (cookie_name) VALUES (%s)'
        insert_cookie_values = list(set([i[11] for i in lines]))
        for na in insert_cookie_values:
            if na.count("") != len(na):
                cur.execute(insert_cookie, (na,))
        
        #inserting unique values into baker
        #13, 14 positions
        cur.execute('DELETE FROM Z1974769.baker')
        insert_baker = 'INSERT INTO Z1974769.baker(baker_name, address) VALUES (%s, %s)'
        insert_baker_values = list(set([(i[13],i[14]) for i in lines]))
        for na in insert_baker_values:
            if na.count("") != len(na):
                cur.execute(insert_baker, na)

        #inserting into girl
        #6,7,8
        cur.execute('DELETE FROM Z1974769.girl')
        insert_girl = 'INSERT INTO Z1974769.girl(girl_name, address, girl_rank) values (%s, %s, %s)'
        insert_girl_values = list(set([(i[6],i[7],i[8]) for i in lines]))
        for na in insert_girl_values:
            if na.count("") != len(na):
                cur.execute(insert_girl, na)

        #inserting into leader
        #4, 5 
        cur.execute("DELETE FROM Z1974769.leader")
        insert_leader = 'INSERT INTO Z1974769.leader(leader_name, address) VALUES (%s, %s)'
        insert_leader_values = list(set([(i[4],i[5]) for i in lines]))
        for na in insert_leader_values:
            if na.count("") != len(na):
                cur.execute(insert_leader, na)
        
        #inserting into customer
        #9, 10
        cur.execute("DELETE FROM Z1974769.customer")
        insert_customer = 'INSERT INTO Z1974769.customer(customer_name, address) VALUES (%s, %s)'
        insert_customer_values = list(set([(i[9],i[10]) for i in lines]))
        for na in insert_customer_values:
            if na.count("") != len(na):
                cur.execute(insert_customer, na)

        #inserting into council
        # 0, 13
        cur.execute("DELETE FROM Z1974769.council")      
        insert_council = "INSERT INTO Z1974769.council(council_name, baker_name) VALUES (%s, %s)"
        insert_council_values = list(set([(i[0],i[13]) for i in lines]))
        for na in insert_council_values:
            if na.count("") != len(na):
                cur.execute(insert_council, na)
        
        #service_unit
        # 0, 2, 1
        cur.execute("DELETE FROM Z1974769.service_unit")
        insert_service_unit = "INSERT INTO Z1974769.service_unit(council_name, service_unit_number, service_unit_name) VALUES (%s, %s, %s)"
        insert_service_unit_values = list(set([(i[0],i[2], i[1]) for i in lines]))
        for na in insert_service_unit_values:
            if na.count("") != len(na):
                cur.execute(insert_service_unit, na)

        #inserting into troop
        #0, 2, 3
        cur.execute("DELETE FROM Z1974769.troop")
        insert_troop = "INSERT INTO Z1974769.troop(council_name, service_number, troop_number) VALUES (%s, %s, %s)"
        insert_troop_values = list(set([(i[0],i[2],i[3]) for i in lines]))
        for na in insert_troop_values:
            if na.count("") != len(na):
                cur.execute(insert_troop, na)
        

        #inserting into offers
        #11, 13
        cur.execute("DELETE FROM Z1974769.offers")
        insert_offers = "INSERT INTO Z1974769.offers(cookie_name, baker_name) VALUES(%s, %s)"
        insert_offers_values = list(set([(i[11], i[13]) for i in lines]))
        for na in insert_offers_values:
            if na.count("") != len(na):
                cur.execute(insert_offers, na)


        # kept on hold not anymore
        #inserting into sells_for
        #0, 11, 12
        cur.execute("DELETE FROM Z1974769.sells_for")
        insert_sells_for = "INSERT INTO Z1974769.sells_for(council_name,cookie_name,cookie_box_price) VALUES(%s, %s, %s)"
        insert_sells_for_values = list(set([(i[0],i[11],i[12]) for i in lines]))
        li = []
        for i in insert_sells_for_values:
            for l in range(len(i)):
                if i[l] == "":
                    i = list(i)
                    i[l] = 0
                    i = tuple(i)
            if (i[0], i[1]) in li:
                pass
            else:
                li.append((i[0], i[1]))
                print(i)
                cur.execute(insert_sells_for, i)



        # #inserting into belong_to
        # #6, 7, 0, 2, 3
        # cur.execute("DELETE FROM Z1974769.belongs_to")
        # insert_belongs_to = "INSERT INTO Z1974769.belongs_to(girl_name, girl_address, troop_council_name, troop_service_number, troop_number) VALUES(%s,%s,%s,%s,%s)"
        # insert_belong_to_values = list(set([(i[6],i[7], i[0], i[2], i[3], i[4], i[5]) for i in lines]))
        # for na in insert_belong_to_values:
        #     if na.count(""):
        #         pass
        #     else:
        #         print(na)
        #         cur.execute(insert_belongs_to, na)

        #inserting into individual_sales
        #11, 6, 7, 9, 10, 16, 15
        cur.execute("DELETE FROM Z1974769.individual_sales")
        insert_individual_sales = "INSERT INTO Z1974769.individual_sales(cookie_name, girl_name, girl_address, customer_name, customer_address, Date, quantity) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        insert_individual_sales_values = list(set([(i[11], i[6], i[7], i[9], i[10], i[16], i[15]) for i in lines]))
        for na in insert_individual_sales_values:
            if na[0:len(na)-1].count(""):
                pass
            else:
                print(na)
                cur.execute(insert_individual_sales, na)
        
        #inserting into shop sales
        #11, 0, 2, 3, 16, 15
        cur.execute("DELETE FROM Z1974769.shop_sales")
        insert_shop_sales = "INSERT INTO Z1974769.shop_sales(cookie_name, troop_council_name, troop_service_number, troop_number, Date, quantity) VALUES (%s, %s, %s, %s, %s, %s)"
        insert_shop_sales_values = []
        for i in lines:
            x = (i[6], i[7], i[9], i[10])
            if x.count("") == len(x):
                y = (i[11], i[0], i[2], i[3], i[16], i[15])
                insert_shop_sales_values.append(y)
        for na in set(insert_shop_sales_values):
            if na[0:len(na)-1].count(""):
                pass
            else:
                print(na)
                cur.execute(insert_shop_sales, na)



        

        
        

        conn.commit()
    except FileExistsError as err:
        print(err)
    finally:
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()


