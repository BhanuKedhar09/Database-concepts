import psycopg2

# hostname = 'courses'
# database = 'z1974769'
# username = 'z1974769'
# password = '1999Sep19'
# port_id = 5432

hostname = 'localhost'
database = 'testingforpython'
username = 'bhanukedhar'
password = 'bhanukedhar'
port_id = 5432

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

    drop_schema = '''DROP SCHEMA Z1974769 CASCADE'''
    cur.execute(drop_schema)

    create_schema = '''CREATE SCHEMA IF NOT EXISTS Z1974769'''
    
    cur.execute(create_schema)
    
    # create_sampletable = '''CREATE TABLE IF NOT EXISTS Z1974769.sample (
    #                         ID int,
    #                         name varchar(50),
    #                         primary key(ID)
    # )'''
    # cur.execute(create_sampletable)

    create_cookie = '''CREATE TABLE IF NOT EXISTS Z1974769.cookie (
                        cookie_name varchar(50),
                        primary key(cookie_name)
    )'''
    cur.execute(create_cookie)

    create_baker = '''CREATE TABLE IF NOT EXISTS Z1974769.baker(
                        baker_name varchar(50),
                        address varchar(100),
                        primary key (baker_name)
    )'''
    cur.execute(create_baker)

    create_council = '''CREATE TABLE IF NOT EXISTS Z1974769.council(
                        council_name varchar(50),
                        baker_name varchar(20),
                        primary key (council_name),
                        foreign key(baker_name) references z1974769.baker(baker_name)
    )'''
    cur.execute(create_council)

    create_service_unit = '''CREATE TABLE IF NOT EXISTS Z1974769.service_unit(
                            council_name varchar(50),
                            service_unit_number int,
                            service_unit_name varchar(50),
                            primary key(council_name, service_unit_number),
                            foreign key(council_name) references Z1974769.council(council_name)
    )'''
    cur.execute(create_service_unit)

    create_troop = '''CREATE TABLE IF NOT EXISTS Z1974769.troop(
                        council_name varchar(50),
                        service_number int,
                        troop_number int,
                        primary key(council_name, service_number, troop_number),
                        foreign key(council_name, service_number) references Z1974769.service_unit(council_name, service_unit_number)
    )'''
    cur.execute(create_troop)

    create_girl = '''CREATE TABLE IF NOT EXISTS Z1974769.girl(
                        girl_name varchar(50),
                        address varchar(100),
                        girl_rank varchar(50),
                        primary key(girl_name, address)
    )'''
    cur.execute(create_girl)

    create_leader = '''CREATE TABLE IF NOT EXISTS Z1974769.leader(
                        leader_name varchar(50),
                        address varchar(100),
                        primary key(leader_name, address)
    )'''
    cur.execute(create_leader)

    create_customer = '''CREATE TABLE IF NOT EXISTS Z1974769.customer(
                        customer_name varchar(50),
                        address varchar(100),
                        primary key(customer_name, address)
    )'''
    cur.execute(create_customer)

    create_offers = '''CREATE TABLE IF NOT EXISTS Z1974769.offers(
                        cookie_name varchar(50),
                        baker_name varchar(50),
                        primary key(cookie_name, baker_name),
                        foreign key(cookie_name) references Z1974769.cookie(cookie_name),
                        foreign key(baker_name) references Z1974769.baker(baker_name) 
    )''' 
    cur.execute(create_offers)

    create_sells_for = '''CREATE TABLE IF NOT EXISTS Z1974769.sells_for(
                        council_name varchar(50),
                        cookie_name varchar(50),
                        cookie_box_price float,
                        primary key(council_name, cookie_name),
                        foreign key(council_name) references Z1974769.council(council_name),
                        foreign key(cookie_name) references Z1974769.cookie(cookie_name)
    )'''
    cur.execute(create_sells_for)

    create_belongs_to = '''CREATE TABLE IF NOT EXISTS Z1974769.belongs_to(
                            girl_name varchar(50),
                            girl_address varchar(100),
                            troop_council_name varchar(50),
                            troop_service_number int,
                            troop_number int,
                            leader_name varchar(50),
                            leader_address varchar(100),
                            primary key(girl_name,girl_address,troop_council_name,troop_service_number,troop_number,leader_name,leader_address),
                            foreign key(girl_name, girl_address) references Z1974769.girl(girl_name, address),
                            foreign key(troop_council_name,troop_service_number,troop_number) references Z1974769.troop(council_name,service_number,troop_number),
                            foreign key(leader_name, leader_address) references Z1974769.leader(leader_name, address)
    )'''
    cur.execute(create_belongs_to)

    create_shop_sales = '''CREATE TABLE IF NOT EXISTS Z1974769.shop_sales(
                                cookie_name varchar(50),
                                troop_council_name varchar(50),
                                troop_service_number int,
                                troop_number int,
                                Date date,
                                quantity int,
                                primary key(cookie_name, troop_council_name, troop_service_number, troop_number, Date),
                                foreign key(cookie_name) references Z1974769.cookie(cookie_name),
                                foreign key(troop_council_name, troop_service_number, troop_number) references Z1974769.troop(council_name,service_number,troop_number)

    )'''
    cur.execute(create_shop_sales)

    create_individual_sales = '''CREATE TABLE IF NOT EXISTS Z1974769.individual_sales(
                                cookie_name varchar(50),
                                girl_name varchar(50),
                                girl_address varchar(100),
                                customer_name varchar(50),
                                customer_address varchar(100),
                                Date date,
                                quantity int,
                                primary key(cookie_name, girl_name, girl_address, customer_name, customer_address, Date),
                                foreign key(cookie_name) references Z1974769.cookie(cookie_name),
                                foreign key(girl_name, girl_address) references Z1974769.girl(girl_name, address),
                                foreign key(customer_name, customer_address) references Z1974769.customer(customer_name, address)

    )'''
    cur.execute(create_individual_sales)



    # cur.execute(drop_schema)
    

    conn.commit()
except Exception as err:
    print(err, "printing error")
finally:
    if conn is not None:
        conn.close()
    if cur is not None:
        cur.close()
