from sqlalchemy import create_engine


class DBConstants:
    Successful_update = 'UPDATE 1'
    Successful_delete = 'DELETE 1'
    DB_products = ['postgresql://postgres:ds5673@35.193.117.225:5432/product_database',
                   'postgresql://postgres:ds5673@34.69.156.181:5432/product_database',
                   'postgresql://postgres:ds5673@34.69.171.76:5432/product_database']
    DB_customers = ['postgresql://postgres:ds5673@35.232.192.83:5432/customer_database',
                    'postgresql://postgres:ds5673@34.123.110.112:5432/customer_database',
                    'postgresql://postgres:ds5673@35.188.148.198:5432/customer_database']


class ABP_servers:
    hosts = [('10.180.0.3', 5005), ('10.180.0.10', 5006), ('10.180.0.11', 5007)]
    raft_servers = [('10.180.0.3', 5010), ('10.180.0.10', 5010), ('10.180.0.11', 5010)]
    total_hosts = len(hosts)


class Request_Constants:
    context = 'origin_server'
    retransmit_context = 'origin_retransmit'


current_host_number = 0


def init_current_server_number(num):
    global current_host_number
    current_host_number = num


def get_current_server_number():
    return current_host_number


from pysyncobj import SyncObj, replicated_sync
sql_alchemy_obj = None

def init_sql_alchemy_obj():
    global sql_alchemy_obj
    sql_alchemy_obj = create_engine(DBConstants.DB_products[get_current_server_number()])


class RaftSeller(SyncObj):
    def __init__(self):
        super(RaftSeller, self).__init__(
            ABP_servers.raft_servers[get_current_server_number()][0] + ':' + str(ABP_servers.raft_servers[get_current_server_number()][1]),
            [host[0] + ':' + str(host[1]) for host in ABP_servers.raft_servers if host !=
             ABP_servers.raft_servers[get_current_server_number()]])
        print("Raft buyer constructor :- ", ABP_servers.raft_servers[get_current_server_number()][0] + ':' + str(ABP_servers.raft_servers[get_current_server_number()][1]))
        print(sql_alchemy_obj)

    def update_item_func(self, key, val, item_id, seller_id):
        from models.items import Items
        # asyncio.run_coroutine_threadsafe(Items.update.values(quantity=diff).where(Items.id == item_id).gino.status(),
        #                                  self.loop)
        with sql_alchemy_obj.connect() as con:
            val = con.execute('Update {} set {}={} where id={} and seller_id={}'.format(Items.__tablename__, key, val,
                                                                                                item_id, seller_id))
            return val.rowcount

    def add_item_func(self, item_id, request):
        from models.items import Items
        keyword_string = '{' + ','.join(request.keywords) + '}'
        with sql_alchemy_obj.connect() as con:
            con.execute(
                "Insert into {} (id , name, category, condition, keywords, sale_price, quantity, seller_id) values ({}, "
                "'{}', {}, {}, '{}', {}, {}, {})".format(Items.__tablename__,
                                                         item_id,
                                                         request.name,
                                                         request.category,
                                                         request.condition,
                                                         keyword_string,
                                                         request.sale_price,
                                                         request.quantity,
                                                         request.seller_id))

    @replicated_sync
    def update_item(self, sale_price, item_id, seller_id):
        print("Raft, updating item :- ", item_id)
        # asyncio.create_task(Items.update.values(quantity=diff).where(Items.id == item_id).gino.status())
        change_val = self.update_item_func('sale_price', sale_price, item_id, seller_id)
        print(change_val)
        return change_val
        # loop.create_task(Items.update.values(quantity=diff).where(Items.id == item_id).gino.status())
        # await Items.update.values(quantity=diff).where(Items.id == item_id).gino.status()

    @replicated_sync
    def add_item(self, item_id, request):
        self.add_item_func(item_id=item_id, request=request)

    @replicated_sync
    def remove_item(self, item_id, seller_id, diff):
        print("inside raft function :- ", item_id, seller_id, diff)
        if diff > 0:
            self.update_item_func('quantity', diff, item_id, seller_id)
        elif diff == 0:
            self.update_item_func('quantity', 0, item_id, seller_id)


raft_seller = None
def init_raft_seller():
    global raft_seller
    raft_seller = RaftSeller()


def get_raft_seller():
    return raft_seller



################################### related to Atomic broadcast protocol

import socket

global_sequence_number = 0
local_sequence_number = -1
UDP_PORT = 5020
sequence_messsages = {}
request_messages = {}
sock = None
UDP_IP = "0.0.0.0"


def init_sock():
    global sock
    sock = socket.socket(socket.AF_INET,
                         socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    sock.settimeout(2)


def init_udp_port(port):
    global UDP_PORT
    UDP_PORT = port


def get_current_server_udp_port():
    return UDP_PORT


def get_sequence_number(sequence_type):
    if sequence_type == 'global':
        return global_sequence_number
    else:
        return local_sequence_number


def incr_sequence_number(sequence_type):
    if sequence_type == 'global':
        global global_sequence_number
        global_sequence_number += 1
    else:
        global local_sequence_number
        local_sequence_number += 1


def set_global_sequence_number(num):
    global global_sequence_number
    global_sequence_number = max(global_sequence_number, num)


def insert_into_sequence_messages(key, value):
    global sequence_messsages
    sequence_messsages[key] = value
    print("sequence messages :- ", sequence_messsages)
    print('--------------------------------------------------')


def insert_into_request_messages(key, value):
    global request_messages
    request_messages[key] = value
    print("request messages :- ", request_messages)
    print('--------------------------------------------------')


def get_messages_dict(message_type):
    if message_type == 'global':
        return sequence_messsages
    else:
        return request_messages
