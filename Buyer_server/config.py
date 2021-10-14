import socket, asyncio
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


class Financial_transactions:
    host_and_port_wsdl = 'http://10.128.0.3:8002/?wsdl'
    success = 'Success'
    failure = 'Failure'

class UDP_constants:
    UDP_IP = "0.0.0.0"


global_sequence_number = 0
local_sequence_number = -1
current_host_number = 0
UDP_PORT = 5005
sequence_messsages = {}
request_messages = {}
raft_buyer = None

class ABP_servers:
    hosts = [('10.180.0.5', 5005), ('10.180.0.13', 5006), ('10.180.0.12', 5007)]
    raft_servers = [('10.180.0.5', 5010), ('10.180.0.13', 5010), ('10.180.0.12', 5010)]
    total_hosts = len(hosts)

class Request_Constants:
    context = 'origin_server'
    retransmit_context = 'origin_retransmit'


sock = None

def init_sock():
    global sock
    sock = socket.socket(socket.AF_INET,
                         socket.SOCK_DGRAM)
    sock.bind((UDP_constants.UDP_IP, UDP_PORT))
    sock.settimeout(2)


def init_current_server_number(num):
    global current_host_number
    current_host_number = num


def get_current_server_number():
    return current_host_number


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


def clear_dict(message_type):
    if message_type == 'global':
        global sequence_messsages
        sequence_messsages = {}
    else:
        global request_messages
        request_messages = {}


from pysyncobj import SyncObj, replicated_sync
sql_alchemy_obj = None

def init_sql_alchemy_obj():
    global sql_alchemy_obj
    sql_alchemy_obj = create_engine(DBConstants.DB_products[get_current_server_number()])


class RaftBuyer(SyncObj):
    def __init__(self):
        super(RaftBuyer, self).__init__(
            ABP_servers.raft_servers[get_current_server_number()][0] + ':' + str(ABP_servers.raft_servers[get_current_server_number()][1]),
            [host[0] + ':' + str(host[1]) for host in ABP_servers.raft_servers if host !=
             ABP_servers.raft_servers[get_current_server_number()]])
        print("Raft buyer constructor :- ", ABP_servers.raft_servers[get_current_server_number()][0] + ':' + str(ABP_servers.raft_servers[get_current_server_number()][1]))
        print(sql_alchemy_obj)

    def update_func(self, item_id, diff):
        from models.items import Items
        # asyncio.run_coroutine_threadsafe(Items.update.values(quantity=diff).where(Items.id == item_id).gino.status(),
        #                                  self.loop)
        with sql_alchemy_obj.connect() as con:
            con.execute('Update {} set quantity={} where id={}'.format(Items.__tablename__, diff, item_id))


    @replicated_sync
    def update_item(self, item_id, diff):
        print("Raft, updating item :- ", item_id)
        # asyncio.create_task(Items.update.values(quantity=diff).where(Items.id == item_id).gino.status())
        self.update_func(item_id, diff)
        # loop.create_task(Items.update.values(quantity=diff).where(Items.id == item_id).gino.status())
        # await Items.update.values(quantity=diff).where(Items.id == item_id).gino.status()


def init_raft_buyer():
    global raft_buyer
    raft_buyer = RaftBuyer()



def get_raft_buyer():
    return raft_buyer
