import logging, pickle
import asyncio
import grpc, random
import buyer_pb2_grpc
import database
from database.buyer_master import BuyerMasterServicer
import socket
import sys
from config import init_current_server_number, init_udp_port, set_global_sequence_number, get_sequence_number, \
    insert_into_sequence_messages, get_current_server_number, insert_into_request_messages, get_messages_dict, \
    init_raft_buyer, init_sql_alchemy_obj, Request_Constants

udp_task_queue = asyncio.Queue()



async def serve(buyer_master_servicer) -> None:
    print("starting server")
    server = grpc.aio.server()
    await database.connect_db(get_current_server_number())
    grpc_port_number = str(sys.argv[3])
    # sock.sendto(b"vasu", ('127.0.0.1', 5006))
    buyer_pb2_grpc.add_BuyerMasterServicer_to_server(servicer=buyer_master_servicer, server=server)
    listen_addr = '0.0.0.0:' + grpc_port_number
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Shuts down the server with 0 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await server.stop(0)


async def listen_on_udp():
    from config import init_sock
    init_udp_port(int(sys.argv[2]))
    init_sock()
    from config import sock
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            request_data = pickle.loads(data)
            if request_data.get('message_type') == 'request_msg':
                # print("this is a request message :- ",  request_data)
                method_name = request_data['method_name']
                await getattr(buyer_master_servicer, method_name)(request=request_data, context=Request_Constants.context)

            elif request_data.get('message_type') == 'sequence_msg':
                # check the sequence message conditions 4 in the google sheet
                # print("this is a sequence message :- ", request_data)
                insert_into_sequence_messages(request_data.get('global'), ((request_data.get('sid'),
                                                                            request_data.get('seq')),
                                                                            'metadata'))
                request_msg_dict_val = get_messages_dict('local')[(request_data.get('sid'), request_data.get('seq'))]
                request_msg_dict_val['global'] = request_data.get('global')
                # print("changed request_dict_val :- ", request_msg_dict_val)
                # print("change in request_messages_dict :- ", get_messages_dict('local'))

                set_global_sequence_number(request_data.get('global') + 1)

            elif request_data.get('message_type') == 'retransmit_resp':
                sequence_message_number = request_data.get('sequence_message_number')
                print("Received retransmit_resp response for sequence number :- ", sequence_message_number)
                seq_msg = request_data.get('seq_msg')
                req_msg = request_data.get('req_msg')
                insert_into_sequence_messages(sequence_message_number, seq_msg[sequence_message_number])
                insert_into_request_messages(seq_msg[sequence_message_number][0], req_msg[seq_msg[sequence_message_number][0]])
                method_name = req_msg.get('method_name')
                await getattr(buyer_master_servicer, method_name)(request=request_data, context=Request_Constants.retransmit_context)

            else:

                sequence_number = request_data.get('sequence_message_number')
                print("processing retransmit request for sequence number :- ", sequence_number)
                seq_msg = get_messages_dict('global')[sequence_number]
                req_msg = get_messages_dict('local')[seq_msg[0]]
                seq_msg_dict = {sequence_number: seq_msg}
                req_msg_dict = {seq_msg[0]: req_msg}
                print("seq_msg_dict is :- ", seq_msg_dict)
                print("-------------------------------------------------------------")
                print("req_msg_dict is :- ", req_msg_dict)
                print("-------------------------------------------------------------")

                send_message = {'seq_msg': seq_msg_dict, 'req_msg': req_msg_dict, 'sequence_message_number': sequence_number,
                                'message_type': 'retransmit_resp'}
                send_message = pickle.dumps(send_message)
                sock.sendto(send_message, addr)

        except socket.timeout:
            print("", end="")
        await asyncio.sleep(1)


def schedule_coroutine(target, loop=None):
    """Schedules target coroutine in the given event loop

    If not given, *loop* defaults to the current thread's event loop

    Returns the scheduled task.
    """
    if asyncio.iscoroutine(target):
        return asyncio.ensure_future(target, loop=loop)
    raise TypeError("target must be a coroutine, "
                    "not {!r}".format(type(target)))


if __name__ == '__main__':
    buyer_master_servicer = BuyerMasterServicer()
    init_current_server_number(int(sys.argv[1]))
    init_sql_alchemy_obj()

    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    init_raft_buyer()

    loop.create_task(serve(buyer_master_servicer))
    loop.create_task(listen_on_udp())
    loop.run_forever()
