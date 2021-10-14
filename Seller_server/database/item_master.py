import items_pb2_grpc, items_pb2
import grpc
import random, pickle, asyncio
from config import DBConstants
from asyncpg.exceptions import UniqueViolationError
from sqlalchemy import and_
from config import get_raft_seller, ABP_servers, Request_Constants, get_messages_dict, incr_sequence_number, \
    get_sequence_number, get_current_server_number, insert_into_request_messages, insert_into_sequence_messages, \
    get_current_server_udp_port


class ItemMasterServicer(items_pb2_grpc.ItemMasterServicer):
    """Implements ItemMaster protobuf service interface."""
    def print_request(self, request, context):
        print("request :", end=" ")
        print((request, context, type(request)))
        print("-------------------")

    @staticmethod
    def generate_rand_id():
        return random.randint(1, pow(2, 63) - 1)

    @staticmethod
    def send_retransmit_request(server_number, seq_msg_number):
        host_port = ABP_servers.hosts[server_number]
        from config import sock
        send_message = {"sequence_message_number": seq_msg_number, 'message_type': 'retransmit'}
        send_message = pickle.dumps(send_message)
        print("retransmitting for sequence number :- ", seq_msg_number)
        print("------------------------------------------------------")
        sock.sendto(send_message, host_port)

    @staticmethod
    async def rotating_sequencer_ABP(request, context, method_name, string_id=None):
        if context == Request_Constants.retransmit_context:
            random_id = request.get(string_id)
            request = request['request']
            is_raft_execute = False
            return request, random_id, is_raft_execute

        from config import sock

        random_id = None
        request_messages_dict = get_messages_dict('local')
        sequence_messages_dict = get_messages_dict('global')

        if context != Request_Constants.context:
            # print("Sending request message.......", context, Request_Constants.context)
            incr_sequence_number('local')
            # print("request_message is :- ", {'sid': get_current_server_number(),
            #                              'seq': get_sequence_number('local'),
            #                              "request": request,
            #                              "method_name": 'CreateAccount',
            #                              'message_type': 'request_msg'})
            #
            # print('--------------------------------------------------')
            send_message = {'sid': get_current_server_number(),
                            'seq': get_sequence_number('local'),
                            "request": request,
                            "method_name": method_name,
                            'message_type': 'request_msg'}

            request_message_val = {"request": request,
                                   "method_name": method_name,
                                   'message_type': 'request_msg'}

            if string_id is not None:
                random_id = ItemMasterServicer.generate_rand_id()
                send_message[string_id] = random_id
                request_message_val[string_id] = random_id

            insert_into_request_messages((get_current_server_number(), get_sequence_number('local')),
                                         request_message_val)

            send_message = pickle.dumps(send_message)
            for host in ABP_servers.hosts:
                if host[1] != get_current_server_udp_port():
                    sock.sendto(send_message, host)
        else:
            insert_into_request_messages((request.get('sid'), request.get('seq')),
                                         {"request": request.get('request'),
                                          string_id: request.get(string_id),
                                          "method_name": request.get('method_name'),
                                          'message_type': request.get('message_type')})

        # to fetch sid, seq, request from the server who broadcasted the request, in case the current server
        # is the server receiving client request, we take current server's sid and seq
        if context == Request_Constants.context:
            sid = request['sid']
            seq = request['seq']
            client_req = request['request']
        else:
            sid = get_current_server_number()
            seq = get_sequence_number('local')
            client_req = request

        if get_sequence_number('global') % ABP_servers.total_hosts == get_current_server_number():
            # clear_dict('global')
            curr_global_sequence_no = get_sequence_number('global')

            # condition 3.1
            for i in range(curr_global_sequence_no):
                if i in sequence_messages_dict:
                    if sequence_messages_dict[i][0] in request_messages_dict:
                        continue
                server_number = i % ABP_servers.total_hosts
                # poll into dict and call the retransmit function until it comes in the dictionary
                while i not in sequence_messages_dict:
                    ItemMasterServicer.send_retransmit_request(server_number, i)
                    print("Sleep, Sequence number missing :- {}, sending retransmit request".format(i))
                    await asyncio.sleep(0.5)

            print("sending sequence number.......", get_current_server_number(), ABP_servers.total_hosts,
                  curr_global_sequence_no)

            # check for condition 3.2
            i = 0
            while i < seq:
                if (sid, i) in request_messages_dict:
                    val = request_messages_dict[(sid, i)]
                    if 'global' in val:
                        i += 1
                        continue
                    print("Sleep, condition 3.2 (sid, seq) :- {}".format((sid, i)))
                    await asyncio.sleep(0.5)

            insert_into_sequence_messages(curr_global_sequence_no, ((sid, seq), 'metadata'))
            request_msg_dict_val = request_messages_dict[(sid, seq)]
            request_msg_dict_val['global'] = curr_global_sequence_no
            # print("changed request_dict_val :- ", request_msg_dict_val)
            # print("change in request_messages_dict :- ", get_messages_dict('local'))

            # client_req is not used currently
            send_message = pickle.dumps({"global": curr_global_sequence_no, "sid": sid,
                                         "seq": seq, "request": client_req, "message_type": 'sequence_msg'})
            for host in ABP_servers.hosts:
                if host[1] != get_current_server_udp_port():
                    sock.sendto(send_message, host)
            incr_sequence_number('global')

        # condition 1.1
        for i in range(get_sequence_number('global')):
            if i in sequence_messages_dict:
                req_key_tuple = sequence_messages_dict[i][0]
                if req_key_tuple in request_messages_dict:
                    if 'global' in request_messages_dict[req_key_tuple]:
                        continue

            print("Sleep, condition 1.1 global seq :- {}".format(i))
            await asyncio.sleep(0.5)

        # to convert the request to actual client request. the condition is to check whether the current server received
        # request from client or UDP request from another server
        is_raft_execute = True
        if context == Request_Constants.context:
            random_id = request.get(string_id)
            request = request['request']
            is_raft_execute = False
            print("request after request and sequence message functions :- ", request)

        # while()

        return request, random_id, is_raft_execute

    async def GetItem(self, request, context):
        """Retrieve a item from the database.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        # item = db.query(models.Item).get(request.id)
        self.print_request(request, context)

        from database import db_product
        item = await db_product.status(db_product.text('select * from items'))
        print(item)
        item = item[1][0]
        if item:
            item_pb = items_pb2.Item(name=item[0], category=item[1], id=item[2], condition=item[3],
                                     keywords=item[4], sale_price=item[5], quantity=item[6], seller_id=item[7])
            return items_pb2.GetItemResponse(item=item_pb)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Item with id %s not found' % request.id)
            return items_pb2.GetItemResponse()

    async def AddItem(self, request, context):
        """Add an item to the database.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        self.print_request(request, context)
        item_id = ItemMasterServicer.generate_rand_id()
        get_raft_seller().add_item(item_id=item_id, request=request.item)
        return items_pb2.AddItemResponse(id=item_id)

    async def ChangeItem(self, request, context):
        """Change item sale price
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.items import Items
        self.print_request(request, context)
        change_val = get_raft_seller().update_item(sale_price=request.sale_price, item_id=request.id,seller_id=request.seller_id)


        # check this because change_val always returns 1 even if the row is not updated
        if change_val == 1:
            return items_pb2.ChangeItemResponse(id=request.id)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Item sale price modification unsuccessful')
            return items_pb2.ChangeItemResponse()

    # if change_val[0] == DBConstants.Successful_update:
        #     return items_pb2.ChangeItemResponse(id=request.id)
        # else:
        #     context.set_code(grpc.StatusCode.NOT_FOUND)
        #     context.set_details('Item sale price modification unsuccessful')
        #     return items_pb2.ChangeItemResponse()

    async def DisplayItem(self, request, context):
        """Display items from the database.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.items import Items
        self.print_request(request, context)
        items = await Items.query.where(Items.seller_id == request.seller_id).gino.all()
        if items is not None:
            display_resp = []
            for item in items:
                display_resp.append(items_pb2.DisplayItem(name=item.name, category=item.category, id=item.id,
                                                          condition=item.condition, keywords=item.keywords,
                                                          sale_price=item.sale_price, quantity=item.quantity,
                                                          seller_id=item.seller_id))
            return items_pb2.DisplayItemResponse(items=display_resp)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('No items found for this seller')
            return items_pb2.DisplayItemResponse()

    async def RemoveItem(self, request, context):
        """Remove item quantity
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.items import Items
        self.print_request(request, context)
        item = await Items.query.where(and_(Items.id == request.id,
                                            Items.seller_id == request.seller_id)).gino.first()
        try:
            diff = item.quantity - request.quantity
            get_raft_seller().remove_item(item_id=request.id, seller_id=request.seller_id, diff=diff)
            return items_pb2.RemoveItemResponse(id=request.id)
        except Exception:
            print("exceptions --------")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Item quantity removal unsuccessful')
            return items_pb2.RemoveItemResponse()

    async def CreateAccount(self, request, context):
        """Remove item quantity
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.sellers import Sellers
        self.print_request(request, context)
        request, seller_id, _ = await ItemMasterServicer.rotating_sequencer_ABP(request=request, context=context,
                                                                                method_name='CreateAccount',
                                                                                string_id='seller_id')
        try:
            seller = await Sellers.create(id=seller_id, name=request.name, user_name=request.user_name,
                                          password=request.password)
            if seller:
                return items_pb2.CreateAccountResponse(seller_id=seller_id)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Account creation unsuccessful')
                return items_pb2.CreateAccountResponse()
        except UniqueViolationError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Account creation unsuccessful - Seller with same username exists')
            return items_pb2.CreateAccountResponse()

    async def Login(self, request, context):
        """Remove item quantity
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.sellers import Sellers
        self.print_request(request, context)
        seller = await Sellers.query.where(and_(Sellers.user_name == request.user_name,
                                                Sellers.password == request.password)).gino.first()
        if seller:
            return items_pb2.LoginResponse(seller_id=seller.id)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Seller not found')
            return items_pb2.LoginResponse()

    async def GetSellerRating(self, request, context):
        """Remove item quantity
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.sellers import Sellers
        self.print_request(request, context)
        seller = await Sellers.query.where(Sellers.id == request.seller_id).gino.first()
        if seller:
            rating = seller.feedback[0] - seller.feedback[1]
            return items_pb2.GetSellerRatingResponse(rating=rating)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Seller rating not found')
            return items_pb2.GetSellerRatingResponse()



