import buyer_pb2_grpc, buyer_pb2
import grpc, sys, asyncio
import random, pickle
from config import DBConstants
from asyncpg.exceptions import UniqueViolationError
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy import and_
from config import ABP_servers, Request_Constants, get_sequence_number, incr_sequence_number, get_current_server_number, \
    insert_into_sequence_messages, insert_into_request_messages, get_current_server_udp_port, get_messages_dict, \
    clear_dict, get_raft_buyer


class BuyerMasterServicer(buyer_pb2_grpc.BuyerMasterServicer):
    """Implements ItemMaster protobuf service interface."""
    def print_request(self, request, context):
        print("request :", end=" ")
        print((request, context, type(request)))
        print("-------------------")

    @staticmethod
    def generate_rand_id():
        return random.randint(1, pow(2, 63) - 1)

    @staticmethod
    def row2dict(row):
        d = {}
        for column in row.__table__.columns:
            d[column.name] = str(getattr(row, column.name))

        return d

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
                random_id = BuyerMasterServicer.generate_rand_id()
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
                    BuyerMasterServicer.send_retransmit_request(server_number, i)
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

    async def SearchItemCart(self, request, context):
        """search items from the database.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart
        from models.items import Items
        from database import db_product as db
        self.print_request(request, context)
        keywords = request.keywords
        item_dict = {}
        for keyword in keywords:
            items = await db.status(db.text
                                    ('select * from items where category = :category and :keyword = any(keywords)'),
                                    {'category': request.category, 'keyword': keyword})
            print(items)
            for item in items[1]:
                # id is at index 2
                new_item = (item[0], item[1], item[2], item[3], item[4], float(item[5]), item[6], item[7])
                print((type(item[0]), type(item[1]), type(item[2]), type(item[3]), type(item[4]), type(item[5]),
                       type(item[6]), type(item[7])))
                item_dict[item[2]] = new_item
        print(item_dict)
        if item_dict:
            search_resp = []
            for key in item_dict:
                item = item_dict[key]
                search_resp.append(buyer_pb2.Item(
                    name=item[0], category=item[1], id = item[2],
                    condition=item[3], keywords=item[4],
                    sale_price=item[5], quantity=item[6],
                    seller_id=item[7]))
            return buyer_pb2.SearchItemCartResponse(items=search_resp)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('No items found for this query')
            return buyer_pb2.SearchItemCartResponse()

    async def AddToCart(self, request, context):
        """Add item to cart.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        request, _, _ = await BuyerMasterServicer.rotating_sequencer_ABP(request=request, context=context, method_name='AddToCart')
        from models.buyer_cart import BuyerCart
        from models.items import Items
        self.print_request(request, context)
        item = await Items.query.where(Items.id == request.item_id).gino.first()
        print(item)
        if item is None or item.quantity < request.quantity:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            if item is None:
                context.set_details('Item does not exist')
            else:
                context.set_details('Not enough Item quantity present')
            return buyer_pb2.AddToCartResponse()
        
        buyer_cart_exist = await BuyerCart.query.where(and_(BuyerCart.item_id == request.item_id,
                                                            BuyerCart.buyer_id == request.buyer_id,
                                                            BuyerCart.checked_out == False)).gino.first()
        print(buyer_cart_exist)
        if buyer_cart_exist:
            add_q = buyer_cart_exist.quantity + request.quantity
            if add_q > item.quantity:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Not enough Item quantity present - already item in cart')
                return buyer_pb2.AddToCartResponse()

            buyer_cart = await buyer_cart_exist.update(quantity=add_q).apply()
        else:
            buyer_cart = await BuyerCart.create(buyer_id=request.buyer_id, 
                                                item_id=request.item_id, 
                                                quantity=request.quantity)
        print(buyer_cart)
        if buyer_cart:
            return buyer_pb2.AddToCartResponse(buyer_id=request.buyer_id)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Add to cart unsuccessful')
            return buyer_pb2.AddToCartResponse()

    async def RemoveItemFromShoppingCart(self, request, context):
        """Remove item from buyer cart.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart
        self.print_request(request, context)
        request, _, _ = await BuyerMasterServicer.rotating_sequencer_ABP(request=request, context=context,
                                                                   method_name='RemoveItemFromShoppingCart')
        buyer_cart_exist = await BuyerCart.query.where(and_(BuyerCart.item_id == request.item_id,
                                                            BuyerCart.buyer_id == request.buyer_id,
                                                            BuyerCart.checked_out == False)).gino.first()
        if buyer_cart_exist:
            diff = buyer_cart_exist.quantity - request.quantity
            if buyer_cart_exist.quantity > request.quantity:
                await buyer_cart_exist.update(quantity=diff).apply()
            elif buyer_cart_exist.quantity == request.quantity:
                await buyer_cart_exist.delete()
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Not enough Item present in buyer cart')
                return buyer_pb2.RemoveItemFromShoppingCartResponse()
            return buyer_pb2.RemoveItemFromShoppingCartResponse(buyer_id=request.buyer_id)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Item not present in buyer cart')
            return buyer_pb2.RemoveItemFromShoppingCartResponse()

    async def ClearCart(self, request, context):
        """Clear buyer cart.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart
        self.print_request(request, context)
        request, _, _ = await BuyerMasterServicer.rotating_sequencer_ABP(request=request, context=context,
                                                                   method_name='ClearCart')
        try:
            await BuyerCart.delete.where(and_(BuyerCart.buyer_id == request.buyer_id,
                                              BuyerCart.checked_out == False)).gino.status()
            return buyer_pb2.ClearCartResponse(buyer_id=request.buyer_id)
        except Exception:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Buyer cart removal unsuccessful')
            return buyer_pb2.ClearCartResponse()

    async def DisplayCart(self, request, context):
        """Display items from cart.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart
        from models.items import Items
        self.print_request(request, context)
        buyer_cart = await BuyerCart.query.where(and_(BuyerCart.buyer_id == request.buyer_id,
                                                      BuyerCart.checked_out == False)).gino.all()
        if buyer_cart is not None:
            display_resp = []
            for cart_item in buyer_cart:
                ts = Timestamp()
                ts.FromDatetime(cart_item.updated_at)
                item = await Items.query.where(Items.id == cart_item.item_id).gino.first()
                display_resp.append(buyer_pb2.DisplayItemsInCart(
                    buyer_id=cart_item.buyer_id, item_id=cart_item.item_id, 
                    quantity=cart_item.quantity, name=item.name, category=item.category,
                    condition=item.condition, keywords=item.keywords,
                    sale_price=item.sale_price, seller_id=item.seller_id, 
                    updated_at=ts))
            return buyer_pb2.DisplayCartResponse(items=display_resp)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('No cart items found for this buyer')
            return buyer_pb2.DisplayCartResponse()

    async def CreateAccount(self, request, context):
        """Create account.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyers import Buyers

        self.print_request(request, context)
        request, buyer_id, _ = await BuyerMasterServicer.rotating_sequencer_ABP(request=request, context=context,
                                                                             method_name='CreateAccount',
                                                                             string_id='buyer_id')

        try:
            buyer = await Buyers.create(id=buyer_id, name=request.name,
                                        user_name=request.user_name, password=request.password)
            if buyer:
                return buyer_pb2.CreateAccountResponse(buyer_id=buyer_id)
            else:
                if context != Request_Constants.context:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details('Account creation unsuccessful')
                return buyer_pb2.CreateAccountResponse()
        except Exception as e:
            tb = sys.exc_info()[2]
            print(e.with_traceback(tb))
            if context != Request_Constants.context:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Account creation unsuccessful - Buyer with same id exists :- ' + str(buyer_id))
            return buyer_pb2.CreateAccountResponse()

    async def Login(self, request, context):
        """Login with username.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyers import Buyers
        self.print_request(request, context)
        buyer = await Buyers.query.where(and_(Buyers.user_name == request.user_name,
                                              Buyers.password == request.password)).gino.first()
        if buyer:
            return buyer_pb2.LoginResponse(buyer_id=buyer.id)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Buyer not found')
            return buyer_pb2.LoginResponse()

    async def MakePurchase(self, request, context):
        """Make purchase of all items in cart.
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart, review_type
        from models.items import Items
        from models.sellers import Sellers

        # check from SOAP call that credit card payment is success
        # send name, number, exp
        from zeep import Client
        from config import Financial_transactions

        request, _, is_raft_execute = await BuyerMasterServicer.rotating_sequencer_ABP(request=request, context=context,
                                                                                       method_name='MakePurchase')

        # client = Client(Financial_transactions.host_and_port_wsdl)
        # result = client.service.pay_using_card(request.card_name, request.card_number, request.card_expiry)
        # result = result[0]
        # print("card result is :- ", result)

        # to remove code
        result = Financial_transactions.success

        if result == Financial_transactions.failure:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('card related transaction was unsuccessful.')
            return buyer_pb2.MakePurchaseResponse(buyer_id=request.buyer_id, transaction_status=False)

        self.print_request(request, context)
        buyer_cart = await BuyerCart.query.where(and_(BuyerCart.buyer_id == request.buyer_id,
                                                      BuyerCart.checked_out == False)).gino.all()
        if buyer_cart:
            # get item entry for each item in cart
            # check quantity, proceed only if enough quantity available in Item db
            # make sufficient item quantity as purchased
            # update seller num_items_sold count
            # remove insufficient items from cart
            for cart_item in buyer_cart:
                item = await Items.query.where(Items.id == cart_item.item_id).gino.first()
                seller = await Sellers.query.where(Sellers.id == item.seller_id).gino.first()
                if item and item.quantity >= cart_item.quantity:
                    diff = item.quantity - cart_item.quantity
                    print("item_id, quantities :- ", (item.id, item.quantity, cart_item.quantity))
                    print("starting to execute Raft is_raft_execute :- ")
                    if is_raft_execute:
                        print("executing Raft for item id :- ", cart_item.item_id)
                        print("------------------------------------------------------------------")
                        get_raft_buyer().update_item(cart_item.item_id, diff)
                    # await item.update(quantity=diff).apply()

                    buyer_cart_purchased = await BuyerCart.query.where(and_(BuyerCart.buyer_id == request.buyer_id,
                                                                            BuyerCart.item_id == cart_item.item_id,
                                                                            BuyerCart.checked_out == True)).gino.first()
                    if buyer_cart_purchased != None:
                        await cart_item.update(seller_review=buyer_cart_purchased.seller_review.name).apply()
                    await cart_item.update(checked_out=True).apply()
                    await seller.update(num_items_sold=seller.num_items_sold+cart_item.quantity).apply()
                else:
                    await cart_item.delete()

            return buyer_pb2.MakePurchaseResponse(buyer_id=request.buyer_id, transaction_status=True)
        else:
            if context != Request_Constants.context:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Cart is empty.')
            return buyer_pb2.MakePurchaseResponse()          

    async def ProvideFeedback(self, request, context):
        """Provide feedback for all checkout items
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart, review_type
        from models.sellers import Sellers
        from models.items import Items
        from database import db_customer as db
        self.print_request(request, context)
        request, _, _ = await BuyerMasterServicer.rotating_sequencer_ABP(request=request, context=context,
                                                                   method_name='ProvideFeedback')
        buyer_carts = await BuyerCart.query.where(and_(BuyerCart.item_id == request.item_id,
                                                       BuyerCart.buyer_id == request.buyer_id,
                                                       BuyerCart.checked_out == True)).gino.all()
        # BuyerCart.seller_review == review_type.NA.name

        print(buyer_carts)
        if buyer_carts:
            if buyer_carts[0].seller_review.name == 'NA':
                item = await Items.query.where(Items.id == request.item_id).gino.first()
                if item is None:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details('Feedback update failed - item not available')
                    return buyer_pb2.ProvideFeedbackResponse()
                seller = await Sellers.query.where(Sellers.id == item.seller_id).gino.first()
                if seller is None:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details('Feedback update failed - seller not available')
                    return buyer_pb2.ProvideFeedbackResponse()
                if request.feedback == True:
                    await seller.update(feedback=(seller.feedback[0] + 1, seller.feedback[1])).apply()
                else:
                    await seller.update(feedback=(seller.feedback[0], seller.feedback[1] + 1)).apply()
                for buyer_cart in buyer_carts:
                    if request.feedback == True:
                        await buyer_cart.update(seller_review=review_type.UP.name).apply()
                    else:
                        await buyer_cart.update(seller_review=review_type.DOWN.name).apply()
                return buyer_pb2.ProvideFeedbackResponse(buyer_id=request.buyer_id)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Item review already present.')
                return buyer_pb2.ProvideFeedbackResponse()
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('No item in buyer cart')
            return buyer_pb2.ProvideFeedbackResponse()

    async def GetSellerRating(self, request, context):
        """Get seller rating
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.sellers import Sellers
        self.print_request(request, context)
        seller = await Sellers.query.where(Sellers.id == request.seller_id).gino.first()
        if seller:
            rating = seller.feedback[0] - seller.feedback[1]
            return buyer_pb2.GetSellerRatingResponse(seller_rating=rating)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Seller rating not found')
            return buyer_pb2.GetSellerRatingResponse()

    async def GetBuyerHistory(self, request, context):
        """Get Buyer History
        Args:
            request: The request value for the RPC.
            context (grpc.ServicerContext)
        """
        from models.buyer_cart import BuyerCart
        from models.items import Items
        self.print_request(request, context)
        buyer_cart = await BuyerCart.query.where(and_(BuyerCart.buyer_id == request.buyer_id,
                                                      BuyerCart.checked_out == True)).gino.all()
        if buyer_cart:
            display_resp = []
            for cart_item in buyer_cart:
                ts = Timestamp()
                ts.FromDatetime(cart_item.updated_at)
                item = await Items.query.where(Items.id == cart_item.item_id).gino.first()
                display_resp.append(buyer_pb2.PurchaseHistory(
                    buyer_id=cart_item.buyer_id, item_id=cart_item.item_id, 
                    quantity=cart_item.quantity, name=item.name, category=item.category,
                    condition=item.condition, keywords=item.keywords,
                    sale_price=item.sale_price, seller_id=item.seller_id, 
                    seller_review=cart_item.seller_review.name, updated_at=ts))
            return buyer_pb2.GetBuyerHistoryResponse(items=display_resp)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Buyer history is empty')
            return buyer_pb2.GetBuyerHistoryResponse()



