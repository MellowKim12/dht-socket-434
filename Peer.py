import socket # import the socket for communication with the Manager and other peers
import sys # reads commandline argumants 
import threading # runs a background listeners for incoming text 
import time  # implement a delay 


# parameters for the peers, IP address of the manager, the port number the manager is listening to, unique name of the peer, message port 
if len(sys.argv) != 6:
    
    print("Usage: python3 peer.py <manager_ip> <manager_port> <peer_name> <m_port> <p_port>")
    sys.exit(1)

mngr_ipAddrr = sys.argv[1]
mngr_port = int(sys.argv[2])
peerName = sys.argv[3]
msgPort = int(sys.argv[4])
pnPort = sys.argv[5]  



# creating peer's socket
socketP = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP socket and binds it to the message port
socketP.bind(("0.0.0.0", msgPort))                         # this socket will bw used for communication with manager and peers
sucsIDF = None                                             # stores info about peer's successor
sucsIP = None
sucs_msgport = None




# listen for incoming message 
def listenerF():
    


    global sucsIDF, sucsIP, sucs_msgport
    print(f"[PEER] is now Listen on port {msgPort}...")
    # peer continuously listens for incoming messages
    while True:
        data, address = socketP.recvfrom(1024)
        comingmessage = data.decode().split()
        
        #if the message starts with (successor), it updates its successor
        # otherwise it assumes the message is a normal text message
        if not comingmessage:
            continue
        if comingmessage[0] == "successor":
            sucsIDF, sucsIP, sucs_msgport = comingmessage[1:]
            
            print(f"[PEER] successor is updated => {sucsIDF} ({sucsIP}:{sucs_msgport})")
            
        else:
            print(f"[PEER] Massage: {' '.join(comingmessage)}")



# registers with manager 
def assgn_to_manager():
    
    # sends a (register) message to the manager and receives a success or failure response
    msgRegister = f"register {peerName} {mngr_ipAddrr} {msgPort} {pnPort}"
    
    print(f"[PEER] Registering with manager: {msgRegister}")
    
    socketP.sendto(msgRegister.encode(), (mngr_ipAddrr, mngr_port))
    sysRespond, _ = socketP.recvfrom(1024)
    
    print("[REGISTER]", sysRespond.decode())




    # setting up DHT 
def setup_dht():
    
    # sends a (setup-dht) request to the manager 
    setup_msg = "setup-dht"
    print(f"[PEER] Sending: {setup_msg}")
    socketP.sendto(setup_msg.encode(), (mngr_ipAddrr, mngr_port))
    sysRespond, _ = socketP.recvfrom(1024)
    sysRespondDeco = sysRespond.decode()
    print("[SETUP-DHT]", sysRespondDeco)
    # if successful, it requests its successor and if no response is received, it retries
    if sysRespondDeco.startswith("SUCCESS!"):
        

        msgRegister = f"successor requested {peerName}"
        print(f"[PEER] is Requesting successor now: {msgRegister}")
        socketP.sendto(msgRegister.encode(), (mngr_ipAddrr, mngr_port))
        print("[PEER] Sent request-successor, waiting for response...")
        time.sleep(1) 

        if sucsIDF is None:
            
            print("[ERROR] unable to find successor information, trying ...")
            
            socketP.sendto(msgRegister.encode(), (mngr_ipAddrr, mngr_port))


# sends message 
def sendingMessage(message):
   

    global sucsIDF, sucsIP, sucs_msgport
    
    # if the setup-dht is not implemented print this message 
    if not sucsIDF:
        
        print("[ERROR] No successor ready.  setup-dht need to be set first.")
        return
    
    # if the successor is set, it sends the message to the next peer
    dataTransfer = message.encode()
    
    print(f"[PEER] Sending message to {sucsIDF} at {sucsIP}:{sucs_msgport}")
    
    socketP.sendto(dataTransfer, (sucsIP, int(sucs_msgport)))
    
    print(f"[SENT] => {sucsIDF} ({sucsIP}:{sucs_msgport}): {message}")



# main fuction it does the following: registers the peer
# starts a listener thread
# processes user commands (setup-dht, send <message>, quit)
def main():
   
    assgn_to_manager()
    listener_thread = threading.Thread(target=listenerF, daemon=True)
    listener_thread.start()
    

    print("[PEER] Commands: setup-dht, send <messsag>, quit")
    while True:
        


        commandInput = input(">>> ").strip()
        if not commandInput:
            continue
        sets = commandInput.split(maxsplit=1)
        if sets[0] == "setup-dht":
            setup_dht()
            print("[MAIN] data is Returing from setup_dht. allowing user again...")
        elif sets[0] == "send":
            if len(sets) == 2:
                sendingMessage(sets[1])
            else:
                print("Usage: send <message>")
        elif sets[0] == "quit":
            

            print("[PEER] peer is closing...")
            break
        else:
            print("[ERROR] command not recognized")
            

if _name_ == "_main_":
    main()