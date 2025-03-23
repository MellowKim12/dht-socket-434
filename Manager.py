import socket # import the socket for communication with the Manager and other peers



# manager oversees all peers, assigns successors, and maintains the DHT structure
# staring the manager 
def managerToStart(selectedPort):
    
    # initializes peer and successor storage
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", selectedPort))
    
    print(f"[MANAGER] is now Starting on port {selectedPort}, waiting for peers to join...")
    sysPeer = {}        
    sysSuccessors = {}    

    while True:
        

        data, address = sock.recvfrom(1024)
        comingmessage = data.decode().split()
        
        if not comingmessage:
            
            continue

        command = comingmessage[0]
        
        # registers peers 
        # storing their IP, ports, and name for the peers
        if command == "register":
           
            peerName, ip_address, msgPort, pnPort = comingmessage[1:]
            if peerName in sysPeer:
                sysrespond = "Error: The Peer already exist"
            else:
                
                sysPeer[peerName] = (ip_address, msgPort, pnPort)
                sysrespond = "SUCCESS: Peer registered"
                print(f"[MANAGER] {peerName} registered successfully: IP={ip_address}, m_port={msgPort}, p_port={pnPort}")
                
            sock.sendto(sysrespond.encode(), address)
            # setting up DHT 
        elif command == "setup-dht":
            
            print("[MANAGER]  setup-dht request has been recived")
            if len(sysPeer) < 3:
                sysrespond = "Error:the number of peers not enough to setup DHT"
            else: # implement a circular ring of peers
                
                listTabl = list(sysPeer.keys())
                sysSuccessors.clear()
             
                for i in range(len(listTabl)):
                    

                    currentSucc = listTabl[i]
                    successorRec = listTabl[(i + 1) % len(listTabl)]
                    sysSuccessors[currentSucc] = successorRec
                    print(f"[MANAGER] DHT ring: {currentSucc} -> {successorRec}")

               
                for all_peers in sysPeer:
                    

                    succssIdf = sysSuccessors[all_peers]
                    ip_address, msgPort, _ = sysPeer[succssIdf]
                    sysRespnd = f"successor {succssIdf} {ip_address} {msgPort}"
                    sock.sendto(sysRespnd.encode(), (sysPeer[all_peers][0], int(sysPeer[all_peers][1])))
                    
                    print(f"[MANAGER] information of successor Sent to {all_peers}: {sysRespnd}")
                    
                    

                sysrespond = "SUCCESS: DHT setup complete"
            sock.sendto(sysrespond.encode(), address)

        elif command == "successor requested": # assigning Successors
            
            certain = comingmessage[1]
            print(f"[MANAGER] {certain} is requesting successor information")
            # assigns successors when requested

            if certain in sysSuccessors:
                
                succssIdf = sysSuccessors[certain]
                ip_address, msgPort, _ = sysPeer[succssIdf]
                sysRespnd = f"successor {succssIdf} {ip_address} {msgPort}"
                print(f"[MANAGER] successor is transfering to {certain}: {sysRespnd}")
                sock.sendto(sysRespnd.encode(), address)
                

            else:
                
                print(f"[MANAGER] Unable to find successor information for {certain}")
                sock.sendto("Error: unable to find successor ".encode(), address)


if _name_ == "_main_":
    managerToStart(5000)


