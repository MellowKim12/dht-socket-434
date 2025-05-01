from pickle import TRUE
import socket
import sys
import threading
import random
import csv
from math import ceil, sqrt

# TO DO Might need to not have file path be hard coded idk tho

class Peer:
    def __init__(self, name, ip, m_port, p_port, manager_ip, manager_port):
        self.name = name
        self.ip = ip
        self.m_port = m_port
        self.p_port = p_port
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.state = 'Free'
        self.dht_info = None
        self.hash_table = {}
        self.manager_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.manager_sock.bind((ip, m_port))
        self.peer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.peer_sock.bind((ip, p_port))
        self.lock = threading.Lock()


    

    def sendToManager(self, message):
        self.manager_sock.sendto(message.encode(), (self.manager_ip, self.manager_port))
        response, _ = self.manager_sock.recvfrom(1024)
        return response.decode()

    def setupDHT(self, n, year):
        response = self.sendToManager(f"setup-dht {self.name} {n} {year}")
        if response.startswith("SUCCESS"):
            peers_info = response.split()[1:]
            peers = []
            for i in range(0, len(peers_info), 3):
                name, ip, p_port = peers_info[i], peers_info[i+1], int(peers_info[i+2])
                peers.append((name, ip, p_port))
            self.dht_info = {
                'id': 0,
                'n': len(peers),
                'peers': [(name, ip, p_port, idx) for idx, (name, ip, p_port) in enumerate(peers)]
                }
            self.state = 'Leader'

            for idx, (name, ip, p_port, _) in enumerate(self.dht_info['peers']):
                if name != self.name:
                    message = f"set-id {idx} {len(peers)} {self.dht_info['s']} " + " ".join(f"{p[0]}, {p[1]}, {p[2]}"
                               for p in self.dht_info['peers'])
                    self.sendToPeer(ip, p_port, message)

                self.processCSV(year)
                self.sendToManager(f"dht-complete {self.name}")
            else:
                print("Setup DHT failed:", response)

    def deregister(self):
        response = self.sendToManager(f"deregister {self.name}")
        print(response)
        if response.startswith("SUCCESS"):
            sys.exit(0)

    def query(self, event_id):
        response = self.sendToManager(f"query-dht {self.name}")
        if not response.startswith("SUCCESS"):
            return

        _, target_name, target_ip, target_port = response.split()
        seq_id = []
        copy_peers = self.dht_info['peers']
        self.sendToPeer(target_ip, int(target_port), f"find-event {event_id} {self.name}  {self.ip}  {self.p_port} {seq_id} {copy_peers}")

    def findEvent(self, event_id, target_name, target_ip, target_port, seq_id, copy_peers):
        # implement hot potato protocol here
        pos = event_id % self.dht_info['s']
        id = pos % self.dht_info['n']
        seq_id.append(id)
        if id == self.dht_info['id']:
            row = self.hash_table[pos]
            if int(row[0]) == event_id:
                self.sendToPeer(target_ip,int(target_port),f"SUCCESS {row}, {id}")
        else:
            if not copy_peers:
                self.sendToPeer(target_ip,int(target_port),f"FAILURE. STORM event {event_id} not found in the DHT.")
            copy_peers.pop(id)
            next = random.choice(copy_peers)
            self.sendToPeer(next.ip, int(next.p_port), f"find-event {event_id} {target_name}  {target_ip}  {target_port} {seq_id} {copy_peers}")
            
        # send result back to requester

    def leaveDHT(self):
        response = self.sendToManager(f"leave-dht {self.name}")
        if (response.startswith("SUCCESS")):
            self.initiateLeaveProtocol()

    def initiateLeaveProtocol(self):
        # implement leave protocol from 1.2.3

        pass

    def joinDHT(self):
        response = self.sendToManager(f"join-dht {self.name}")
        if (response.startswith("SUCCESS")):
            self.initiateJoinProtocol()
    
    def initiateJoinProtocol():
        # implement join protocol 
        pass

    def teardownDHT(self):
        response = self.sendToManager(f"teardown-dht {self.name}")
        if (response.startswith("SUCCESS")):
            self.initiateTeardown()

    def initiateTeardown(self):
        # implement teardown (send teardown command throughout the ring)

        pass



    """
    Reads in data from the csv and calculates primes
    """
    def calculatePrime(self):
        # need to figure out some way to calculate something as the first prime > 2 * 1
        filename = "storm_data_search_results.csv"
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            l = sum(1 for _ in reader) - 1
        s = 2 * l + 1
        while True:
            if self.is_prime(s):
                return s
            s += 1

    def is_prime(self, n):
        if n <= 1:
            return False
        for i in range(2, ceil(sqrt(n)) + 1):
            if n % i == 0:
                return False
        return True

    """
    Parses through CSV file and sends commands to store data accordingly
    """
    def processCSV(self):
        filename = "storm_data_search_results.csv"
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                event_id = int(row[0])
                pos = event_id % self.dht_info['s']
                target_id = pos % self.dht_info['n']
                if target_id == self.dht_info['id']:
                    self.store_record(pos, row)
                else:
                    self.forward_store_command(target_id, pos, row)

    def store_record(self, pos, record):
        with self.lock:
            self.hash_table[pos] = record
    
    def forward_store_command(self, target_id, pos, record):
        right_id = (self.dht_info['id'] + 1) % self.dht_info['n']
        for peer in self.dht_info['peers']:
            if peer[3] == right_id:
                ip, port = peer[1], peer[2]
                message = f"store {target_id} {pos} {'|'.join(record)}"
                self.sendToPeer(ip, port, message)
                break
        
    def sendToPeer(self, ip, port, message):
        self.peer_sock.sendto(message.encode(), (ip, port))

    def handlePeerMessage(self, data):
        parts = data.split()
        cmd = parts[0]
        if cmd == 'set-id':
            peer_id = int(parts[1])
            n = int(parts[2])
            s = int(parts[3])
            peers = []
            for p_str in parts[4:]:
                name, ip, p_port = p_str.split(',')
                peers.append((name, ip, int(p_port)))
            self.dht_info = {
                'id': peer_id,
                'n': n,
                's': s,
                'peers': [(name, ip, p_port, idx) for idx, (name, ip, p_port) in enumerate(peers)]
                }
            self.state = 'InDHT'
            print(f"Set ID to {peer_id} with {n} peers")
        elif cmd == 'store':
            target_id = int(parts[1])
            pos = int(parts[2])
            record = parts[3]
            if self.dht_info['id'] == target_id:
                self.store_record(pos, record)
            else:
                self.forward_store_command(target_id, pos, record)
        elif cmd == 'find-event':
            self.findEvent(parts[1], parts[2], parts[3], parts[4])
        elif cmd == 'reset-id':
            # need to implement
            pass
        elif cmd == 'teardown':
            self.teardownDHT()

    def listenManager(self):
        while True:
            data, addr = self.manager_sock.recvfrom(1024)
            print("Manager response:", data.decode())

    def listenPeer(self):
        while True:
            data, addr = self.peer_sock.recvfrom(1024)
            self.handlePeerMessage(data.decode())

    def run(self):
        threading.Thread(target=self.listenManager, daemon=True).start()
        threading.Thread(target = self.listenPeer, daemon=True).start()
        while True:
            cmd = input().strip()
            if cmd == 'exit':
                break
            if cmd.startswith('setup-dht'):
                _,n, year = cmd.split()
                self.setupDHT(int(n), year)
            elif cmd.startswith('deregister'):
                self.deregister()
            elif cmd.startswith('query-dht'):
                _, event_id =cmd.split()
                self.query(int(event_id))
            elif cmd.startswith('leave-dht'):
                self.leaveDHT()
            elif cmd.startswith('join-dht'):
                self.joinDHT()
            elif cmd.startswith('teardown-dht'):
                self.teardownDHT()
            

if __name__ == '__main__':
    if len(sys.argv) != 7:
        print ("Usage: python PeerFixed.py <name> <ip> <p_port> <m_port> <manager_ip> <manager_port>")
        sys.exit(1)
    name, ip, m_port, p_port, manager_ip, manager_port = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], int(sys.argv[6])
    peer = Peer(name, ip, m_port, p_port, manager_ip, manager_port)
    response = peer.sendToManager(f"register {name} {ip} {m_port} {p_port}")
    if response.startswith("SUCCESS"):
        print("Registered Successfully")
        peer.run()
    else:
        print("Registration failed: ", response)
