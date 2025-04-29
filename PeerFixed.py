import socket
import sys
import threading
import random

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

    def calculatePrime(self, year):
        # need to figure out some way to calculate something as the first prime > 2 * 1

        return 1000003

    """
    Parses through CSV file and sends commands to store data accordingly
    """
    def processCSV(self, year):
        
        pass
        
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
