import socket
import threading
import os
import time
import hashlib
from json import dumps, loads


class Node:
    def __init__(self, host, port):
        self.stop = False
        self.host = host
        self.port = port
        self.M = 16
        self.N = 2**self.M
        self.key = self.hasher(host+str(port))
        # You will need to kill this thread when leaving, to do so just set self.stop = True
        threading.Thread(target=self.listener).start()
        self.files = []
        self.backUpFiles = []
        if not os.path.exists(host+"_"+str(port)):
            os.mkdir(host+"_"+str(port))
        '''
        ------------------------------------------------------------------------------------
        DO NOT EDIT ANYTHING ABOVE THIS LINE
        '''
        # Set value of the following variables appropriately to pass Intialization test
        self.own_address = {"host": self.host, "port": self.port}
        self.successor = (self.own_address["host"], self.own_address["port"])
        self.predecessor = (self.own_address["host"], self.own_address["port"])
        self.grandchild = ()
        threading.Thread(target = self.pinging_func).start()
        # additional state variables

    def hasher(self, key):
        '''
        DO NOT EDIT THIS FUNCTION.
        You can use this function as follow:
                For a node: self.hasher(node.host+str(node.port))
                For a file: self.hasher(file)
        '''
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

    def lookup(self, addr):
        node_key = self.hasher(self.successor[0] + str(self.successor[1]))
        key = self.hasher(addr[0] + str(addr[1]))
        my_key = self.key
        
        if((key > my_key) and (key < node_key)) or ((my_key > node_key) and ((key < my_key) and (key < node_key))):
            return self.successor
        elif((my_key > node_key) and ((key > my_key) and (key > node_key))) or (my_key == node_key):
            return self.successor
        else:
            lookup_socket = socket.socket()
            lookup_socket.connect(self.successor)
            dict_addr = {"host":addr[0], "port": addr[1]}
            lookup_socket.send(("FIND " + dumps(dict_addr)).encode('utf-8'))
            lookup_result = loads(lookup_socket.recv(1024*2).decode('utf-8'))
            lookup_socket.close()
            return (lookup_result["host"], lookup_result["port"])
    
    def lookfile(self, filename):
        key = self.hasher(filename)
        node_key = self.hasher(self.successor[0] + str(self.successor[1]))
        my_key = self.key
        
        if((key > my_key) and (key < node_key)) or ((my_key > node_key) and ((key < my_key) and (key < node_key))):
            return self.successor
        elif((my_key > node_key) and ((key > my_key) and (key > node_key))) or (my_key == node_key):
            return self.successor
        else:
            lookup_socket = socket.socket()
            lookup_socket.connect(self.successor)
            lookup_socket.send(("FINDFILE " + filename).encode('utf-8'))
            lookup_result = loads(lookup_socket.recv(1024*2).decode('utf-8'))
            lookup_socket.close()
            return (lookup_result["host"], lookup_result["port"])
        
    def tellSuccessor(self,tellsuc):
        
        tellsuc.connect(self.successor)
        tellsuc.send("TELLSUCC ".encode('utf-8')) 
        grandsucc = tellsuc.recv(1024*2).decode('utf-8')
        grandsucc = loads(grandsucc)
        grandsucc_host = grandsucc["host"]
        grandsucc_port = int(grandsucc["port"])
        grandsucc_tup = (grandsucc_host,grandsucc_port)
        self.grandchild = grandsucc_tup
        tellsuc.close()
        
    def grandSuccessor(self,make_predsock):
        make_pred = {"host": self.own_address["host"],"port": self.own_address["port"]}
        make_predsock.connect(self.grandchild)
        make_predsock.send(("GRANDPRED "+dumps(make_pred)).encode('utf-8'))
        
    def dumpFile(self,make_predsock):
        make_predsock.connect(self.grandchild)
        make_predsock.send("FILING ".encode('utf-8'))
        
    def newSuccessor(self,new_succsock):
        new_succsock.send(("UPDSUCC " + dumps(self.own_address)).encode('utf-8'))
        
    def newPredecessor(self,new_predsock):
        new_predsock.send(("UPDPRED " + dumps(self.own_address)).encode('utf-8'))

    def pinging_func(self):
        while self.stop != True:
            time.sleep(0.3)
            try:
                succ_soc = socket.socket()
                succ_soc.connect(self.successor)
                succ_soc.send("FINDPRED ".encode('utf-8'))
                succ_response = loads(succ_soc.recv(1024*2).decode('utf-8'))
                if (self.own_address["host"], self.own_address["port"]) != (succ_response["host"], succ_response["port"]):
                    
                    self.predecessor = (succ_response["host"], succ_response["port"])
                    new_succsock = socket.socket()
                    new_succsock.connect(self.predecessor)
                    self.newSuccessor(new_succsock)
                    new_succsock.close()

                    new_predsock = socket.socket()
                    new_predsock.connect(self.successor)
                    self.newPredecessor(new_predsock)
                    new_predsock.close()
                    
                    succ_sock = socket.socket()
                    succ_sock.connect(self.successor)
                    succ_sock.send("FILETRANSFER ".encode('utf-8'))	
                    succ_sock.close()
                                    
                tellsuc = socket.socket()
                try:
                    self.tellSuccessor(tellsuc)
                    succfile_sock = socket.socket()
                    succfile_sock.connect(self.successor)
                    succfile_sock.send(("FILES "+dumps(self.files)).encode('utf-8'))
                    succfile_sock.close()
                except:
                    tellsuc.close()
                    succfile_sock.close()
                    
                succ_soc.close()          
            except:
                succ_soc.close()
                self.successor = self.grandchild  
                            
                make_predsock = socket.socket()
                self.grandSuccessor(make_predsock)
                make_predsock.close()
                
                make_predsock = socket.socket()
                self.dumpFile(make_predsock)
                make_predsock.close()

    def handleConnection(self, client, addr):
        '''
         Function to handle each inbound connection, called as a thread from the listener.
        '''
        get_message = client.recv(1024*2).decode('utf-8')
        if get_message.split(" ", 1)[0] == "JOIN":
            addr_dict = loads(get_message.split(" ", 1)[1])
            joininAddr = (addr_dict["host"], addr_dict["port"])
            successor = self.lookup(joininAddr)
            succ_addr = {"host":successor[0], "port": successor[1]}
            client.send(dumps(succ_addr).encode('utf-8'))
        elif get_message.split(" ", 1)[0] == "FIND":
            addr_dict = loads(get_message.split(" ",1)[1])
            addr = (addr_dict["host"], addr_dict["port"])
            successor = self.lookup(addr)
            succ_addr = {"host":successor[0], "port": successor[1]}
            client.send(dumps(succ_addr).encode('utf-8'))
        elif get_message.split(" ",1)[0] == "FINDPRED":
            client.send(dumps({"host": self.predecessor[0], "port": self.predecessor[1]}).encode('utf-8'))
        elif get_message.split(" ",1)[0] == "UPDSUCC":
            addr_dict = loads(get_message.split(" ",1)[1])
            addr = (addr_dict["host"], addr_dict["port"])
            self.successor = addr
        elif get_message.split(" ",1)[0] == "UPDPRED":
            addr_dict = loads(get_message.split(" ",1)[1])
            addr = (addr_dict["host"], addr_dict["port"])
            self.predecessor = addr
        elif get_message.split(" ",1)[0] == "FINDFILE":
            filename = get_message.split(" ",1)[1]
            successor = self.lookfile(filename)
            succ_addr = {"host":successor[0], "port": successor[1]}
            client.send(dumps(succ_addr).encode('utf-8'))
        elif get_message.split(" ",1)[0] == "SAVEFILE":
            try:
                FILENAME = get_message.split(" ",1)[1]
                path = self.own_address["host"] + "_" + str(self.own_address["port"]) + "/" + FILENAME
                client.send("ok".encode('utf-8'))
                self.files.append(FILENAME)
                self.recieveFile(client, path)
            except:
                client.close()     
        elif get_message.split(" ",2)[0] == "FETCHFILE":   
            CheckFile =  get_message.split(" ",2)[1]
            request_addr = loads(get_message.split(" ",2)[2])
            get_host = request_addr["host"]
            get_port = int(request_addr["port"])
            dict_tup = (get_host,get_port)
            check = False
            for filename in self.files:
                if filename == CheckFile:
                    client.send(CheckFile.encode('utf-8'))
                    check = True
                    soc_file = socket.socket()
                    try:
                        soc_file.connect(dict_tup)
                        soc_file.send(("SAVEFILE "+filename).encode('utf-8'))
                        confirmation_msg = soc_file.recv(1024*2).decode('utf-8')
                        self.sendFile(soc_file, filename)
                        soc_file.close()
                    except:
                        soc_file.close()
                    break
            if check == False:
                client.send("".encode('utf-8'))  
        elif get_message.split(" ",1)[0] == "FILETRANSFER":
            storedFiles = self.files
            self.files = []
            for filename in storedFiles:
                path = self.own_address["host"]+"_"+str(self.own_address["port"])+"/"+filename
                file_node = self.lookfile(filename)
                filetransfer_sock = socket.socket()
                filetransfer_sock.connect(file_node)
                filetransfer_sock.send(("SAVEFILE "+filename).encode('utf-8'))
                filetransfer_sock.recv(1024*2).decode('utf-8')
                self.sendFile(filetransfer_sock,path)
                filetransfer_sock.close()                           
        elif get_message.split(" ",1)[0] == "NOPRED":
            request_addr = loads(get_message.split(" ",1)[1])
            get_host = request_addr["host"]
            get_port = int(request_addr["port"]) 
            self.predecessor = (get_host,get_port)               
        elif get_message.split(" ",1)[0] == "NOSUCC":
            request_addr = loads(get_message.split(" ",1)[1])
            get_host = request_addr["host"]
            get_port = int(request_addr["port"]) 
            self.successor = (get_host,get_port) 
        elif get_message.split(" ",1)[0] == "TELLSUCC":
            sending_succ = {"host": self.successor[0],"port": self.successor[1]}
            client.send(dumps(sending_succ).encode('utf-8'))
        elif get_message.split(" ",1)[0] == "FILES":
            fetchfiles = get_message.split(" ",1)[1]
            self.backUpFiles = fetchfiles
            client.send("gotfiles".encode('utf-8'))
        elif get_message.split(" ",1)[0] == "FILING":
            self.files = self.backUpFiles
            client.send("fileshifted".encode('utf-8'))
        elif get_message.split(" ",1)[0] == "GRANDPRED":
            request_addr = loads(get_message.split(" ",1)[1])
            grandpredhost = request_addr["host"]
            grandpredport = int(request_addr["port"])
            self.predecessor = (grandpredhost,grandpredport)
        client.close()
        
    def listener(self):
        '''
        We have already created a listener for you, any connection made by other nodes will be accepted here.
        For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
        to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
        '''
        listener = socket.socket()
        listener.bind((self.host, self.port))
        listener.listen(10)
        while not self.stop:
            client, addr = listener.accept()
            threading.Thread(target=self.handleConnection,
                             args=(client, addr)).start()
        print("Shutting down node:", self.host, self.port)
        try:
            listener.shutdown(2)
            listener.close()
        except:
            listener.close()

    def join(self, joiningAddr):
        '''
        This function handles the logic of a node joining. This function should do a lot of things such as:
        Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
        '''
        if len(joiningAddr):
            join_soc = socket.socket()
            join_soc.connect(joiningAddr)
            join_soc.send(("JOIN " + dumps(self.own_address)).encode('utf-8'))
            placement_addr = loads(join_soc.recv(1024*2).decode('utf-8'))
            self.successor = (placement_addr["host"], placement_addr["port"])
            join_soc.close()

    def put(self, fileName):
        '''
        This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
        Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
        in directory given by host_port e.g. "localhost_20007/file.py".
        '''
        soc_file = socket.socket()
        try:
            spot = self.lookfile(fileName)
            soc_file.connect(spot)
            soc_file.send(("SAVEFILE "+fileName).encode('utf-8'))
            confirmation_msg = soc_file.recv(1024*2).decode('utf-8')
            self.sendFile(soc_file, fileName)
            soc_file.close()
        except:
            soc_file.close()
        
    def get(self, fileName):
        '''
        This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
        i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
        '''
        if len(fileName):
            spot = self.lookfile(fileName)
            soc_file = socket.socket()
            soc_file.connect(spot)
            soc_file.send(("FETCHFILE "+fileName+" "+dumps(self.own_address)).encode('utf-8'))
            confirmation_msg = soc_file.recv(1024*2).decode('utf-8')
            if confirmation_msg == fileName:
                return fileName
            else:
                return None
        return None
    
    def leftPred(self,predNode,succ_dict,predNodesock):
        predNodesock.connect(predNode)
        predNodesock.send(("NOSUCC "+dumps(succ_dict)).encode('utf-8'))
        predNodesock.recv(1024*2).decode('utf-8')
        
    def leftSucc(self,succNode,pred_dict,succNodesock):
        succNodesock.connect(succNode)
        succNodesock.send(("NOPRED "+dumps(pred_dict)).encode('utf-8'))
        succNodesock.recv(1024*2).decode('utf-8')
        
    def leaveFile(self,succNode):
        
        for FILE in self.files:
            path = self.own_address["host"]+"_"+str(self.own_address["port"])+"/"+FILE
            leavenode_sock = socket.socket()
            leavenode_sock.connect(succNode)
            leavenode_sock.send(("SAVEFILE "+FILE).encode('utf-8'))
            leavenode_sock.recv(1024*2).decode('utf-8')
            self.sendFile(leavenode_sock,path)
            leavenode_sock.close()

    def leave(self):
        '''
        When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
        it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
        by setting self.stop flag to True
        '''
        succNode = self.successor
        self.successor = (self.own_address["host"],self.own_address["port"])
        succ_dict = {"host": succNode[0],"port":succNode[1]}
        
        predNode = self.predecessor
        self.predecessor = (self.own_address["host"],self.own_address["port"])      
        pred_dict = {"host": predNode[0],"port":predNode[1]}
        
        predNodesock = socket.socket()        
        self.leftPred(predNode,succ_dict,predNodesock)
        predNodesock.close()   
        
        succNodesock = socket.socket() 
        self.leftSucc(succNode,pred_dict,succNodesock)
        succNodesock.close()       
       
        self.leaveFile(succNode)
        
    def sendFile(self, soc, fileName):
        '''
        Utility function to send a file over a socket
                Arguments:	soc => a socket object
                                        fileName => file's name including its path e.g. NetCen/PA3/file.py
        '''
        fileSize = os.path.getsize(fileName)
        soc.send(str(fileSize).encode('utf-8'))
        soc.recv(1024).decode('utf-8')
        with open(fileName, "rb") as file:
            contentChunk = file.read(1024)
            while contentChunk != "".encode('utf-8'):
                soc.send(contentChunk)
                contentChunk = file.read(1024)

    def recieveFile(self, soc, fileName):
        '''
        Utility function to recieve a file over a socket
                Arguments:	soc => a socket object
                                        fileName => file's name including its path e.g. NetCen/PA3/file.py
        '''
        fileSize = int(soc.recv(1024).decode('utf-8'))
        soc.send("ok".encode('utf-8'))
        contentRecieved = 0
        file = open(fileName, "wb")
        while contentRecieved < fileSize:
            contentChunk = soc.recv(1024)
            contentRecieved += len(contentChunk)
            file.write(contentChunk)
        file.close()

    def kill(self):
        # DO NOT EDIT THIS, used for code testing
        self.stop = True
