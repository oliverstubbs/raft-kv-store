import random
import time
import httpx

NODES = {str(i): f"http://localhost:{8000 + i}" for i in range(1, 6)}

class Client:
    def __init__(self, nodes=NODES, timeout=12, max_attempts=5, delay=0.1):
        self.nodes, self.max_attempts, self.delay = nodes, max_attempts, delay
        self.session = httpx.Client(timeout=timeout)
        self.node_id = random.choice(list(nodes))
        
    def other_node(self):
        self.node_id = random.choice([n for n in self.nodes if n != self.node_id])
        
    def request(self, method, key, value=None):
        params = {"value": value} if value is not None else None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.request(method, f"{self.nodes[self.node_id]}/value/{key}", params=params)
            except httpx.HTTPError:
                self.other_node()
            else:
                # if successful, return the response
                if response.status_code == 200:
                    return response.json(), attempt
                # if not a leader, check for leader id
                if response.status_code == 421:
                    leader_id = response.json()["detail"].get("leader_id")
                    if leader_id in self.nodes:
                        self.node_id = leader_id
                    else:
                        self.other_node()
                # check for 503
                elif response.status_code != 503:
                    response.raise_for_status()
            time.sleep(self.delay)
        
        raise RuntimeError(f"No leader afrer {self.max_attempts} attempts") 

if __name__ == "__main__":
    client = Client()
    key = "x"
    value = "7"
    
    try:
        # try and put value
        res1, attempts1 = client.request(method="PUT", key=key, value=value)
        print(f"Successful put after {attempts1} attempts")
        
        # try and get value
        res2, attempts2 = client.request(method="GET", key=key)
        if (res2.get("value") == value):
            print(f"Successfully retrieved correct value after {attempts2} attempts")
        else:
            print("Unsuccessful")
        
    except RuntimeError as e:
        print(e)
        
    finally:
        client.session.close()
    