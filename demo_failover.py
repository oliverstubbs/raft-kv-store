import subprocess
import httpx

from client import Client, NODES

def find_leader(session):
    # loop through nodes and call root endpoint until leader found
    for node_id, url in NODES.items():
        try:
            response = session.request("GET", url)
        except httpx.HTTPError:
            continue
        if response.json().get("state") == "leader":
            return node_id
    return None

if __name__ == "__main__":
    client = Client(max_attempts=15)
    key = "colour"
    killed = None

    try:
        # write to healthy cluster with leader
        _, attempts = client.request(method="PUT", key=key, value="red")
        print(f"Set {key}=red after {attempts} attempts")

        # find the leader
        leader = find_leader(client.session)
        if leader is None:
            raise RuntimeError("No leader found - is the cluster up?")

        # kill the leader
        print(f"Killing the leader (node{leader})")
        killed = leader
        subprocess.run(["docker", "kill", f"node{killed}"], check=True, stdout=subprocess.DEVNULL)

        # write to healthy cluster with new leader
        _, attempts = client.request(method="PUT", key=key, value="blue")
        print(f"Set {key}=blue after {attempts} attempts")

        # try to retrieve the value 
        res, attempts = client.request(method="GET", key=key)
        print(f"Read back {key}={res.get('value')} after {attempts} attempts")

    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(e)

    finally:
        if killed is not None:
            print(f"Restarting node{killed}")
            subprocess.run(["docker", "start", f"node{killed}"], stdout=subprocess.DEVNULL)
        client.session.close()
