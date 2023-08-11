import socket

def get_host_address():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        addr = s.getsockname()[0]
    return addr