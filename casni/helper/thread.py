import threading

def list_threads():
    return {thread.name:thread for thread in threading.enumerate()}