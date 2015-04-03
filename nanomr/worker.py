import multiprocessing
import os
import sys
import zmq


from util import coroutine


def worker(pull_addr):
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.PULL)
    zmq_socket.connect(pull_addr)

    with open("{}.txt".format(os.getpid()), "w") as out:
        while True:
            message = zmq_socket.recv_json()
            if "stop" in message:
                break
            print >>out, message["line"]

    sys.exit(0)



def run_worker(pull_addr):
    process = multiprocessing.Process(target=worker, args=(pull_addr,))
    process.start()
    return process
