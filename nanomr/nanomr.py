#!/usr/bin/env python

import zmq

from util import coroutine

import worker


def text_reader(filename, target):
    with open(filename, "r") as inp:
        for line in inp:
            target.send(line.rstrip())
        target.close()

@coroutine
def zmq_sender(push_addr):
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.PUSH)
    zmq_socket.bind(push_addr)

    print "starting"
    try:
        while True:
            line = (yield)
            zmq_socket.send_json({"line": line})
    except GeneratorExit:
        print "generator exit"
    zmq_socket.send_json({"stop": True})
    zmq_socket.send_json({"stop": True})


def main():
    addr = "tcp://127.0.0.1:5557"

    w1 = worker.run_worker(addr)
    w2 = worker.run_worker(addr)

    text_reader("test.txt", zmq_sender(addr))

    w1.join()
    w2.join()



if __name__ == "__main__":
    main()
