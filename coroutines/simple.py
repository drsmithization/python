#!/usr/bin/env python


def coroutine(f):
    def start(*args, **kwargs):
        cr = f(*args, **kwargs)
        cr.next()
        return cr
    return start


@coroutine
def receiver():
    try:
        while True:
            print "receiver: waiting"
            line = yield "ok"
            print "received: {}".format(line)
    except GeneratorExit:
        pass


def main():
    r = receiver()
    print r.send("a")
    r.throw(RuntimeError)
    print r.send("b")
    r.close()


if __name__ == "__main__":
    main()
