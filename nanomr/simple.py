#!/usr/bin/env python

import collections


def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return start


def text_reader(filename, target):
    with open(filename, "r") as inp:
        for line in inp:
            target.send(line.rstrip())
        target.close()


@coroutine
def text_writer(filename):
    with open(filename, "w") as out:
        while True:
            key, value = (yield)
            print >>out, "\t".join(map(str, (
                key, value
            )))


@coroutine
def map_runner(map_function, target):
    while True:
        line = (yield)
        for key, value in map_function(line):
            target.send((key, value))


@coroutine
def reduce_runner(reduce_function, target):
    records = collections.defaultdict(list)
    try:
        while True:
            key, value = (yield)
            records[key].append(value)
    except GeneratorExit:
        for key, values in records.iteritems():
            for key, value in reduce_function(key, values):
                target.send((key, value))


class NanoMr:
    def __init__(self):
        pass

    def run(self, input, output, map_function, reduce_function):
        output_writer = text_writer(output)
        pipeline = map_runner(map_function, reduce_runner(
            reduce_function, output_writer))
        text_reader(input, pipeline)


def my_map(line):
    for word in line.split(" "):
        yield word, 1


def my_reduce(key, values):
    yield key, sum(values)


def main():
    mr = NanoMr()
    mr.run("input.txt", "output.txt", my_map, my_reduce)


if __name__ == "__main__":
    main()
