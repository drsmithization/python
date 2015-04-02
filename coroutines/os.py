#!/usr/bin/env python

import select
import socket

from Queue import Queue


class SystemCall(object):
    def handle(self):
        pass


class Task(object):
    task_id = 0

    def __init__(self, target):
        Task.task_id += 1
        self.id = Task.task_id
        self.target = target
        self.sendvalue = None

    def run(self):
        return self.target.send(self.sendvalue)


class Scheduler(object):
    def __init__(self):
        self.ready = Queue()
        self.taskmap = {}
        self.exit_waiting = {}

        self.read_waiting = {}
        self.write_waiting = {}

    def wait_for_read(self, task, fd):
        self.read_waiting[fd] = task

    def wait_for_write(self, task, fd):
        self.write_waiting[fd] = task

    def iopoll(self, timeout):
        if self.read_waiting or self.write_waiting:
            r, w, e = select.select(self.read_waiting,
                                    self.write_waiting,
                                    [],
                                    timeout)
            for fd in r:
                self.schedule(self.read_waiting.pop(fd))
            for fd in w:
                self.schedule(self.write_waiting.pop(fd))

    def new(self, target):
        new_task = Task(target)
        self.taskmap[new_task.id] = new_task
        self.schedule(new_task)
        return new_task.id

    def schedule(self, task):
        self.ready.put(task)

    def iotask(self):
        while True:
            if self.ready.empty():
                self.iopoll(None)
            else:
                self.iopoll(0)
            yield

    def mainloop(self):
        self.new(self.iotask())
        while self.taskmap:
            task = self.ready.get()
            try:
                result = task.run()
                if isinstance(result, SystemCall):
                    result.task = task
                    result.scheduler = self
                    result.handle()
                    continue
            except StopIteration:
                self.exit(task)
                continue
            self.schedule(task)

    def exit(self, task):
        del self.taskmap[task.id]
        for task in self.exit_waiting.pop(task.id, []):
            self.schedule(task)

    def waitforexit(self, task, wait_id):
        if wait_id in self.taskmap:
            self.exit_waiting.setdefault(wait_id, []).append(task)
            return True
        return False


class GetTaskId(SystemCall):
    def handle(self):
        self.task.sendvalue = self.task.id
        self.scheduler.schedule(self.task)


class NewTask(SystemCall):
    def __init__(self, target):
        self.target = target

    def handle(self):
        new_task_id = self.scheduler.new(self.target)
        self.task.sendvalue = new_task_id
        self.scheduler.schedule(self.task)


class KillTask(SystemCall):
    def __init__(self, task_id):
        self.task_id = task_id

    def handle(self):
        task = self.scheduler.taskmap.get(self.task_id, None)
        if task:
            task.target.close()
            self.task.sendvalue = True
        else:
            self.task.sendvalue = False
        self.scheduler.schedule(self.task)


class WaitTask(SystemCall):
    def __init__(self, task_id):
        self.task_id = task_id

    def handle(self):
        res = self.scheduler.waitforexit(self.task, self.task_id)
        self.task.sendvalue = res
        if not res:
            self.scheduler.schedule(self.task)


class ReadWait(SystemCall):
    def __init__(self, f):
        self.f = f

    def handle(self):
        fd = self.f.fileno()
        self.scheduler.wait_for_read(self.task, fd)


class WriteWait(SystemCall):
    def __init__(self, f):
        self.f = f

    def handle(self):
        fd = self.f.fileno()
        self.scheduler.wait_for_write(self.task, fd)


class AsyncEchoServer:
    def __init__(self, port, handler, backlog=5):
        self.port = port
        self.handler = handler
        self.backlog = backlog

    def __call__(self):
        print "starting"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", self.port))
        s.listen(self.backlog)
        while True:
            yield ReadWait(s)
            client, addr = s.accept()
            yield NewTask(self.handler(client, addr))


def client_handler(client, addr):
    print "connection from {}".format(addr)
    while True:
        yield ReadWait(client)
        data = client.recv(65535)
        if not data:
            break
        yield WriteWait(client)
        client.send(data)
    client.close()
    print "connection from {} has been closed".format(addr)


def main():
    def alive():
        while True:
            print "I'm alive!"
            yield

    scheduler = Scheduler()

    scheduler.new(alive())

    server = AsyncEchoServer(5481, client_handler)
    scheduler.new(server())

    scheduler.mainloop()


if __name__ == "__main__":
    main()
