#!/usr/bin/env python

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

    def new(self, target):
        new_task = Task(target)
        self.taskmap[new_task.id] = new_task
        self.schedule(new_task)
        return new_task.id

    def schedule(self, task):
        self.ready.put(task)

    def mainloop(self):
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


class GetTaskId(SystemCall):
    def handle(self):
        self.task.sendvalue = self.task.id
        self.scheduler.schedule(self.task)


def main():
    def x():
        my_task_id = yield GetTaskId()
        for _ in xrange(5):
            print "x: {}".format(my_task_id)
            yield

    def y():
        my_task_id = yield GetTaskId()
        for _ in xrange(10):
            print "y: {}".format(my_task_id)
            yield

    scheduler = Scheduler()

    scheduler.new(x())
    scheduler.new(y())

    scheduler.mainloop()



if __name__ == "__main__":
    main()
