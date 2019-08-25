import collections
import math

class OSBuffer:
    def __init__(self, capacity, frame_size):
        self.capacity = capacity #size in bytes
        self.size = 0
        self.frame_size = frame_size #size in bytes
        self.frame_capacity = math.floor(capacity / frame_size)
        self.frames = 0
        self.cache = collections.OrderedDict()

    def get(self, key):
        try:
            rm_val = self.cache.pop(key)
            self.frames -= math.ceil(rm_val / self.frame_size)
            self.size -= rm_val
            self.cache[key] = rm_val
            return rm_val
        except KeyError:
            return -1

    def is_in_set(self, key):
        if key in self.cache:
            True
        else:
            False

    def remove_from_buffer(self, key):
        if key in self.cache:
            rm_val = self.cache.pop(key)
            self.size -= rm_val
            self.frames -= math.ceil(rm_val / self.frame_size)
        else:
            pass

    def flush_buffer(self):
        flushed = []
        while self.cache:
            rm_key, rm_val = self.cache.popitem(last=False)
            self.size -= rm_val
            self.frames -= math.ceil(rm_val / self.frame_size)
            flushed.append(rm_val)
        return flushed

    def set(self, key, value):
        frame_occupation = math.ceil(value / self.frame_size)
        removed = []
        try:
            rm_val = self.cache.pop(key)
            self.size -= rm_val
            self.frames -= math.ceil(rm_val / self.frame_size)
        except KeyError:
            pass
        while (self.size + value) > self.capacity and (self.frames + frame_occupation) > self.frame_capacity:
            rm_key, rm_val = self.cache.popitem(last=False)
            removed.append(rm_val)
            self.size -= rm_val
            self.frames -= math.ceil(rm_val / self.frame_size)
        self.cache[key] = value
        self.size += value
        self.frames += frame_occupation
        return removed