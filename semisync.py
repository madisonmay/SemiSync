from multiprocessing import Queue, Process
from collections import defaultdict
from copy import deepcopy
from types import FunctionType, MethodType

class SharedData(object): pass

def queue_function(fn, args, q):
  q.put([fn(*args), fn, args])

def start_process(fn, args, q, processes):
   p = Process(target=queue_function, args=(fn, args, q))
   p.start()

   processes.append(p)

def cleanup(processes, q):
  # ensure no processes remain in a zombie state
  for p in processes:
    p.join()

  q.close()

def dependency_trees(tree):
  depends_on = defaultdict(set)
  needed_for = defaultdict(set)
  tree = defaultdict(lambda : defaultdict(tuple), tree)

  for fn in tree.keys():
    for dependency in tree[fn].get('dependencies', []):
      depends_on[fn].add(dependency)
      needed_for[dependency].add(fn)

  return depends_on, needed_for

def independent_fns(tree):
  return set([key for key in tree.keys() if not tree[key].get('dependencies', False)])

def extract_shared_data(data):
  if isinstance(data, tuple):
    for datum in data:
      if isinstance(datum, SharedData):
        return SharedData
  if isinstance(data, SharedData):
    return data

def merge(new, master):
  if isinstance(new, SharedData):
    for k, v in new.__dict__.items():
      setattr(master, k, v)

def semisync(tree=None, on_completed=None, shared_data=None):
  # applies fn(*args) for each obj in object, ensuring
  # that the proper attributes of shared_data exist before calling a method

  # because some functions depend on the results of other functions, this is 
  # a semi-synchronous operation -- certain methods must be guaranteed to
  # terminate before others 

  results = defaultdict(lambda : defaultdict(tuple))
  depends_on, needed_for = dependency_trees(tree)

  q = Queue()
  processes = []

  # start a new process for each object that has no dependencies
  for fn in independent_fns(tree):
    start_process(fn, tree[fn]['args'], q, processes)

  # read from queue as items are added
  i = 0
  while i < len(processes):

    # update note with new data
    result, fn, args = q.get()
    new_data = extract_shared_data(result)
    merge(new_data, shared_data)

    if on_completed:
      on_completed(result, fn, args)

    results[fn][args] = result

    # iterate through objects that depended on the completed obj
    # and remove the completed object from the list of their dependencies
    for other_fn in needed_for[fn]:
      depends_on[other_fn].remove(fn)

      # if any objects now have zero dependencies
      # start an async process for them
      if not depends_on[other_fn]:
        start_process(other_fn, tree[other_fn]['args'], q, processes)

    i += 1

  cleanup(processes, q)

  return dict(results), shared_data.__dict__

if __name__ == '__main__':

  #shared data
  shared = SharedData()
  shared.value = 5

  def add(x, shared_data):
    shared_data.sum = shared_data.value + x
    return shared_data

  def subtract(x, shared_data):
    shared_data.difference = shared_data.value - x
    return shared_data

  def multiply(x, shared_data):
    shared_data.product = shared_data.sum * x
    return shared_data

  class Class:
    def method(self, shared_data):
      shared_data.text = 'Hello World!'
      return shared_data

  c = Class()

  # wrap method in fn to call semisynchronously
  def method(obj, shared_data):
    return obj.method(shared_data)

  def on_completed(result, fn, args):
    print fn.__name__

  tree = {add: {'args': (2, shared)},
          subtract: {'args': (3, shared)},
          multiply: {'dependencies': set([add]), 'args': (5, shared)},
          method: {'args': (c, shared)}}

  results, shared_data = semisync(tree=tree, on_completed=on_completed, shared_data=shared)
  print shared_data
