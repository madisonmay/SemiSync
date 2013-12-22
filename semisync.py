from multiprocessing import Queue, Process
from collections import defaultdict
from copy import deepcopy
from types import FunctionType, MethodType
from pprint import pprint

class SharedData(object): pass

def queue_function(fn, args, kwargs):
  semisync.q.put([fn(*args, **kwargs), id(fn)])

def start_process(fn, args, kwargs):
  p = Process(target=queue_function, args=(fn, args, kwargs))
  p.start()

  semisync.processes.append(p)

def cleanup():
  # ensure no processes remain in a zombie state
  for p in semisync.processes:
    p.join()

  semisync.q.close()

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

def extract_data_type(data, datatype):
  if isinstance(data, tuple):
    for datum in data:
      if isinstance(datum, datatype):
        return datum
  if isinstance(data, datatype):
    return data

def merge(new, master):
  if isinstance(new, SharedData):
    for k, v in new.__dict__.items():
      try:
        current = getattr(master, k)
        setattr(master, k, current + v)
      except:
        setattr(master, k, v)

def exec_semisync():
  # applies fn(*args) for each obj in object, ensuring
  # that the proper attributes of shared_data exist before calling a method

  # because some functions depend on the results of other functions, this is 
  # a semi-synchronous operation -- certain methods must be guaranteed to
  # terminate before others 

  # aliasing
  shared = semisync.shared
  tree, q, processes = semisync.tree, semisync.q, semisync.processes

  results = defaultdict(list)
  depends_on, needed_for = dependency_trees(tree)
  fn_map = {}

  # start a new process for each object that has no dependencies
  for fn in independent_fns(tree):
    fn_map[id(fn)] = fn
    for i in range(len(tree[fn]['args'])):
      print "Before calling function", semisync.shared.__dict__
      start_process(fn, tree[fn]['args'][i], tree[fn]['kwargs'][i])

  # read from queue as items are added
  i = 0
  while i < len(processes):

    # update note with new data
    result, fn_id = semisync.q.get()
    print "After function return", semisync.shared.__dict__

    # execute callback function
    fn = fn_map[id(fn)]
    if tree[fn]['callback']:
      tree[fn]['callback'](result)

    new_data = extract_data_type(result, SharedData)
    merge(new_data, shared)

    print "After data merge", semisync.shared.__dict__

    results[fn] += [result]

    # iterate through objects that depended on the completed obj
    # and remove the completed object from the list of their dependencies

    for other_fn in needed_for[fn]:
      depends_on[other_fn].remove(fn)

      # if any objects now have zero dependencies
      # start an async process for them
      if not depends_on[other_fn]:
        fn_map[id(other_fn)] = other_fn
        for j in range(len(tree[other_fn]['args'])):
          start_process(other_fn, tree[other_fn]['args'][j], tree[other_fn]['kwargs'][j])

    needed_for[fn] = set()


    i += 1

  cleanup()

  return dict(results)

# wrap method in fn to call semisynchronously
def semisync_method(c, method_name):
  def method(*args, **kwargs):
    getattr(c, method_name)(*args, **kwargs)
  return method 

def merge_dicts(d1, d2):
  for key in ['args', 'kwargs']:
    d1[key] += d2.get(key, [])
  return d1
    
class semisync:
  tree = {}
  q = Queue()
  processes = []
  map = {}
  shared = SharedData()

  def __init__(self, callback=False, dependencies=set()):
    self.callback = callback
    self.dependencies = dependencies

  def __call__(self, fn):
    """Returns decorated function"""
    def semisync_fn(*args, **kwargs):
      fn_call = {'callback': self.callback, 'args': [args], 'kwargs': [kwargs],
                 'dependencies': set([semisync.map[d] for d in self.dependencies])}
      semisync.tree[fn] = merge_dicts(fn_call, semisync.tree.get(fn, {}))
    semisync.map[semisync_fn] = fn
    return semisync_fn
                


if __name__ == '__main__':

  #shared data
  shared = semisync.shared
  shared.sum = 0

  def process(result):
    pass
    # print result.__dict__

  @semisync(callback=process)
  def add(x):
    print "In function before modification", semisync.shared.__dict__
    shared.sum += x
    print "In function after modification", semisync.shared.__dict__
    return shared

  @semisync(callback=process, dependencies=set([add]))
  def multiply(x):
    print "In function before modification", semisync.shared.__dict__
    shared.product = shared.sum * x
    print "In function after modification", semisync.shared.__dict__
    return shared


  # @semisync()
  # def subtract(x, shared_data):
  #   shared_data.sums -= x
  #   return shared_data

  add(1)
  add(2)
  multiply(3)
  multiply(4)
  # subtract(2, shared)


  # class Class:
  #   @semisync
  #   def method(self, shared_data, printout=False):
  #     shared_data.text = 'Hello World!'
  #     if printout: print shared_data.text
  #     return shared_data

  # method = semisync_method(Class(), 'method')


  results = exec_semisync()
  print shared.__dict__
"""
Notes:
Make semisync decorator do the work of initialization and termination
Eliminate need to return shared data
"""