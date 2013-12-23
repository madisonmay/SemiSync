from multiprocessing import Queue, Process, Manager
from collections import defaultdict
from copy import deepcopy
from types import FunctionType, MethodType
from pprint import pprint

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

  # functions cannot be added to queue
  # work around this by passing an id instead
  for fn in tree.keys():
    fn_map[id(fn)] = fn

  # start a new process for each object that has no dependencies
  for fn in independent_fns(tree):
    for i in range(len(tree[fn]['args'])):
      start_process(fn, tree[fn]['args'][i], tree[fn]['kwargs'][i])

  # read from queue as items are added
  i = 0
  while i < len(processes):

    # update note with new data
    result, fn_id = semisync.q.get()

    # execute callback function
    fn = fn_map[fn_id]
    if tree[fn]['callback']:
      tree[fn]['callback'](result)

    results[fn] += [result]

    # iterate through objects that depended on the completed obj
    # and remove the completed object from the list of their dependencies

    for other_fn in needed_for[fn]:
      depends_on[other_fn].remove(fn)

      # if any objects now have zero dependencies
      # start an async process for them
      if not depends_on[other_fn]:
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
  shared = Manager().Namespace()

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
    print shared

  @semisync(callback=process)
  def add(x):
    shared.sum += x

  @semisync(callback=process, dependencies=set([add]))
  def subtract(x):
    shared.sum -= x

  @semisync(callback=process, dependencies=set([subtract]))
  def multiply(x):
    shared.product = shared.sum * x



  add(1)
  subtract(2)
  multiply(3)

  # subtract(2, shared)


  # class Class:
  #   @semisync
  #   def method(self, shared_data, printout=False):
  #     shared_data.text = 'Hello World!'
  #     if printout: print shared_data.text
  #     return shared_data

  # method = semisync_method(Class(), 'method')


  results = exec_semisync()
"""
Notes:
Make semisync decorator do the work of initialization and termination
Add check for irresolvable dependencies
"""