from multiprocessing import Queue, Process, Manager
from collections import defaultdict
from time import sleep

def queue_function(fn, args, kwargs):
  semisync.q.put([fn(*args, **kwargs), id(fn)])

def start_process(fn, args, kwargs):
  p = Process(target=queue_function, args=(fn, args, kwargs))
  p.start()

  semisync.processes.append(p)

def cleanup():
  # ensure no processes remain in a zombie state
  while semisync.processes:
    p = semisync.processes.pop()
    p.join()

def generate_dependency_trees(tree):
  for fn in tree.keys():
    for dependency in tree[fn].get('dependencies', []):
      semisync.depends_on[fn].add(dependency)
      semisync.needed_for[dependency].add(fn)

def dependencies(fn):
  tree, completed = semisync.tree, semisync.completed
  return [d for d in tree[fn]['dependencies'] if d not in completed]

def independent_fns(tree):
  result = []
  for key in tree.keys():
    if not dependencies(key):
      result.append(key)
  return result

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
  manager = Manager()
  depends_on = defaultdict(set)
  needed_for = defaultdict(set)
  completed = set()
  fn_map = {}
  lock = manager.Lock()

  def __init__(self, callback=False, dependencies=set()):
    self.callback = callback
    self.dependencies = dependencies

  def __call__(self, fn):
    """Returns decorated function"""
    def semisync_fn(*args, **kwargs):
      fn_call = {'callback': self.callback, 'args': [args], 'kwargs': [kwargs],
                 'dependencies': set([semisync.map[d] for d in self.dependencies])}
      semisync.tree[fn] = merge_dicts(fn_call, semisync.tree.get(fn, {}))

    # functions cannot be added to queue
    # work around this by passing an id inst
    semisync.fn_map[id(fn)] = fn

    #mapping from decorated function to undecorated function
    semisync.map[semisync_fn] = fn
    return semisync_fn

  @classmethod
  def clear(self):
    semisync.completed = set()
    semisync.depends_on = defaultdict(set)
    semisync.needed_for = defaultdict(set)

  @classmethod
  def begin(self):
    # applies fn(*args) for each obj in object, ensuring
    # that the proper attributes of shared_data exist before calling a method

    # because some functions depend on the results of other functions, this is 
    # a semi-synchronous operation -- certain methods must be guaranteed to
    # terminate before others 

    # aliasing
    completed = semisync.completed
    tree, q, processes = semisync.tree, semisync.q, semisync.processes
    depends_on, needed_for = semisync.depends_on, semisync.needed_for
    fn_map = semisync.fn_map

    generate_dependency_trees(tree)

    # start a new process for each object that has no dependencies
    for fn in independent_fns(tree):
      for i in range(len(tree[fn]['args'])):
        args, kwargs = tree[fn]['args'].pop(), tree[fn]['kwargs'].pop()
        start_process(fn, args, kwargs)


    # read from queue as items are added
    i = 0
    while i < len(processes):

      # update note with new data
      result, fn_id = semisync.q.get()
      fn = fn_map[fn_id]
      completed.add(fn)

      #execute callback
      if tree[fn]['callback']:
        tree[fn]['callback'](*result)

      # iterate through objects that depended on the completed obj
      # and remove the completed object from the list of their dependencies

      for other_fn in needed_for[fn]:
        depends_on[other_fn].remove(fn)

        # if any objects now have zero dependencies
        # start an async process for them
        if not depends_on[other_fn]:
          for j in range(len(tree[other_fn]['args'])):
            args, kwargs = tree[other_fn]['args'].pop(), tree[other_fn]['kwargs'].pop()
            start_process(other_fn, args, kwargs)


      i += 1

    cleanup()