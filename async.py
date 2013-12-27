from multiprocessing import Queue, Process, Manager
from collections import defaultdict

def queue_function(fn, args, kwargs):
  async.q.put([fn(*args, **kwargs), id(fn)])

def start_process(fn, args, kwargs):
  p = Process(target=queue_function, args=(fn, args, kwargs))
  p.start()

  async.processes.append(p)

def cleanup():
  # ensure no processes remain in a zombie state
  while async.processes:
    p = async.processes.pop()
    p.join()

def merge_dicts(d1, d2):
  for key in ['args', 'kwargs']:
    d1[key] += d2.get(key, [])
  return d1
    
class async:
  tree = {}
  q = Queue()
  processes = []
  map = {}
  manager = Manager()
  fn_map = {}
  lock = manager.Lock()

  def __init__(self, callback=False, dependencies=set()):
    self.callback = callback
    self.dependencies = dependencies

  def __call__(self, fn):
    """Returns decorated function"""
    def async_fn(*args, **kwargs):
      fn_call = {'args': [args], 'kwargs': [kwargs]}
      async.tree[fn] = merge_dicts(fn_call, async.tree.get(fn, {}))

    # functions cannot be added to queue
    # work around this by passing an id inst
    async.fn_map[id(fn)] = fn

    #mapping from decorated function to undecorated function
    async.map[async_fn] = fn
    return async_fn

  @classmethod
  def begin(self):
    # applies fn(*args) for each obj in object, ensuring
    # that the proper attributes of shared_data exist before calling a method

    # because some functions depend on the results of other functions, this is 
    # a semi-synchronous operation -- certain methods must be guaranteed to
    # terminate before others 

    # aliasing
    tree, q, processes = async.tree, async.q, async.processes
    fn_map = async.fn_map

    # start a new process for each object that has no dependencies
    for fn, v in tree.items():
      for i in range(len(v['args'])):
        args, kwargs = v['args'].pop(), v['kwargs'].pop()
        start_process(fn, args, kwargs)

    # read from queue as items are added
    i = 0
    while i < len(processes):

      # update note with new data
      result, fn_id = async.q.get()
      fn = fn_map[fn_id]
      i += 1

    cleanup()

if __name__ == '__main__':

  #shared data
  shared = async.manager.Namespace()
  shared.sum = 0

  def process(result):
    pass
    # print "After modification in fn", result

  @async()
  def add(x):
    shared.sum += x
    return shared.sum

  d = defaultdict(int)

  for i in range(100):
    #shared data
    shared = async.manager.Namespace()
    shared.sum = 0
    add(1)
    add(1)
    async.begin()
    d[shared.sum] += 1

  print d
