SemiSync
========

A decorator-based python module for semi-synchronous programming.    
Synchronous when you need it, and asynchronous when you don't!
Pull requests welcome...

What is semisync.py?
-------------------------------------

Some problems are best solved synchronously, while others are a better fit for the asynchronous paradigm.  Most problems fall somewhere in between -- they could benefit from asynchronous execution, but require some events to happen in a certain order.  This module seeks to make blending the two paradigms a bit easier by introducing a concept of dependencies.    If one process must not run until another process has completed, that process is said to be "dependent" on the second process.  Semisync.py was built using python's multiprocessing library and a liberal dose of decorator syntax.

Installation
------------
Install via pip

    sudo pip install semisync
    
or via setup.py

    sudo python setup.py install

Let's See Some Code
-------------------
    from semisync import semisync
    from multiprocessing import Manager
    from random import random, randint
    from time import sleep
  
    # shared data between processes
    shared = Manager().Namespace()
  
    # a demo callback function
    def output(field, value):
      print field + ": $" + str(value)
  
    # simple callback syntax
    @semisync(callback=output)
    def revenue():
      # simulated api call
      sleep(random())
      shared.revenue = randint(1, 1000)
      return "Revenue", shared.revenue
  
    @semisync(callback=output)
    def expenses():
      # simulated api call
      sleep(random())
      shared.expenses = randint(1, 500)
      return "Expenses", shared.expenses
  
    # will run only when revenue() and expenses() have completed
    @semisync(callback=output, dependencies=[revenue, expenses])
    def profit():
      shared.profit = shared.revenue - shared.expenses
      return "Profit", shared.profit
  
    # queue function calls
    revenue()
    expenses()
    profit()
    
    # executes queued calls semi-synchronously
    semisync.begin()
    
    
To repeat the process, simply clear the cache of function calls by using semisync.clear() after each iteration

    for i in range(10):
      revenue()
      expenses()
      profit()
      semisync.begin()
      semisync.clear()


In this simple example, moving from synchronous to semi-synchronous execution cuts the average execution time from 1.00 seconds to .700 seconds.  And although the example used is trivial, dependency trees can be arbitrarily complex.

Additional Notes 
-----------------

In order to make the module more flexible, few assumptions are made about how you choose to deal with shared data.  Although Manager() from the multiprocessing library is used in the example, you're free to use whatever format you desire.  You're also in charge of locking shared data if multiple processes access the same variable.  With great flexibility comes great responsibility.  

