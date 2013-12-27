from semisync import semisync
from multiprocessing import Manager
from random import random, randint
from time import sleep

# shared data between processes
shared = Manager().Namespace()

def output(field, value):
  print field + ": $" + str(value)

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

@semisync(callback=output, dependencies=[revenue, expenses])
def profit():
  shared.profit = shared.revenue - shared.expenses
  return "Profit", shared.profit

revenue()
expenses()
profit()
semisync.begin()
    