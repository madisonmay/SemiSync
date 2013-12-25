SemiSync
========

A decorator-based python module for semi-synchronous programming.    
Pull requests welcome! 

What is Semi-Synchronous Programming?
-------------------------------------

Some problems are best solved synchronously, while others are a better fit for the asynchronous paradigm.  I believe most problems fall somewhere in between -- they could benefit from asynchronous execution, but require some events to happen in a certain order.  This module seeks to make blending the two paradigms a bit easier by introducing a concept of dependencies.    If one process must not run until another process has completed, that process is said to be "dependent" on the second process.

Let's See Some Code
-------------------

