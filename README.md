pycoinnet -- Speaking the Bitcoin Protocol
==========================================

The pycoinnet library is designed for use in conjunction with the Python pycoin library. It provides utilities and examples for writing tools in pure Python that speak the bitcoin protocol on the bitcoin network.

Note that is uses the new asyncio library due to be release in Python 3.4 (and available from pypi in Python 3.3 -- type "pip install asyncio"), and so requires Python 3.3 or higher (unlike pycoin, which supports Python 2.7).


Install
-------

Using virtual environments:

```
$ NEW_ENV=~/.virtualenv-pycoinnetwork # or whatever path you'd like to use
$ virtualenv -p python3 $NEW_ENV
$ source $NEW_ENV/bin/activate
```


Python 3 Broken on Mac OS X Brew
--------------------------------

On Mac OS X, using brew Python3, I've noticed that virtualenvs do NOT properly
set up the pip install path, so packages are installed into the global Python3
installation. This kind of defeats the purpose of virtualenvs!

To fix this, edit the pip files.

```
$ vi $NEW_ENV/bin/pip*
```

In all three of these pip files, change ```#!/usr/local/bin/python3``` to ```#!/Users/<whatever>/.virtualenv-pycoinnetwork/env/bin/python3```
substituting in your actual path to virtualenv python.


Install Dependencies
--------------------

Anyway, now install the dependencies.

```
$ pip install -r requirements.txt
$ pip install .
```

Nothing here really pycoinnet-specific except the dependancy on pycoin.


Try It
------

```
$ python pycoinnet/examples/address_keeper.py
```

This example will connect, fetch a list of peers, and keep it groomed in a text file called addresses.txt. Check the source for more info. NOTE: it will probably display a lot of errors when it runs, as most of the initial peers, acquired via DNS, are down.


Future Direction
----------------

More and more standard handling features will be added, especially for functionality whose canonical operation should be clear (like forwarding transactions and blocks between peers).

There is not much built-in support for persistent storage for now beyond the "address keeper" demo.


Donate
------

Want to donate? Feel free. Send to 1KissEskteXTAXbh17qJYLtMes1B6kJxZj. Or hire me to do bitcoin consulting... him@richardkiss.com.


[pycoin]: https://github.com/richardkiss/pycoin
