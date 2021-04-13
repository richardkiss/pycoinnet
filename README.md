pycoinnet -- Speaking the Bitcoin Protocol
==========================================

The pycoinnet library is designed for use in conjunction with the Python pycoin library. It provides utilities and examples for writing tools in pure Python that speak the bitcoin protocol on the bitcoin network.

This library uses the asyncio library and requires Python 3.6 (unlike pycoin, which supports Python 2.7).


Install
-------

Using virtual environments:

```
$ NEW_ENV=~/.virtualenv-pycoinnetwork # or whatever path you'd like to use
$ python3 -m venv $NEW_ENV
$ source $NEW_ENV/bin/activate
```


Install Dependencies
--------------------

Now install the dependencies.

```
$ pip install -r requirements.txt
$ pip install .
```

Nothing here really pycoinnet-specific except the dependency on pycoin.


Try the SPV Demo
----------------


```
$ mkdir -p ~/.wallet/default
$ echo 1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3 > ~/.wallet/default/watch_addresses  # first transaction ever, block 170
$ echo 1CaNHx4vzpmPBv4a6U7pcKQF6R8U6JeLUy >> ~/.wallet/default/watch_addresses # very common address starting block 258045
$ ku -ua 1 >> ~/.wallet/default/watch_addresses # address for secret exponent 1
$ PYTHONPATH=`pwd` python pycoinnet/examples/wallet.py
```

This example will connect, then update its view of the blockchain, noting spendables that are paid to the addresses in `~/.wallet/default/watch_addresses`. Check the source for more info. NOTE: it will probably display a lot of errors when it runs, as most of the initial peers, acquired via DNS, are down.


Future Direction
----------------

More work on the SPV wallet, and features to make it more reliable and more useful as a wallet.


Donate
------

Want to donate? Feel free. Send to 1KissEskteXTAXbh17qJYLtMes1B6kJxZj.
Or hire me to do bitcoin consulting... him@richardkiss.com.


[pycoin]: https://github.com/richardkiss/pycoin
