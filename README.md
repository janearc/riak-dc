riak-dc
=======

So, Basho does not have an official javascript interface to Riak. However, there is this [riak-js](http://riakjs.com/) thing. I found it to be poorly documented, and the interface was not suitable for very small programs (I write command-line tools, rather than web-front-end-ish type things). I wanted something synchronous, something simple, that wouldn't take up too much space in my code.

Accordingly, I wrote a tiny little wrapper around `http`, and here is the product.

It is my hope that the code behind this never actually exceeds the length of the documentation & test suite, and that anyone should be able to just `npm install riak-js` and be coding against a Riak quickly.

### You will need a working Riak.

Sorry, I know it's a pain.

### Exported Functions

* `Riak.init( hostname, port )`

`init` takes two arguments: `hostname`, and `port`, which are what you would expect them to be. Note that this call is optional, and `riak-dc` defaults to `localhost`, and `8098`, respectively.

* `Riak.get_keys( bucket )`

`get_keys()` takes a single argument, the name of the bucket you'd like keys for, and returns a promise to a list of the keys in that bucket.

* `Riak.get_tuple( bucket, key )`

`get_tuple()` takes two arguments, the name of the bucket and key you wish to have the value for. This is returned as a promised object, and will be whatever Riak has stored. Typically this is JSON, but you will need to parse that yourself (such as `JSON.parse(tuple)`), as it can be anything.

* `Riak.get_buckets( )`

`get_buckets()` takes no arguments, and returns a promise of a list of the buckets Riak knows about.

* `Riak.put_tuple( bucket, tuple, key )`

Riak is very helpful in that it will provide you a key for the things you would store. So while in order to store something you must provide a bucket and the thing you would store (the tuple), the `key` argument is optional. If you supply a key, that will be provided as Riak for the tuple you have asked to store. User-supplied keys are useful for [key filters](http://docs.basho.com/riak/latest/dev/using/keyfilters/), if hierarchically-named keys is something you want.

If you have omitted a key for storing the tuple, you will be returned a promise for the key that Riak has provided.

* `Riak.del_tuple( bucket, key )`

Should you wish to remove a tuple from your Riak, you must specify a bucket and a key.