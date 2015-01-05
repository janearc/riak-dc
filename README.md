riak-dc
=======

So, Basho does not have an official javascript interface to Riak. However, there
is this [riak-js](http://riakjs.com/) thing. I found it to be poorly documented,
and the interface was not suitable for very small programs (I write command-line
tools, rather than web-front-end-ish type things). I wanted something
synchronous, something simple, that wouldn't take up too much space in my code.

Accordingly, I wrote a tiny little wrapper around `http`, and here is the product.

It is my hope that the code behind this never actually exceeds the length of the
documentation & test suite, and that anyone should be able to just
`npm install riak-dc` and be coding against a Riak quickly.

### Exported Functions

* `Riak.init( hostname, port, proxy )`
  - Takes two arguments: `hostname`, and `port`, which are what you would
  expect them to be. Note that this call is optional, and defaults are `localhost`
  and `8098`, respectively. If you wish to use an http proxy, you may pass an
  object that looks like `{ host: 'localhost', port: '3128' }` and `riak-dc`
  will attempt to use this proxy for all calls. You may also set the
  environment variables `HTTP_PROXY_HOST` and `HTTP_PROXY_PORT`. Using `init()`
  will override any environment variables you may have set.

* `Riak.ping()`
  - Takes no arguments, returns a true value if the server gives a 200 response,
  and something false otherwise.

* `Riak.get_keys( bucket )`
  - Takes a single argument, the name of the bucket you'd like keys
  for, and returns a promise to a list of the keys in that bucket.
  *Note that Riak does not care if this bucket does not exist.* If you ask for
  the keys from a non-existent bucket, Riak will dutifully tell you the list of
  keys in that bucket is `[]`, which is what `get_keys` will return to you here.

* `Riak.get_tuple( bucket, key )`
  - Takes two arguments, the name of the bucket and key you wish to
  have the value for. This is returned as a promised object, and will be whatever
  Riak has stored. Typically this is JSON, but you will need to parse that
  yourself (such as `JSON.parse(tuple)`), as it can be anything.
  *In the event that Riak has stored a 0-byte tuple:* (that is, the bucket/key pair
  are valid, the HTTP request returns 200, but the record is empty), you will
  receive an Error. This is not strictly an error condition, but it is helpful to
  know that the request itself was at least successful (whereas returning nothing
  is not helpful).

* `Riak.get_buckets( )`
  - Takes no arguments, and returns a promise of a list of the
  buckets Riak knows about.

* `Riak.put_tuple( bucket, tuple, key )`
  - Riak is very helpful in that it will provide you a key for the things you would
  store. So while in order to store something you must provide a bucket and the
  thing you would store (the tuple), the `key` argument is optional. If you supply
  a key, that will be provided as Riak for the tuple you have asked to store.
  User-supplied keys are useful for [key filters](http://docs.basho.com/riak/latest/dev/using/keyfilters/),
  if hierarchically-named keys is something you want.
  If you have omitted a key for storing the tuple, you will be returned a promise
  for the key that Riak has provided.

* `Riak.del_tuple( bucket, key )`
  - Should you wish to remove a tuple from your Riak, you must specify a bucket and
  a key. Returns what Riak returns; in the case you have tried to remove a tuple
  Riak doesn't know about, Riak will return an Error.

author
====

[@janearc](https://github.com/janearc), jane@cpan.org
