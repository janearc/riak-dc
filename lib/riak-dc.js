/*
 Riak is very fancy. Too fancy. This makes it much simpler. Following is
 a set of simple, synchronous-ish calls to get, put, and delete things
 from a Riak.

 riak don't care dot js
*/

var riak_host    = 'localhost'
	, riak_port    = 8098
	, transactions = { }
	, q            = require('q')
	, moment       = require('moment')



function get_buckets () { // {{{
	// get_buckets returns a (promise of a) list of all the buckets riak knows about.
	//

	// We will store the response here later.
	//
	var gotten = '';

	var deferred = q.defer();

	var req = require('http').request( {
		host    : riak_host,
		path    : '/riak/?buckets=true',
		port    : riak_port,
		method  : 'GET',
		headers : {
			'Content-Type' : 'application/json'
		}
	}, function (result) {
		result.on('data', function (chunk) {
			// We assume this will be json that looks like:
			// { buckets: [ 'thing', ... ] }
			//
			var bucket_names = JSON.parse(chunk);
			deferred.resolve( bucket_names['buckets'] );
	} ) } ); // http.request

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.end();

	return deferred.promise;

} // }}} get_buckets

function ping () { // {{{
	// ping returns a promise to a true value if your Riak replies in the
	// affirmative. If you get anything other than 200, your promise will be
	// false. At present there is no test for timeout.
	//
	var deferred = q.defer();

	var req = require('http').request( {
		host    : riak_host,
		path    : '/ping',
		port    : riak_port,
		method  : 'GET',
		headers : {
			'Content-Type' : 'application/json'
		}
	}, function (response) {
		response.on('data', function (chunk) { deferred.resolve( true ) } );
		response.on( 'end', function () {      deferred.resolve( true ) } );
	} ); // http.request

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'error', function(e) {
		deferred.resolve( false, new Error( 'Ping failed: ' + e ) );
	} );

	req.end();

	return deferred.promise;

} // }}}

function get_keys (bucket) { // {{{
	// get_tuples keys a (promise of a) list of all the keys that Riak knows
	// about in a given bucket.
	//

	// We will store the response here later.
	//
	var gotten = '';

	var deferred = q.defer();

	var path = '/riak/' + bucket + '?keys=true';

	var req = require('http').request( {
		host    : riak_host,
		'path'  : path,
		port    : riak_port,
		method  : 'GET',
		headers : {
			'Content-Type' : 'application/json'
		}
	}, function (result) {
		result.on('data', function (chunk) {
			// We assume this will be json that looks like:
			// { keys: [ 'thing', ... ] }
			//
			var key_names = JSON.parse(chunk);
			deferred.resolve( key_names['keys'] );
	} ) } ); // http.request

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.end();

	return deferred.promise;

} // }}} get_keys

function get_tuple (bucket, key) { // {{{
	// get_tuple cannot return to you the stringy value of the bucket/key tuple,
	// because node. so instead we return to you a promise.
	//
	// TODO: Check for tombstones
	//

	var gotten = '';
	var subpath = bucket + '/' + key;

	var deferred = q.defer();

	var req = require('http').request( {
		host    : riak_host,
		path    : '/riak/' + subpath,
		port    : riak_port,
		method  : 'GET',
		headers : {
			'Content-Type' : 'application/json'
		}
	}, function (response) {
		response.on('data', function (chunk) {
			gotten = gotten + chunk;
			deferred.resolve( gotten );
		} );
		response.on( 'end', function () {
			if (!gotten.length) {
				// So what has happened here is we have zero data from Riak, and we
				// have reached the end of our http response. This means we have a
				// zero-byte tuple. We have to let the user know about this, but it
				// is not strictly a bug.
				//
				deferred.resolve( new Error( '0-byte tuple encountered for key ' + key ) );
			}
		} );
	} ); // http.request

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	req.end();

	return deferred.promise;

} // }}} get_tuple

function del_tuple (bucket, key) { // {{{
	// del_tuple aims to delete a tuple given a bucket/key pair. The way Riak
	// works means that we don't /exactly/ delete it, but we mark it for later
	// deletion (a 'tombstone'). On success, Riak says nothing. If it's not
	// found, you'll get an Error.
	//

	var subpath = bucket + '/' + key;

	var deferred = q.defer();

	var req = require('http').request( {
		host    : riak_host,
		path    : '/riak/' + subpath,
		port    : riak_port,
		method  : 'DELETE',
		headers : {
			'Content-Type' : 'application/json'
		}
	}, function (result) {
		result.on('data', function (chunk) {
			if (chunk == 'not found') {
				return new Error( 'tuple ' + bucket + '/' + key + ' not found' );
			}
			deferred.resolve( chunk );
	} ) } ); // request

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );
	req.end();

	return deferred.promise;
} // }}} del_tuple

function put_tuple (bucket, payload, forced_key) { // {{{
	// put_tuple will send off your payload and do its best to return a promise
	// which contains a serial from the Riak you may use in the future to refer to it.
	//

	// Data from the request gets stored here, assuming chunking.
	//
	var gotten = ''
		, serial = '';

	var deferred = q.defer();

	if (forced_key) {
		bucket = bucket + '/' + forced_key;
	}

	var req = require('http').request( {
		host    : riak_host,
		path    : '/riak/' + bucket + '?returnbody=true',
		port    : riak_port,
		method  : 'POST',
		headers : {
			'Content-Type' : 'application/json',
		}
	}, function (result) {
		result.on('data', function (chunk) {
			gotten = gotten + chunk;
		} );
	} ); // http.request

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	// You must stringify things you post with the request object.
	//
	req.write( JSON.stringify(payload) );

	req.on( 'response', function ( response ) {

		// Riak returns us something like:
		//
		//   location: /riak/a_new_bucket/MYUbKWjuO5JvJGEoHHLI3ajfj5B
		//
		if (!forced_key) {
			var key;
			key = response.headers['location'].split('/')[3];

			// And hand it off to q.
			//
			deferred.resolve( key );
		}
		else {
			deferred.resolve( forced_key );
		}
	} );

	req.end();

	return deferred.promise;

} // }}} put_tuple

var init = function ( host, port ) { // {{{
	if (host) riak_host = host;
	if (port) riak_port = port;

	return 200;
} // }}} init

// Exported things
//

exports.init = init;

exports.get_keys    = get_keys;
exports.get_tuple   = get_tuple;
exports.get_buckets = get_buckets;
exports.put_tuple   = put_tuple;
exports.del_tuple   = del_tuple;
exports.ping        = ping;

// Not-exported things.
//
// TODO: Replace with log4js
//
function default_logger (s) {
	if (process.env['DEBUG']) {
		console.log(s);
	}
}

function nonce () { return require('crypto').randomBytes(Math.ceil(32)).toString('hex') }

function xact (serial) { // {{{
	return {
		'serial' : serial,
		'state'  : 'open',
		'opened' : '',
		'closed' : ''
	};
} // }}}
function add_xact (transaction) { // {{{
	if (transactions[transaction.serial]) {
		return new Error( 'Requested new transaction ID ' + serial + ' but this is already-existing transaction.' );
	}
	if (transactions[transaction.serial].state === 'closed') {
		return new Error( 'Requested new transaction ID ' + serial + ' but this transaction happened in the past and is now closed.' );
	}

	transactions[transaction.serial] = transaction;
	transactions[transaction.serial].opened = moment().format();
	return transaction.serial;
} // }}}
function end_xact (serial) { // {{{
	if (transactions[serial] ) {
		if (transactions[serial].state === 'closed') {
			return new Error( 'Attempted to close transaction ID ' + serial + ' but this transaction happened in the past and is already closed.' );
		}
		else if (transactions[serial].state != 'open') {
			return new Error( 'Request to close transaction ID ' + serial + ' but this transaction is in an unknown state.' );
		}
	}
	else (! transactions[serial]) {
		return new Error( 'Attempted to close transaction ID ' + serial + ' but this does not look like an open transaction ID.' );
	}
	transactions[serial].state  = 'closed';
	transactions[serial].closed = moment().format();
	return transactions[serial];
} // }}}

// @janearc // jane@cpan.org // vim:tw=79:ts=2:noet
