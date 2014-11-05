/*
 Riak is very fancy. Too fancy. This makes it much simpler. Following is
 a set of simple, synchronous-ish calls to get, put, and delete things
 from a Riak.

 riak don't care dot js
*/

var riak_host = 'localhost';
var riak_port = 8098;

var q = require('q');


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
	} ) } ); // request

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.end();

	return deferred.promise;

} // }}} get_buckets

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
	} ) } ); // request

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
			// this should be switched to json once testing is done
			//
			'Content-Type' : 'application/json'
		}
	}, function (response) {
		response.on('data', function (chunk) {
			gotten = gotten + chunk;
			deferred.resolve( gotten );
		} );
		response.on( 'end', function (response) {
			// TODO: verbose logging
			//
			if (!gotten.length) {
				// So what has happened here is we have zero data from Riak, and we
				// have reached the end of our http response. This means we have a
				// zero-byte tuple. We have to let the user know about this, but it
				// is not strictly a bug.
				//
				deferred.resolve( new Error( '0-byte tuple encountered for key ' + key ) );
			}
		} );
	} ); // request

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
	// found, you'll get 'not found'.
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

	var serial = '';
	var gotten = '';


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
	} );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'response', function ( response ) {
		// You must stringify things you post with the request object.
		//
		req.write( JSON.stringify(payload) );

		// Riak returns us something like:
		//
		//   location: /riak/a_new_bucket/MYUbKWjuO5JvJGEoHHLI3ajfj5B
		//
		var key;
		key = response.headers['location'].split('/')[3];

		// And hand it off to q.
		//
		deferred.resolve( key );
	} );

	// This should be the part that actually posts the data to the server
	//

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

// Not-exported things.
//
function default_logger (s) {
	if (process.env['DEBUG']) {
		console.log(s);
	}
}
