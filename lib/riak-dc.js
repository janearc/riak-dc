/*
 Riak is very fancy. Too fancy. This makes it much simpler. Following is
 a set of simple, synchronous-ish calls to get, put, and delete things
 from a Riak.

 riak don't care dot js
*/

'use strict';

var riak_host    = 'localhost'
	, riak_port    = 8098
	, proxy_port   = undefined
	, proxy_host   = undefined
	, transactions = { }
	, q            = require('q')
	, http         = require('http')

// chibi http client factory
//
var chcf = function ( path, method, headers, callback ) {
	var args = { };

	args['host']    = riak_host;
	args['port']    = riak_port;
	args['path']    = path;
	args['method']  = method;
	args['headers'] = headers;

	if ((process.env.HTTP_PROXY_PORT != undefined) &&
		(process.env.HTTP_PROXY_HOST != undefined)    ) {

		proxy_port = process.env.HTTP_PROXY_PORT;
		proxy_host = process.env.HTTP_PROXY_HOST;
	}

	if ( (proxy_port != undefined) && (proxy_host != undefined) ) {
		args['host']            = proxy_host;
		args['port']            = proxy_port;
		args['path']            = 'http://'.concat(riak_host, ':', riak_port, path);
		args['headers']['Host'] = riak_host;
		args['headers']['Port'] = riak_port;
	}

	return http.request( args, callback );
}
/**
 * Gets a list of all the buckets in riak, returned as a promise
 *
 */
function get_buckets () { // {{{
	// get_buckets returns a (promise of a) list of all the buckets riak knows about.
	//

	// We will store the response here later.
	//
	var gotten = '';

	var deferred = q.defer();

	var req = chcf( '/riak/?buckets=true', 'GET', {
		'Content-Type': 'application/json'
	}, function (result) {
		result.on('data', function (chunk) {
			// We assume this will be json that looks like:
			// { buckets: [ 'thing', ... ] }
			//
			var bucket_names = JSON.parse(chunk);
			deferred.resolve( bucket_names['buckets'] );
	} ) } );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.end();

	return deferred.promise;

} // }}} get_buckets

/**
 * Sends a ping to the database, asking for an HTTP 200, returns a promise
 * to an Error object if it fails.
 *
 */
function ping () { // {{{
	// ping returns a promise to a true value if your Riak replies in the
	// affirmative. If you get anything other than 200, your promise will be
	// false. At present there is no test for timeout.
	//
	var deferred = q.defer();

	var req = chcf( '/ping', 'GET', {
	}, function (response) {
		response.on('data', function (chunk) { deferred.resolve( true ) } );
		response.on( 'end', function () {      deferred.resolve( true ) } );
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'error', function(e) {
		deferred.resolve( false, new Error( 'Ping failed: ' + e ) );
	} );

	req.end();

	return deferred.promise;

} // }}}

/**
 * Gets a list of keys for a supplied bucket, returned as a promise.
 * @param {object} bucket - the name of the bucket
 */
function get_keys (bucket) { // {{{
	// get_tuples keys a (promise of a) list of all the keys that Riak knows
	// about in a given bucket.
	//

	// We will store the response here later.
	//
	var gotten = '';

	var deferred = q.defer();

	var path = '/riak/' + bucket + '?keys=true';

	var req = chcf( path, 'GET', {
		'Content-Type' : 'application/json'
	}, function (result) {
		result.on('data', function (chunk) {
			// We assume this will be json that looks like:
			// { keys: [ 'thing', ... ] }
			//
			var key_names = JSON.parse(chunk);
			deferred.resolve( key_names['keys'] );
	} ) } );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.end();

	return deferred.promise;

} // }}} get_keys

/**
 * Gets a tuple from Riak
 * @param {object} bucket - the name of the bucket
 * @param {object} key - the name of the key for the tuple you want.
 */
function get_tuple (bucket, key) { // {{{
	// get_tuple cannot return to you the stringy value of the bucket/key tuple,
	// because node. so instead we return to you a promise.
	//
	// TODO: Check for tombstones
	//

	var gotten = '';
	var subpath = bucket + '/' + key;

	var deferred = q.defer();

	var req = chcf( '/riak/'.concat( subpath ), 'GET', {
		'Content-Type' : 'application/json'
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
	} );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );

	req.end();

	return deferred.promise;

} // }}} get_tuple

/**
 * Deletes a tuple from Riak (mostly)
 * @param {object} bucket - the name of the bucket
 * @param {object} key - the name of the key for the tuple you want.
 */
function del_tuple (bucket, key) { // {{{
	// del_tuple aims to delete a tuple given a bucket/key pair. The way Riak
	// works means that we don't /exactly/ delete it, but we mark it for later
	// deletion (a 'tombstone'). On success, Riak says nothing. If it's not
	// found, you'll get an Error.
	//

	var subpath = bucket + '/' + key;

	var deferred = q.defer();

	var req = chcf( '/riak/'.concat( subpath ), 'DELETE', {
		'Content-Type' : 'application/json'
	}, function (result) {
		result.on('data', function (chunk) {
			if (chunk == 'not found') {
				return new Error( 'tuple ' + bucket + '/' + key + ' not found' );
			}
			deferred.resolve( chunk );
	} ) } );

	[ 'connect', 'socket', 'end' ].forEach( function (state) {
		req.on( state, function () { default_logger( 'State ' + state + ' entered.' ) } )
	} );

	req.on( 'error', function(e) {
		return new Error("http: failure during request: " + e.message);
	} );
	req.end();

	return deferred.promise;
} // }}} del_tuple

/**
 * Places a tuple in a bucket in Riak.
 * @param {object} bucket - the name of the bucket
 * @param {object} payload - the object you wish to store
 * @param {object} key - if you wish to force the key name, supply it
 */
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

	var req = chcf( '/riak/'.concat( bucket, '?returnbody=true' ), 'POST', {
		'Content-Type' : 'application/json',
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

/**
 * Specify configuration options for the riak connection.
 * @param {string} host - the hostname/address of the Riak you wish to use
 * @param {string} port - the port to use on the supplied host
 * @param {object} proxy - a hash containing 'port' and 'host' referring to a proxy
 */
var init = function ( host, port, proxy ) { // {{{
	if (host) riak_host = host;
	if (port) riak_port = port;

	if (proxy) {
		proxy_port = proxy.port;
		proxy_host = proxy.host;
	}
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

// @janearc // jane@cpan.org // vim:tw=79:ts=2:noet
