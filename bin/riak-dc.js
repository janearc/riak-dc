#!/usr/bin/env node
// quick-and-dirty interface to riak from the shell
//

var Riak = require('riak-dc');

// parse opts
//
// var clean_args = require( 'components/common/js/supplemental.js' ).fix_quoted_array( process.argv );
var nopt = require('nopt')
	, noptUsage = require('nopt-usage')
	, Stream    = require('stream').Stream
	, path      = require('path')
	, knownOpts = {
			'list-buckets'   : [ Boolean, null ],
			'list-keys'      : [ Boolean, null ],
			'get-tuple'      : [ Boolean, null ],
			'put-tuple'      : [ Boolean, null ],
			'del-tuple'      : [ Boolean, null ],
			'bucket'         : [ String, null ],
			'key'            : [ String, null ],
			'tuple'          : [ String ],
			'help'           : [ Boolean, null ],
			'ignore-empties' : [ Boolean ]
		}
	, description = {
			'list-buckets'   : 'Show all the buckets in riak',
			'list-keys'      : 'List all keys in a bucket',
			'get-tuple'      : 'Get a single tuple from a bucket/key pair',
			'put-tuple'      : 'Attempts to write a tuple to Riak; returns the serial Riak generates',
			'del-tuple'      : 'Attempts to delete a tuple front Riak; no return value',
			'bucket'         : 'To specify a bucket for operations',
			'key'            : 'To specify a key for operations',
			'tuple'          : 'To specify tuple for operations - this must be base64-encoded',
			'ignore-empties' : 'Ignores 0-byte tuples (silently)',
			'help'           : 'Sets the helpful bit.'
		}
	, defaults = {
			'help' : false
		}
	, shortHands = {
			'h'            : [ '--help' ],
		}
	, parsed = nopt(knownOpts, process.argv)
	, usage = noptUsage(knownOpts, shortHands, description, defaults)

if (parsed['help']) {
	// Be halpful
	//
	console.log( 'Usage: ' );
	console.log( usage );
	process.exit(0); // success
}

if (parsed['list-buckets']) {
	// Display the buckets in Riak
	//
	var pbuckets = Riak.get_buckets();
	pbuckets.then( console.log );
}

if (parsed['list-keys']) {
	// Display the keys in a given bucket
	//
	if (parsed['bucket']) {
		var bucket = parsed['bucket']
			, pkeys  = Riak.get_keys( bucket );
		pkeys.then( console.log )
	}
	else {
		console.log( 'You need to supply a bucket name if you want to list keys.' );
		console.log( usage );
		process.exit( -255 );
	}
}

if (parsed['get-tuple']) {
	// Display a given tuple (bucket/key pair)
	//
	if (parsed['bucket'] && parsed['key']) {
		var bucket = parsed['bucket']
			, key = parsed['key']
			, ptuple = Riak.get_tuple(bucket, key);

		ptuple.then( function (k) {
			if (k instanceof Error) {
				if (!parsed['ignore-empties']) {
					console.log( 'Zero-byte tuple found at ' + bucket + '/' + key );
					process.exit(-255)
				}
			}
			console.log( k )
		} );
	}
	else {
		console.log( 'You need to supply a bucket & key name if you want a tuple.' );
		console.log( usage );
		process.exit( -255 );
	}
}

if (parsed['put-tuple']) {
	// Place a tuple in Riak. Note that this needs to be a base64 encoded string.
	//
	if (parsed['bucket'] && parsed['tuple']) {
		if (parsed['key'] && !parsed['i-mean-it']) {
			console.log( 'Please let Riak decide the key to use for this tuple.' );
			console.log( usage );
			process.exit( -255 );
		}

		var third_arg = parsed['key'] ? parsed['key'] : undefined;

		var bucket = parsed['bucket']
			, tuple = new Buffer(parsed['tuple'], 'base64').toString('ascii')
			, presult = Riak.put_tuple(bucket, tuple, third_arg);

		console.log( 'attempted to place ' + tuple + ' in Riak' );

		presult.then( console.log );
	}
	else {
		console.log( 'You need to supply a bucket and data (a tuple) for this operation.' );
		console.log( usage );
		process.exit( -255 );
	}
}

// jane@cpan.org // vim:tw=80:ts=2:noet
