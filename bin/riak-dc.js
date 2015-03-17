#!/usr/bin/env node
// shell wrapper around riak-dc - you get json and you'll like it.

var Riak = require('riak-dc');

var args = require( 'minimist' )(process.argv.slice(2));
if (args.halp || args.help || args.h) {
	[ 'Usage:',
		'--list-buckets     Show all the buckets in riak',
		'--list-keys        List all keys in a bucket',
		'--get-tuple        Get a single tuple from a bucket/key pair',
		'--get-tuple-short  Get a single tuple from a bucket/key pair (1-argument)',
		'--del-tuple-short  Delete a single tuple from a bucket/key pair (1-argument)',
		'--put-tuple        Attempts to write a tuple to Riak; returns the serial Riak generates',
		'--i-mean-it        If you\'re serious and want to tell Riak what key to use for its own tuple.',
		'--del-tuple        Attempts to delete a tuple front Riak; no return value',
		'--bucket           To specify a bucket for operations',
		'--key              To specify a key for operations',
		'--tuple            To specify tuple for operations - this must be base64-encoded',
		'--ignore-empties   Ignores 0-byte tuples (silently)',
		'--ping             Attempts to get a 200 off the Riak' ]
			.forEach( function (h) { console.log(h) } )
	process.exit(0);
}

if (args['ping']) {
	var pping = Riak.ping();
	pping.then( console.log );
}

if (args['list-buckets']) {
	// Display the buckets in Riak
	//
	var pbuckets = Riak.get_buckets();
	pbuckets.then( console.log );
}

if (args['list-keys']) {
	// Display the keys in a given bucket
	//
	if (args['bucket']) {
		var bucket = args['bucket']
			, pkeys  = Riak.get_keys( bucket );
		pkeys.then( console.log )
	}
	else {
		console.log( 'You need to supply a bucket name if you want to list keys.' );
		console.log( usage );
		process.exit( -255 );
	}
}

if (args['get-tuple'] || args['get-tuple-short'] || args['del-tuple'] || args['del-tuple-short']) {
	// Display a given tuple (bucket/key pair)
	//
	var bucket, key
		, gt = args['get-tuple-short']
		, dt = args['del-tuple-short'];

	if ((gt) && (new RegExp( '([^/]+)/(.*)$' ).test( gt.toString() ))) {
		bucket = gt.substr( 0, gt.indexOf( '/' ) );
		key    = gt.substr( gt.indexOf( '/' ) + 1, gt.length );
	}
	else if ((dt) && (new RegExp( '([^/]+)/(.*)$' ).test( dt.toString() ))) {
		bucket = dt.substr( 0, dt.indexOf( '/' ) );
		key    = dt.substr( dt.indexOf( '/' ) + 1, dt.length );
	}
	else if (args['bucket'] && args['key']) {
		bucket = args['bucket'];
		key    = args['key'];
	}
	else {
		console.log( 'You need to supply a bucket & key name to specify a tuple.' );
		console.log( 'Example: --bucket bucketname --key keyname' );
		console.log( 'Example: --get-tuple-short bucketname/keyname' );
		console.log( usage );
		process.exit( -255 );
	}

	if (args['get-tuple'] || gt) {
		var ptuple = Riak.get_tuple(bucket, key);

		ptuple.then( function (k) {
			if (k instanceof Error) {
				if (!args['ignore-empties']) {
					console.log( 'Zero-byte tuple found at '.concat( bucket, '/', key ) );
					process.exit(-255)
				}
			}
			console.log( k )
		} );
	}
	else if (args['del-tuple'] || dt) {
		var dtuple = Riak.del_tuple(bucket, key);
		dtuple.then( function (k) {
			if (k instanceof Error) {
				if (!args['ignore-empties']) {
					console.log( 'Zero-byte tuple found at '.concat( bucket, '/', key ) );
					process.exit(-255)
				}
			}
			console.log( k )
		} );
	}
	else {
		console.log( 'Unclear what operation is intended here.' );
		process.exit( -255 );
	}
}

if (args['put-tuple']) {
	// Place a tuple in Riak. Note that this needs to be a base64 encoded string.
	//
	if (args['bucket'] && args['tuple']) {
		if (args['key'] && !args['i-mean-it']) {
			console.log( 'Please let Riak decide the key to use for this tuple.' );
			console.log( usage );
			process.exit( -255 );
		}

		var third_arg = args['key'] ? args['key'] : undefined;

		var bucket = args['bucket']
			, tuple = new Buffer(args['tuple'], 'base64').toString('ascii')
			, presult = Riak.put_tuple(bucket, tuple, third_arg);

		console.log( 'attempted to place '.concat( tuple, ' in Riak' ) );

		presult.then( console.log );
	}
	else {
		console.log( 'You need to supply a bucket and data (a tuple) for this operation.' );
		console.log( usage );
		process.exit( -255 );
	}
}

// jane@cpan.org // vim:tw=80:ts=2:noet
