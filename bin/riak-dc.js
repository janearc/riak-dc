#!/usr/bin/env node
// quick-and-dirty interface to riak from the shell
//

var Riak = require('riak-dc');

// parse opts
//
var parsed = require('sendak-usage').parsedown( {
	'list-buckets'   : {
		'description' : 'Show all the buckets in riak',
		'type' : [ Boolean ],
	},
	'list-keys'      : {
		'description' : 'List all keys in a bucket',
		'type' : [ Boolean ],
	},
	'get-tuple'      : {
		'description' : 'Get a single tuple from a bucket/key pair',
		'type' : [ Boolean, String ],
	},
	'put-tuple'      : {
		'description' : 'Attempts to write a tuple to Riak; returns the serial Riak generates',
		'type' : [ Boolean ],
	},
	'del-tuple'      : {
		'description' : 'Attempts to delete a tuple front Riak; no return value',
		'type' : [ Boolean ],
	},
	'bucket'         : {
		'description' : 'To specify a bucket for operations',
		'type' : [ String ],
	},
	'key'            : {
		'description' : 'To specify a key for operations',
		'type' : [ String ],
	},
	'tuple'          : {
		'description' : 'To specify tuple for operations - this must be base64-encoded',
		'type' : [ String ],
	},
	'help'           : {
		'type' : [ Boolean ],
	},
	'ignore-empties' : {
		'description' : 'Ignores 0-byte tuples (silently)',
		'type' : [ Boolean ],
	},
	'ping'           : {
		'description' : 'Attempts to get a 200 off the Riak',
		'type' : [ Boolean ]
	}
}, process.argv )
	, usage = parsed[1]
	, nopt  = parsed[0];

if (nopt['help']) {
	// Be halpful
	//
	console.log( 'Usage: ' );
	console.log( usage );
	process.exit(0); // success
}

if (nopt['ping']) {
	var pping = Riak.ping();
	pping.then( console.log );
}

if (nopt['list-buckets']) {
	// Display the buckets in Riak
	//
	var pbuckets = Riak.get_buckets();
	pbuckets.then( console.log );
}

if (nopt['list-keys']) {
	// Display the keys in a given bucket
	//
	if (nopt['bucket']) {
		var bucket = nopt['bucket']
			, pkeys  = Riak.get_keys( bucket );
		pkeys.then( console.log )
	}
	else {
		console.log( 'You need to supply a bucket name if you want to list keys.' );
		console.log( usage );
		process.exit( -255 );
	}
}

if (nopt['get-tuple']) {
	// Display a given tuple (bucket/key pair)
	//
	var bucket, key
		, gt = nopt['get-tuple'];

	if (new RegExp( '([^/]+)/(.*)$' ).test( gt.toString() )) {
		bucket = gt.substr( 0, gt.indexOf( '/' ) );
		key    = gt.substr( gt.indexOf( '/' ) + 1, gt.length );
	}
	else if (nopt['bucket'] && nopt['key']) {
			bucket = nopt['bucket'];
			key    = nopt['key'];
	}
	else {
		console.log( 'You need to supply a bucket & key name if you want a tuple.' );
		console.log( usage );
		process.exit( -255 );
	}

	var ptuple = Riak.get_tuple(bucket, key);

	ptuple.then( function (k) {
		if (k instanceof Error) {
			if (!nopt['ignore-empties']) {
				console.log( 'Zero-byte tuple found at ' + bucket + '/' + key );
				process.exit(-255)
			}
		}
		console.log( k )
	} );
}

if (nopt['put-tuple']) {
	// Place a tuple in Riak. Note that this needs to be a base64 encoded string.
	//
	if (nopt['bucket'] && nopt['tuple']) {
		if (nopt['key'] && !nopt['i-mean-it']) {
			console.log( 'Please let Riak decide the key to use for this tuple.' );
			console.log( usage );
			process.exit( -255 );
		}

		var third_arg = nopt['key'] ? nopt['key'] : undefined;

		var bucket = nopt['bucket']
			, tuple = new Buffer(nopt['tuple'], 'base64').toString('ascii')
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
