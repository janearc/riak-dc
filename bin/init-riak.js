#!/usr/bin/env node

var args = require( 'minimist' )(process.argv.slice(2));
var riak = require( 'riak-dc' );

if (args.halp || args.help || args.h) {
	[ 'Usage:',
		'--dry-run      means don\'t actually burn the db',
		'--reap-empties means delete empty tuples',
		'--verbose      means tell me everything' ]
			.forEach( function (h) { console.log(h) } )
	process.exit(0);
}

function default_logger (s) {
	if (process.env['DEBUG'] || args['verbose']) {
		console.log(s);
	}
}

riak.get_buckets().then( function (buckets) {
	buckets.forEach( function (bucket) {
		default_logger( 'traversing bucket: '.concat( bucket ) );
		riak.get_keys( bucket ).then( function (keys) {
			keys.forEach( function (key) {
				default_logger( 'retrieving key: '.concat( key ) );
				riak.get_tuple( bucket, key ).then( function ( tuple ) {
					var tid = bucket.concat( '/', key );
					if (typeof tuple == 'error') {
						default_logger( 'tuple '.concat(tid, ' found empty' ) );
						if (!args['dry-run'] && args['reap-empties']) {
							default_logger( 'reaping empty tuple '.concat( tid ) );
						}
					}
					if (!args['dry-run']) {
						default_logger( 'deleting extant tuple '.concat( tid ) );
						riak.del_tuple( bucket, key ).then( function () {
							default_logger( 'tuple '.concat( tid, ' deleted.' ) );
						} );
					}
					default_logger( bucket.concat( '/', key, ': ', tuple ) );
				} ) // get_tuple
			} ) // keys.foreach
		} ) // get_keys
	} ) // buckets.foreach
} ) // get_buckets
