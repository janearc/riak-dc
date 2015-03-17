#!/usr/bin/env node

var nopt = require('nopt')
	, knownopts = {
			'dry-run'         : [ Boolean ],
			'reap-empties'    : [ Boolean ],
			'verbose'         : [ Boolean ]
		}
	, parsed = nopt( knownopts, process.argv )

	, Riak = require( 'riak-dc' )

function default_logger (s) {
	if (process.env['DEBUG'] || parsed['verbose']) {
		console.log(s);
	}
}

Riak.get_buckets().then( function (buckets) {
	buckets.forEach( function (bucket) {
		default_logger( 'traversing bucket: ' + bucket );
		Riak.get_keys( bucket ).then( function (keys) {
			keys.forEach( function (key) {
				default_logger( 'retrieving key: ' + key );
				Riak.get_tuple( bucket, key ).then( function ( tuple ) {
					var tid = bucket + '/' + key;
					if (typeof tuple == 'error') {
						default_logger( 'tuple ' + tid + ' found empty' );
						if (!parsed['dry-run'] && parsed['reap-empties']) {
							default_logger( 'reaping empty tuple ' + tid );
						}
					}
					if (!parsed['dry-run']) {
						default_logger( 'deleting extant tuple ' + tid );
						Riak.del_tuple( bucket, key ).then( function () {
							default_logger( 'tuple ' + tid + ' deleted.' );
						} );
					}
					default_logger( bucket + '/' + key + ': ' + tuple )
				} ) // get_tuple
			} ) // keys.foreach
		} ) // get_keys
	} ) // buckets.foreach
} ) // get_buckets
