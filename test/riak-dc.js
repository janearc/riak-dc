var Riak   = require( '../lib/riak-dc.js' )
	, assert = require( 'assert' )
	, nock   = require( 'nock' );

it( 'Riak package initialised with test values', function () {
	assert.equal(Riak.init( 'localhost', '8098' ), 200)
} );

it( 'get_buckets', function () {
	var buckets_rsp = { "buckets": [ "testing" ] };

	nock( 'http://localhost:8098', { "Content-Type": "application/json" } )
		.get( '/riak/?buckets=true' )
		.reply( 200, buckets_rsp );

	var pbuckets = Riak.get_buckets();
	pbuckets.then( function (buckets) {

		assert( buckets );
		assert.equal( buckets.length, 1 );
		assert.equal( buckets[0], 'testing' );
	} );

});

/*
var result = Riak.put_tuple( 'bucket', 'key', 'value' );
var serial = Riak.put_tuple( 'a_new_bucket', {
	keyname1 : 'keyvalue1',
	keyname2 : 'keyvalue2'
} );
*/

/*
serial.then( function (tid) {
	console.log( 'testing put_tuple and get_tuple');
	console.log( 'looking for ' + tid );
	var tuple = Riak.get_tuple( 'bucket', tid );
	tuple.then( function (body) {
		console.log( 'BODY: ' + body );
	} )
} );
*/
