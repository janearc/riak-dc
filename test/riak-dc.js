var Riak   = require( '../lib/riak-dc.js' )
	, assert = require( 'assert' );

it( 'Riak package initialised with test values', function () {
	assert.equal(Riak.init( 'localhost', '80' ), 200)
} );

it( 'get_buckets', function () {
	var buckets_rsp = { "buckets": [ "testing" ] };

	var server = require('sinon').fakeServer.create();
	server.autoRespond = true;

	server.requests[0].respondWith(
		'/riak/?buckets=true'
		[ 200, { "Content-Type": "application/json" } ],
		JSON.stringify(buckets_rsp)
	);

	Riak.get_buckets().then( function (pbuckets) {

		assert( pbuckets );
		assert.equal( pbuckets.length, 0 );
		assert.equal( pbuckets[0], 'testing' );
	} );

	server.restore();

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
